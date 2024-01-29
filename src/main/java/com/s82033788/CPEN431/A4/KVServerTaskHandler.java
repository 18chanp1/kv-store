package com.s82033788.CPEN431.A4;

import com.google.common.cache.Cache;
import com.google.protobuf.ByteString;
import com.s82033788.CPEN431.A4.cache.RequestCacheKey;
import com.s82033788.CPEN431.A4.cache.RequestCacheValue;
import com.s82033788.CPEN431.A4.map.KeyWrapper;
import com.s82033788.CPEN431.A4.map.ValueWrapper;
import com.s82033788.CPEN431.A4.proto.KeyValueRequest.KVRequest;
import com.s82033788.CPEN431.A4.proto.Message.Msg;
import com.s82033788.CPEN431.A4.wrappers.*;
import org.apache.commons.pool2.impl.GenericObjectPool;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import static com.s82033788.CPEN431.A4.cache.ResponseType.*;

public class KVServerTaskHandler implements Runnable {
    private final AtomicInteger bytesUsed;
    private final Lock bytesUsedLock;
    private final DatagramPacket iPacket;
    private final DatagramSocket socket;
    private final Cache<RequestCacheKey, RequestCacheValue> requestCache;
    private final ConcurrentMap<KeyWrapper, ValueWrapper> map;
    private final ReadWriteLock mapLock;
    //this is concurrently inconsequential, they can just try again later.
    private final ThreadPoolExecutor tpe;
    private boolean responseSent = false;
    private PublicBuffer incomingPublicBuffer;
    final private GenericObjectPool bytePool;  //this is thread safe

    final private ThreadSharedResource resources;

    // Constants
    final static int KEY_MAX_LEN = 32;
    final static int VALUE_MAX_LEN = 10_000;
    final static int MEMORY_SAFETY = 524_288;

    // Response codes
    public final static int RES_CODE_INVALID_KEY = 0x6;
    public final static int RES_CODE_INVALID_VALUE = 0x7;
    public final static int RES_CODE_INVALID_OPCODE = 0x5;
    public final static int RES_CODE_SUCCESS = 0x0;
    public final static int RES_CODE_INVALID_OPTIONAL = 0x21;
    public final static int RES_CODE_RETRY_NOT_EQUAL = 0x22;
    public final static int RES_CODE_NO_KEY = 0x01;
    public final static int RES_CODE_NO_MEM = 0x2;
    public final static int RES_CODE_OVERLOAD = 0x3;

    //parameters

    public final static int CACHE_OVL_WAIT_TIME = 80;
    public final static int THREAD_OVL_WAIT_TIME = 10;

    public KVServerTaskHandler(DatagramPacket iPacket,
                               Cache<RequestCacheKey, RequestCacheValue> requestCache,
                               ConcurrentMap<KeyWrapper, ValueWrapper> map,
                               ReadWriteLock mapLock,
                               ThreadPoolExecutor tpe,
                               AtomicInteger bytesUsed,
                               Lock bytesUsedLock,
                               GenericObjectPool<ThreadSharedResource> bytePool,
                               ThreadSharedResource resource,
                               DatagramSocket server) {
        this.iPacket = iPacket;
        this.socket = server;
        this.requestCache = requestCache;
        this.map = map;
        this.mapLock = mapLock;
        this.tpe = tpe;
        this.bytesUsed = bytesUsed;
        this.bytesUsedLock = bytesUsedLock;
        this.bytePool = bytePool;
        this.resources = resource;
    }


    @Override
    public void run()
    {
        mainHandlerFunction();
        bytePool.returnObject(resources);
    }

    public void mainHandlerFunction() {
        if (responseSent) throw new IllegalStateException();

        //decode the message
        UnwrappedMessage unwrappedMessage;
        try {
            unwrappedMessage = unpackPacket(iPacket);
        } catch (IOException e) {
            //System.err.println("Packet does not match .proto");
            //System.err.println(e);
            //System.err.println("Stopping packet handling and returning");

            //No response, like A1/A2
            return;
        } catch (InvalidChecksumException e) {
            //System.err.println("Packet checksum does not match");
            //System.err.println("Stopping packet handling and returning");

            //no response, like A1/A2
            return;
        }


       //TODO check cache space remaining
        RequestCacheValue reply;
        try {
            reply = requestCache.get(new RequestCacheKey(unwrappedMessage.getReqID().toByteArray(), unwrappedMessage.getCrc()),
                    () -> createAndSendResponse(unwrappedMessage));
        } catch (ExecutionException e) {
            //TODO deal with this
            throw new RuntimeException(e);
        }

        //assert that the request is exactly the same
        if(reply.getIncomingCRC() != unwrappedMessage.getCrc()) {
            try {
                RequestCacheValue res = new RequestCacheValue.Builder(
                        unwrappedMessage.getCrc(),
                        iPacket.getAddress(),
                        iPacket.getPort(),
                        unwrappedMessage.getReqID(),
                        incomingPublicBuffer)
                        .setResponseType(RETRY_NOT_EQUAL)
                        .build();

                sendResponse(res.generatePacket());
            } catch (IOException e) {
                //System.err.println("Unable to send mismatched retry due to IO");
                //System.err.println("Will not send response packet");
            }
            return;
        }

        //send the reply, no concurrency problem here, since packet will be sent eventually.
        // (tbh don't even need to synchronize load other than to avoid double loading)
        if (!responseSent) {
            try {
                sendResponse(reply.generatePacket());
            } catch (IOException e) {
                //System.err.println("Unable to send cached response due to IO");
                //System.err.println("Will not send response packet");
            }
        }
    }

    private RequestCacheValue createAndSendResponse(UnwrappedMessage unwrappedMessage) throws
            IOException {


        UnwrappedPayload payload;
        RequestCacheValue.Builder scaf = new RequestCacheValue.Builder(
                unwrappedMessage.getCrc(),
                iPacket.getAddress(),
                iPacket.getPort(),
                unwrappedMessage.getReqID(),
                incomingPublicBuffer);

        try {
            payload = unpackPayload(unwrappedMessage.getPayload());
        }  catch (ValueLengthException e) {
            //System.err.println("Value field exceeds size");
            //System.err.println("Sending rejection packet");
            RequestCacheValue res;
            scaf.setResponseType(INVALID_VALUE);
            res = scaf.build();
            sendResponse(res.generatePacket());
            return res;
        } catch (KeyLengthException e) {
            //System.err.println("Value field exceeds size");
            //System.err.println("Sending rejection packet");

            scaf.setResponseType(INVALID_KEY);
            RequestCacheValue res = scaf.build();
            sendResponse(res.generatePacket());
            return res;
        }

        //overload
        if(requestCache.size() >= KVServer.CACHE_SZ){
            System.out.println("Cache overflow. Delay Requested");
            RequestCacheValue res = new RequestCacheValue.Builder(unwrappedMessage.getCrc(),
                    iPacket.getAddress(),
                    iPacket.getPort(),
                    unwrappedMessage.getReqID(),
                    incomingPublicBuffer)
                    .setResponseType(OVERLOAD_CACHE)
                    .build();

            sendResponse(res.generatePacket());
            return res;
        }

        if(tpe.getPoolSize() > 64)
        {
            System.out.println("Cache overflow. Delay Requested");
            RequestCacheValue res = new RequestCacheValue.Builder(unwrappedMessage.getCrc(),
                    iPacket.getAddress(),
                    iPacket.getPort(),
                    unwrappedMessage.getReqID(),
                    incomingPublicBuffer)
                    .setResponseType(OVERLOAD_THREAD)
                    .build();

            sendResponse(res.generatePacket());
        }

        //process the packet by request code
        RequestCacheValue res;
        switch(payload.getCommand())
        {
            case 0x01: res = handlePut(scaf, payload); break;
            case 0x02: res = handleGet(scaf, payload); break;
            case 0x03: res = handleDelete(scaf, payload); break;
            case 0x04: res = handleShutdown(scaf, payload); break;
            case 0x05: res = handleWipeout(scaf, payload);  break;
            case 0x06: res = handleIsAlive(scaf, payload); break;
            case 0x07: res =  handleGetPID(scaf, payload); break;
            case 0x08: res = handleGetMembershipCount(scaf, payload);  break;

            default: {
                res = scaf.setResponseType(INVALID_OPCODE).build();
                sendResponse(res.generatePacket());
            }
        }

        if(!responseSent) throw new IllegalStateException();
        return res;
    }


    //helper function to unpack packet
    private UnwrappedMessage unpackPacket(DatagramPacket iPacket)
            throws IOException, InvalidChecksumException {
        long expectedCRC;

        incomingPublicBuffer = new PublicBuffer(iPacket.getData(), PB_ContentType.PACKET, iPacket.getLength());

        Msg deserialized = Msg.parseFrom(incomingPublicBuffer.readPacketFromPB());
        expectedCRC = deserialized.getCheckSum();
        ByteString messageID = deserialized.getMessageID();

        //into the public buffer (for big things)
        deserialized.getMessageID().writeTo(incomingPublicBuffer.writeIDToPB());
        deserialized.getPayload().writeTo(incomingPublicBuffer.writePayloadToPBAfterID());

        //verify checksum
        long actualCRC = incomingPublicBuffer.getCRCFromBody();

        if (actualCRC != expectedCRC) throw new InvalidChecksumException();

        return new UnwrappedMessage(messageID, incomingPublicBuffer, actualCRC);
    }

    // helper function to unpack payload
    private UnwrappedPayload unpackPayload(PublicBuffer payload) throws
            IOException, KeyLengthException, ValueLengthException {
        KVRequest deserialized = KVRequest.parseFrom(payload.readPayloadFromPBBody());
        int command = deserialized.getCommand();
        byte[] key = deserialized.getKey().toByteArray();
        deserialized.getValue().writeTo(incomingPublicBuffer.writeValueToPB());
        int version = deserialized.getVersion();

        if(key.length > KEY_MAX_LEN) throw new KeyLengthException();
        if(incomingPublicBuffer.getLen() > VALUE_MAX_LEN) throw new ValueLengthException();

        return new UnwrappedPayload(command,
                key,
                incomingPublicBuffer.getValueCopy(),
                version,
                deserialized.hasKey(),
                deserialized.hasValue(),
                deserialized.hasVersion() );
    }

    private void sendResponse(DatagramPacket d) throws IOException {
        if (responseSent) throw new IllegalStateException();

        responseSent = true;
        socket.send(d);
    }

    //helper functions to process requests


    private RequestCacheValue handleGetMembershipCount(RequestCacheValue.Builder scaf, UnwrappedPayload payload)
            throws IOException {
        if(payload.isValueExists() || payload.isVersionExists() || payload.isKeyExists())
        {
            RequestCacheValue res = scaf.setResponseType(INVALID_OPTIONAL).build();
            sendResponse(res.generatePacket());
            return res;
        }

        RequestCacheValue res = scaf
                .setResponseType(INVALID_OPTIONAL)
                .setMembershipCount(1)
                .build();
        sendResponse(res.generatePacket());
        return res;
    }

    private RequestCacheValue handleGetPID(RequestCacheValue.Builder scaf, UnwrappedPayload payload) throws IOException {
        if(payload.isValueExists() || payload.isVersionExists() || payload.isKeyExists())
        {
            RequestCacheValue res = scaf.setResponseType(INVALID_OPTIONAL).build();
            sendResponse(res.generatePacket());
            return res;
        }

        long pid = ProcessHandle.current().pid();
        RequestCacheValue res = scaf
                .setResponseType(PID)
                .setPID(pid)
                .build();
        sendResponse(res.generatePacket());
        return res;
    }


    private RequestCacheValue handleIsAlive(RequestCacheValue.Builder scaf, UnwrappedPayload payload)
            throws IOException {
        if(payload.isValueExists() || payload.isVersionExists() || payload.isKeyExists())
        {
            RequestCacheValue res = scaf.setResponseType(INVALID_OPTIONAL).build();
            sendResponse(res.generatePacket());
            return res;
        }

        RequestCacheValue res = scaf.setResponseType(ISALIVE).build();
        sendResponse(res.generatePacket());
        return res;
    }


    private RequestCacheValue handleShutdown (RequestCacheValue.Builder scaf, UnwrappedPayload payload) throws IOException {
        if(payload.isValueExists() || payload.isVersionExists() || payload.isKeyExists())
        {
            RequestCacheValue res = scaf.setResponseType(INVALID_OPTIONAL).build();
            sendResponse(res.generatePacket());
            return res;
        }


        RequestCacheValue res = scaf.setResponseType(SHUTDOWN).build();
        sendResponse(res.generatePacket());

        System.out.println("Recevied shutdown command, shutting down now");
        System.exit(0);
        return res;
    }

    private RequestCacheValue handlePut(RequestCacheValue.Builder scaf, UnwrappedPayload payload)
    throws IOException {
        if(!payload.isKeyExists() || !payload.isValueExists())
        {
            RequestCacheValue res = scaf.setResponseType(INVALID_OPTIONAL).build();
            sendResponse(res.generatePacket());
            return res;
        }

        // no need to check version, since by default it is parsed as 0,
        // which according to the spec, is the default value we want

        //defensive design to reject 0 length keys
        if(payload.getKey().length <= 0)
        {
            RequestCacheValue res = scaf.setResponseType(INVALID_KEY).build();
            sendResponse(res.generatePacket());
            return res;
        }


        //check memory remaining
        long remainingMemory = Runtime.getRuntime().maxMemory() -
                (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory());

        //add additional 3000 elem size to deal with non gc'd items hogging up the memory.
        if (map.size() > 4800 && remainingMemory < MEMORY_SAFETY) {
            RequestCacheValue res = scaf.setResponseType(NO_MEM).build();
            sendResponse(res.generatePacket());
            return res;
        }

        bytesUsedLock.lock();

        if(bytesUsed.get() >= 60_817_408) {
            RequestCacheValue res = scaf.setResponseType(NO_MEM).build();
            sendResponse(res.generatePacket());
            bytesUsedLock.unlock();
            return res;
        }


        //atomically put and respond, `tis thread safe.
        mapLock.readLock().lock();
        AtomicReference<IOException> ioexception= new AtomicReference<>();
        AtomicReference<RequestCacheValue> res = new AtomicReference<>();
        map.compute(new KeyWrapper(payload.getKey()), (key, value) -> {
            try {
                res.set(scaf.setResponseType(PUT).build());
                sendResponse(res.get().generatePacket());
                bytesUsed.addAndGet(payload.getValue().length);
                return new ValueWrapper(payload.getValue(), payload.getVersion());
            } catch (IOException e) {
                ioexception.set(e);
                //System.err.println(e);
                //System.err.println("Socket exception when returning result of PUT");
                //System.err.println("Reply packet will not be sent");
            }
            return value;
        });

        bytesUsedLock.unlock();
        mapLock.readLock().unlock();

        if (ioexception.get() != null) throw ioexception.get();

        return res.get();
    }
    private RequestCacheValue handleGet(RequestCacheValue.Builder scaf, UnwrappedPayload payload) throws IOException {
        if((!payload.isKeyExists()) || payload.isValueExists() || payload.isVersionExists())
        {
            RequestCacheValue res = scaf.setResponseType(INVALID_OPTIONAL).build();
            sendResponse(res.generatePacket());
            return res;
        }

        //defensive design to reject 0 length keys
        if(payload.getKey().length <= 0)
        {
            RequestCacheValue res = scaf.setResponseType(INVALID_KEY).build();
            sendResponse(res.generatePacket());
            return res;
        }

        //atomically get and respond
        mapLock.readLock().lock();
        AtomicReference<IOException> ioexception = new AtomicReference<>();
        AtomicReference<RequestCacheValue> res = new AtomicReference<>();
        map.compute(new KeyWrapper(payload.getKey()), (key, value) -> {
            try {
                if (value == null) {
                    res.set(scaf.setResponseType(NO_KEY).build());
                } else {
                    res.set(scaf
                            .setResponseType(VALUE)
                            .setValue(value)
                            .build());
                }
                sendResponse(res.get().generatePacket());
                return value;
            } catch (IOException e) {
                //System.err.println(e);
                //System.err.println("Socket exception when returning result of GET");
                //System.err.println("Reply packet will not be sent");
                ioexception.set(e);
            }
            return value;
        });
        mapLock.readLock().unlock();

        if (ioexception.get() != null) throw ioexception.get();

        return res.get();
    }

    private RequestCacheValue handleDelete(RequestCacheValue.Builder scaf, UnwrappedPayload payload) throws IOException {
        if((!payload.isKeyExists()) || payload.isValueExists() || payload.isVersionExists())
        {
            RequestCacheValue res = scaf.setResponseType(INVALID_OPTIONAL).build();
            sendResponse(res.generatePacket());
            return res;
        }

        //defensive design to reject 0 length keys
        if(payload.getKey().length <= 0)
        {
            RequestCacheValue res = scaf.setResponseType(INVALID_KEY).build();
            sendResponse(res.generatePacket());
            return res;
        }

        //atomically del and respond
        AtomicReference<IOException> ioexception = new AtomicReference<>();
        AtomicReference<RequestCacheValue> res = new AtomicReference<>();
        mapLock.readLock().lock();
        bytesUsedLock.lock();
        map.compute(new KeyWrapper(payload.getKey()), (key, value) -> {
            try {
                if (value == null) {
                    res.set(scaf.setResponseType(NO_KEY).build());
                    sendResponse(res.get().generatePacket());
                    return value;
                } else {
                    bytesUsed.addAndGet(-value.getValue().length);
                    res.set(scaf.setResponseType(DEL).build());
                    sendResponse(res.get().generatePacket());
                    return null;
                }
            } catch (IOException e) {
                //System.err.println(e);
                //System.err.println("Socket exception when returning result of GET");
                //System.err.println("Reply packet will not be sent");
                ioexception.set(e);
            }
            return value;
        });

        bytesUsedLock.unlock();
        mapLock.readLock().unlock();
        if (ioexception.get() != null) throw ioexception.get();

        return res.get();
    }

    private RequestCacheValue handleWipeout(RequestCacheValue.Builder scaf, UnwrappedPayload payload)
            throws IOException {
        if(payload.isValueExists() || payload.isVersionExists() || payload.isKeyExists())
        {
            RequestCacheValue res = scaf.setResponseType(INVALID_OPTIONAL).build();
            sendResponse(res.generatePacket());
            return res;
        }

        //atomically wipe and respond
        mapLock.writeLock().lock();
        bytesUsedLock.lock();
        map.clear();
        bytesUsed.set(0);
        bytesUsedLock.unlock();
        RequestCacheValue res = scaf.setResponseType(WIPEOUT).build();
        sendResponse(res.generatePacket());
        mapLock.writeLock().unlock();

        System.gc();

        return res;
    }


    // Custom Exceptions

    static class InvalidChecksumException extends Exception {}
    static class KeyLengthException extends Exception {}
    static class ValueLengthException extends Exception {}

}
