package com.g7.CPEN431.A12;

import com.g7.CPEN431.A12.cache.RequestCacheKey;
import com.g7.CPEN431.A12.cache.RequestCacheValue;
import com.g7.CPEN431.A12.client.KVClient;
import com.g7.CPEN431.A12.consistentMap.ConsistentMap;
import com.g7.CPEN431.A12.consistentMap.ServerRecord;
import com.g7.CPEN431.A12.map.KeyWrapper;
import com.g7.CPEN431.A12.map.ValueWrapper;
import com.g7.CPEN431.A12.newProto.KVMsg.KVMsg;
import com.g7.CPEN431.A12.newProto.KVMsg.KVMsgFactory;
import com.g7.CPEN431.A12.newProto.KVMsg.KVMsgSerializer;
import com.g7.CPEN431.A12.newProto.KVRequest.*;
import com.g7.CPEN431.A12.wrappers.UnwrappedMessage;
import com.g7.CPEN431.A12.wrappers.UnwrappedPayload;
import com.google.common.cache.Cache;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.zip.CRC32;

import static com.g7.CPEN431.A12.KVServer.*;
import static com.g7.CPEN431.A12.cache.ResponseType.*;
import static com.g7.CPEN431.A12.consistentMap.ServerRecord.CODE_DED;

public class KVServerTaskHandler implements Runnable {
    /* Thread parameters */
    private final AtomicLong lastReqTime;
    private final AtomicInteger bytesUsed;
    private final DatagramPacket iPacket;
    private final Cache<RequestCacheKey, DatagramPacket> requestCache;
    private boolean byteArrRet = false;


    /**
     * This is synchronized. You must obtain the maplock's readlock (See KVServer for full explanation)
     * if you wish to modify it. Wipeout obtains the writelock.
     */
    private final ConcurrentMap<KeyWrapper, ValueWrapper> map;
    private final ReadWriteLock mapLock;
    private boolean responseSent = false;
    private Timer timer;
    /**
     * Consistent map is thread safe. (Internally synchronized with R/W lock)
     */
    private ConsistentMap serverRing;

    final private BlockingQueue<byte[]> bytePool;  //this is thread safe
    final private boolean isOverloaded;
    final private DatagramSocket outSock;
    final private ConcurrentLinkedQueue<ServerRecord>pendingRecordDeaths;
    final private Semaphore keyUpdateRequested;
    //do not use
    ExecutorService threadPool;

    /* Constants */
    public final static int KEY_MAX_LEN = 32;
    public final static int VALUE_MAX_LEN = 10_000;
    public final static int CACHE_OVL_WAIT_TIME = 1000;   // Temporarily unused since cache doesn't overflow
    public final static int THREAD_OVL_WAIT_TIME = 500;

    /* Response Codes */
    public final static int RES_CODE_SUCCESS = 0x0;
    public final static int RES_CODE_NO_KEY = 0x1;
    public final static int RES_CODE_NO_MEM = 0x2;
    public final static int RES_CODE_OVERLOAD = 0x3;
    public final static int RES_CODE_INTERNAL_ER = 0x4;
    public final static int RES_CODE_INVALID_OPCODE = 0x5;
    public final static int RES_CODE_INVALID_KEY = 0x6;
    public final static int RES_CODE_INVALID_VALUE = 0x7;
    public final static int RES_CODE_INVALID_OPTIONAL = 0x21;
    public final static int RES_CODE_RETRY_NOT_EQUAL = 0x22;

    /* Request Codes */
    public final static int REQ_CODE_PUT = 0x01;

    public final static int REQ_CODE_BULKPUT = 0x200;
    public final static int REQ_CODE_GET = 0X02;
    public final static int REQ_CODE_DEL = 0X03;
    public final static int REQ_CODE_SHU = 0X04;
    public final static int REQ_CODE_WIP = 0X05;
    public final static int REQ_CODE_ALI = 0X06;
    public final static int REQ_CODE_PID = 0X07;
    public final static int REQ_CODE_MEM = 0X08;
    public final static int REQ_CODE_DED = 0x100;


    public final static int STAT_CODE_OK = 0x00;
    public final static int STAT_CODE_OLD = 0x01;
    public final static int STAT_CODE_NEW = 0x02;
    public KVServerTaskHandler(DatagramPacket iPacket,
                               Cache<RequestCacheKey, DatagramPacket> requestCache,
                               ConcurrentMap<KeyWrapper, ValueWrapper> map,
                               ReadWriteLock mapLock,
                               AtomicInteger bytesUsed,
                               BlockingQueue<byte[]> bytePool,
                               boolean isOverloaded,
                               ConsistentMap serverRing,
                               ConcurrentLinkedQueue<ServerRecord> pendingRecordDeaths,
                               ExecutorService threadPool,
                               AtomicLong lastReqTime,
                               Semaphore keyUpdateRequested,
                               Timer timer,
                               DatagramSocket outSock) {
        this.iPacket = iPacket;
        this.requestCache = requestCache;
        this.map = map;
        this.mapLock = mapLock;
        this.bytesUsed = bytesUsed;
        this.bytePool = bytePool;
        this.isOverloaded = isOverloaded;
        this.serverRing = serverRing;
        this.pendingRecordDeaths = pendingRecordDeaths;
        this.threadPool = threadPool;
        this.lastReqTime = lastReqTime;
        this.keyUpdateRequested = keyUpdateRequested;
        this.timer = timer;
        this.outSock = outSock;

    }

    // empty constructor for testing DeathUpdateTest
    public KVServerTaskHandler(ConsistentMap serverRing, ConcurrentLinkedQueue<ServerRecord> pendingRecordDeaths) {
        this.iPacket = null;
        this.requestCache = null;
        this.map = null;
        this.mapLock = null;
        this.bytesUsed = null;
        this.bytePool = null;
        this.isOverloaded = false;
        this.serverRing = serverRing;
        this.pendingRecordDeaths = pendingRecordDeaths;
        this.lastReqTime = new AtomicLong();
        this.keyUpdateRequested = null;
        try {
            this.outSock = new DatagramSocket();
        } catch (SocketException e) {
            throw new RuntimeException(e);
        }
    }

    // empty constructor for testing BulkPutTest
    public KVServerTaskHandler(ConcurrentMap<KeyWrapper, ValueWrapper> map, ReadWriteLock mapLock, AtomicInteger bytesUsed){
        this.iPacket = null;
        this.requestCache = null;
        this.map = map;
        this.mapLock = mapLock;
        this.bytesUsed = bytesUsed;
        this.bytePool = null;
        this.isOverloaded = false;
        this.serverRing = null;
        this.pendingRecordDeaths = null;
        this.lastReqTime = new AtomicLong();
        this.keyUpdateRequested = null;
        try {
            this.outSock = new DatagramSocket();
        } catch (SocketException e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public void run()
    {
        try {
            this.lastReqTime.set(Instant.now().toEpochMilli());
            mainHandlerFunction();
        } catch (Exception e) {
            System.err.println("Thread Crash");
            throw e;
        }
        finally {
            // Return shared objects to the pool
            if(!byteArrRet) bytePool.offer(iPacket.getData());
            byteArrRet = true;
        }
    }

    // used for testing in BulkPutTest
    public ConcurrentMap<KeyWrapper, ValueWrapper> getMap(){
        return this.map;
    }

    /**
     * Executes the main logic of the thread to process incoming requests and replies.
     */
    public void mainHandlerFunction() {
        if (responseSent) throw new IllegalStateException();

        //decode the message
        UnwrappedMessage unwrappedMessage;
        try {
            unwrappedMessage = unpackPacket(iPacket);
        } catch (IOException e) {
            System.err.println("Packet does not match .proto");
            System.err.println(e);
            System.err.println("Stopping packet handling and returning");

            //No response, like A1/A2
            return;
        } catch (InvalidChecksumException e) {
            System.err.println("Packet checksum does not match");
            System.err.println("Stopping packet handling and returning");

            //no response, like A1/A2
            return;
        }


        UnwrappedPayload payload;
        try {
            payload = unpackPayload(unwrappedMessage.getPayload());
        } catch (IOException e) {
            System.err.println("Unable to decode payload. Doing nothing");
            return;
        }

        /* check whether it is handled by self or will it be forwarded*/
        try {
            /* Forward if key exists and is not mapped to current server*/
            if(payload.hasKey())
            {
                byte[] key = payload.getKey();
                ConsistentMap.RTYPE type = serverRing.getRtype(key);


                //I am a replica, and the request is a write. Forward to primary.
                if(type != ConsistentMap.RTYPE.PRI)
                {
                    ServerRecord destination = serverRing.getReplicas(key).get(0);
                    if(unwrappedMessage.hasSourceAddress() || unwrappedMessage.hasSourceAddress())
                    {
                        //drop the packet due to misrouting.
                        System.err.println("misrouting detected, packet dropped");
                        return;
                    }
                    // Set source so packet will be sent to correct sender.
                    unwrappedMessage.setSourceAddress(iPacket.getAddress());
                    unwrappedMessage.setSourcePort(iPacket.getPort());
                    DatagramPacket p = unwrappedMessage.generatePacket(destination);
                    sendResponse(p);
                    return;
                }
            }
        } catch (ConsistentMap.NoServersException e) {
            System.err.println("There are no servers in the server ring");
            System.err.println("Doing nothing");
            return;
        }



        /* Prepare scaffolding for response */
        if(unwrappedMessage.hasSourceAddress() != unwrappedMessage.hasSourcePort())
        {
            System.err.println("The sender's source address did not have both address and port!");
            System.err.println("Doing nothing");
            return;
        }

        RequestCacheValue.Builder scaf;
        try {
            scaf = new RequestCacheValue.Builder(
                    unwrappedMessage.getCheckSum(),
                    unwrappedMessage.hasSourceAddress() ? InetAddress.getByAddress(unwrappedMessage.getSourceAddress()) : iPacket.getAddress(),
                    unwrappedMessage.hasSourcePort() ? unwrappedMessage.getSourcePort() : iPacket.getPort(),
                    unwrappedMessage.getMessageID()
            );
        } catch (UnknownHostException e) {
            System.err.println("Could not parse the forwarding address. Doing nothing");
            return;
        }



        /* Requests here can be handled locally */
//        if(!byteArrRet) bytePool.offer(iPacket.getData());
//        byteArrRet = true;



        DatagramPacket reply;
        try {
            reply = requestCache.get(new RequestCacheKey(unwrappedMessage.getMessageID(), unwrappedMessage.getCheckSum()),
                    () -> newProcessRequest(scaf, payload));
        } catch (ExecutionException e) {
            if(e.getCause() instanceof IOException)
            {
                System.err.println("Unable to decode payload. Doing nothing");
            }
            else {
                System.err.println(e);
            }
            return;
        }

        // Send the packet (if it was in cache, and the cache loading function was not called)
        // (tbh don't even need to synchronize load other than to avoid double loading)
        if (!responseSent) {
            sendResponse(reply);
        }
    }

    /**
     * The function that handles the request and returns responses after the message is unwrapped
     * @param payload The incoming payload
     * @param scaf The scaffolding for the response (pre-built)
     * @return The packet sent in response
     * @throws IOException If there are problems unpacking or packing into the public buffer
     */
    private DatagramPacket newProcessRequest(RequestCacheValue.Builder scaf, UnwrappedPayload payload) throws
            IOException{
        //verify overload condition
        if(isOverloaded) {
            System.out.println("Cache overflow. Delay Requested");
            RequestCacheValue res = scaf
                    .setResponseType(OVERLOAD_THREAD)
                    .build();
            return generateAndSend(res);
        }

        //process the packet by request code
        DatagramPacket res;
        switch(payload.getCommand())
        {
            case REQ_CODE_PUT: res = handlePut(scaf, payload); break;
            case REQ_CODE_GET: res = handleGet(scaf, payload); break;
            case REQ_CODE_DEL: res = handleDelete(scaf, payload); break;
            case REQ_CODE_SHU: res = handleShutdown(scaf, payload); break;
            case REQ_CODE_WIP: res = handleWipeout(scaf, payload);  break;
            case REQ_CODE_ALI: res = handleIsAlive(scaf, payload); break;
            case REQ_CODE_PID: res = handleGetPID(scaf, payload); break;
            case REQ_CODE_MEM: res = handleGetMembershipCount(scaf, payload);  break;
            case REQ_CODE_DED: res = handleDeathUpdate(scaf, payload); break;
            case REQ_CODE_BULKPUT: res = handleBulkPut(scaf, payload); break;

            default: {
                RequestCacheValue val = scaf.setResponseType(INVALID_OPCODE).build();
                res =  generateAndSend(val);
            }
        }

        //response should have been sent out by one of the above functions, so this is just extra checking.
        if(!responseSent) {
            System.out.println(payload.getCommand());
            throw new IllegalStateException();
        }
        return res;
    }

    /**
     * Helper function to unpack packet
     * @param iPacket incoming packet with client request
     * @return And unwrapped packet
     * @throws IOException If there was a problem parsing the content into the public buffer
     * @throws InvalidChecksumException If the checksum does not match
     */
    private UnwrappedMessage unpackPacket(DatagramPacket iPacket)
            throws IOException, InvalidChecksumException {

        KVMsg deserialized = KVMsgSerializer.parseFrom(new KVMsgFactory(), iPacket.getData(), 0, iPacket.getLength());

        if(!deserialized.hasMessageID() || !deserialized.hasPayload() || !deserialized.hasCheckSum())
        {
            throw new IOException("Message does not have required elements, skipping handling");
        }

        byte[] id = deserialized.getMessageID();
        byte[] pl = deserialized.getPayload();

        //verify checksum
        long actualCRC = getCRC(id, pl);
        if (actualCRC != deserialized.getCheckSum()) throw new InvalidChecksumException();

        return (UnwrappedMessage) deserialized;
    }

    private long getCRC(byte[] id, byte[] payload)
    {
        ByteBuffer b = ByteBuffer.allocate(id.length + payload.length);
        b.put(id);
        b.put(payload);

        b.flip();

        CRC32 crc32 = new CRC32();
        crc32.update(b.array());
        return crc32.getValue();
    }

    /**
     * unpacks the payload into an accesible format
     * @param payload
     * @return The unpacked object
     * @throws IOException If there was a problem unpacking it from the public buffer;
     */
    private UnwrappedPayload unpackPayload(byte[] payload) throws
            IOException{

        KVRequest deserialized = KVRequestSerializer.parseFrom(new KVRequestFactory(), payload);
        return (UnwrappedPayload) deserialized;
    }

    /**
     * Enqueues the packet to be sent by sender thread;
     * @param d Packet to send
     */
    private void sendResponse(DatagramPacket d)  {
        if (responseSent) throw new IllegalStateException();

        responseSent = true;
        try {
            outSock.send(d);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * @param res Prebuilt body to be sent
     * @return the packet to be sent.
     */
    DatagramPacket generateAndSend(RequestCacheValue res) {
        DatagramPacket pkt = res.generatePacket();
        sendResponse(pkt);
        return pkt;
    }
    //helper functions to process requests

    /**
     *  Sends the response if the request is for membership count
     * @param scaf Scaffold builder for Request CacheValue that is partially filled in (with IP / port etc.)
     * @param payload Payload from the client
     * @return The packet sent
     */
    private DatagramPacket handleGetMembershipCount(RequestCacheValue.Builder scaf, UnwrappedPayload payload) {
        if(payload.hasValue() || payload.hasVersion() || payload.hasKey())
        {
            RequestCacheValue res = scaf.setResponseType(INVALID_OPTIONAL).build();
            return generateAndSend(res);
        }

        RequestCacheValue res = scaf
                .setResponseType(MEMBERSHIP_COUNT)
                .setMembershipCount(serverRing.getServerCount())
                .build();

        return generateAndSend(res);
    }

    /**
     * Helper function that responds to GET PID Requests
     * @param scaf Scaffold builder for Request CacheValue that is partially filled in (with IP / port etc.)
     * @param payload Payload from the client
     * @return The packet sent
     */
    private DatagramPacket handleGetPID(RequestCacheValue.Builder scaf, UnwrappedPayload payload) {
        if(payload.hasValue() || payload.hasVersion() || payload.hasKey())
        {
            RequestCacheValue res = scaf.setResponseType(INVALID_OPTIONAL).build();
            return generateAndSend(res);
        }

        long pid = ProcessHandle.current().pid();
        RequestCacheValue res = scaf
                .setResponseType(PID)
                .setPID(pid)
                .build();

        return generateAndSend(res);
    }


    /**
     * Helper function that responds to is Alive Requests
     * @param scaf Scaffold builder for Request CacheValue that is partially filled in (with IP / port etc.)
     * @param payload Payload from the client
     * @return The packet sent
     */
    private DatagramPacket handleIsAlive(RequestCacheValue.Builder scaf, UnwrappedPayload payload) {
        if(payload.hasValue() || payload.hasVersion() || payload.hasKey())
        {
            RequestCacheValue res = scaf.setResponseType(INVALID_OPTIONAL).build();
            return generateAndSend(res);
        }

        RequestCacheValue res = scaf.setResponseType(ISALIVE).build();
        return generateAndSend(res);
    }


    /**
     * Helper function that responds to shutdown requests
     * @param scaf Scaffold builder for Request CacheValue that is partially filled in (with IP / port etc.)
     * @param payload Payload from the client
     * @return The packet sent
     */
    private DatagramPacket handleShutdown (RequestCacheValue.Builder scaf, UnwrappedPayload payload) throws IOException {
//        if(payload.hasValue() || payload.hasVersion() || payload.hasKey())
//        {
//            RequestCacheValue res = scaf.setResponseType(INVALID_OPTIONAL).build();
//            return generateAndSend(res);
//        }
//
//        RequestCacheValue res = scaf.setResponseType(SHUTDOWN).build();
//        DatagramPacket pkt = generateAndSend(res);
//
//        System.out.println("Recevied shutdown command, shutting down now");
        System.exit(0);
        return null;
//        return pkt;
    }

    /**
     * Helper function that responds to put requests
     * @param scaf Scaffold builder for Request CacheValue that is partially filled in (with IP / port etc.)
     * @param payload Payload from the client
     * @return The packet sent
     */
    private DatagramPacket handlePut(RequestCacheValue.Builder scaf, UnwrappedPayload payload) {
        if(!payload.hasKey() || !payload.hasValue())
        {
            RequestCacheValue res = scaf.setResponseType(INVALID_OPTIONAL).build();
            return generateAndSend(res);
        }

        //defensive design to reject 0 length keys
        if(payload.getKey().length == 0 || payload.getKey().length > KEY_MAX_LEN)
        {
            RequestCacheValue res = scaf.setResponseType(INVALID_KEY).build();
            return generateAndSend(res);
        }

        if(payload.getValue().length > VALUE_MAX_LEN)
        {
            RequestCacheValue res = scaf.setResponseType(INVALID_VALUE).build();
            return generateAndSend(res);
        }

        if(bytesUsed.get() >= MAP_SZ) {
            System.out.println("clearing memory");
            RequestCacheValue res = scaf.setResponseType(NO_MEM).build();
            //TODO UNSAFE, but shitty client so whatever...
            mapLock.writeLock().lock();
            map.clear();
            bytesUsed.set(0);
            mapLock.writeLock().unlock();
            return generateAndSend(res);
        }

        long insertionTime = System.currentTimeMillis();

        boolean forwardOK = forwardToReplica(payload, insertionTime);

        if(!forwardOK)
        {
            RequestCacheValue res = scaf.setResponseType(OVERLOAD_CACHE).build();
            return generateAndSend(res);
        }

        mapLock.readLock().lock();

        //atomically put and respond, `tis thread safe.
        AtomicReference<IOException> ioexception= new AtomicReference<>();
        AtomicReference<DatagramPacket> pkt = new AtomicReference<>();
        bytesUsed.addAndGet(payload.getValue().length);
        ValueWrapper old = map.put(new KeyWrapper(payload.getKey()), new ValueWrapper(payload.getValue(), payload.getVersion(), insertionTime));
        if(old != null) bytesUsed.addAndGet(old.getValue().length);
        mapLock.readLock().unlock();

        RequestCacheValue res = scaf.setResponseType(PUT).build();
        return generateAndSend(res);
    }

    private boolean forwardToReplica (UnwrappedPayload payload, long insertionTime)
    {
        KVPair pair;

        //handle different callees
        if(payload.getCommand() == REQ_CODE_PUT)
        {
            assert payload.hasKey();
            assert payload.hasValue();
            pair = new KVPair(payload.getKey(), payload.getValue(), payload.getVersion(), insertionTime);
        }
        else if (payload.getCommand() == REQ_CODE_DEL)
        {
            assert payload.hasKey();
            //null -> delete.
            pair = new KVPair(payload.getKey(), null, payload.getVersion(), insertionTime);
        }
        else
        {
            throw new IllegalStateException("Function called by some idiot where it is not supposed to be");
        }



        //get replicas
        List<ServerRecord> replicas = serverRing.getReplicas(payload.getKey());
        //remove myself
        replicas.remove(self);
        replicas.remove(selfLoopback);

        //construct list
        List<PutPair> pairToForward = new ArrayList<>();
        pairToForward.add(pair);

        //for each replica - create a task and submit to thread pool.
        byte[] msg = KVClient.bulkPutPumpStatic(pairToForward);
        for(ServerRecord server : replicas)
        {
            DatagramPacket p = new DatagramPacket(msg, msg.length, server.getAddress(), server.getPort());
            try {
                outSock.send(p);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        return true;
    }

    /**
     * Handle a bulk put operation, response contains a server status code list which indicates the status of each individual put
     * @param scaf Scaffold builder for Request CacheValue that is partially filled in (with IP / port etc.)
     * @param payload Payload from the client
     * @return The packet sent
     */
    private DatagramPacket handleBulkPut (RequestCacheValue.Builder scaf, UnwrappedPayload payload) {
        if(!payload.hasPutPair())
        {
            RequestCacheValue res = scaf.setResponseType(INVALID_OPTIONAL).build();
            return generateAndSend(res);
        }



        bulkPutHelper(payload.getPutPair());
        RequestCacheValue res = scaf.setResponseType(ISALIVE).build();
        return generateAndSend(res);
    }

    //TODO fix later
    public void bulkPutHelper(List<PutPair> pairs){
        assert map != null;
        assert mapLock != null;
        assert bytesUsed != null;


        for (PutPair pair: pairs) {
            if(!pair.hasKey() )
            {
                throw new IllegalArgumentException("Pair does not have fields");
            }

            //defensive design to reject 0 length keys
            if(pair.getKey().length == 0 || pair.getKey().length > KEY_MAX_LEN)
            {
                throw new IllegalArgumentException("0 Length keys detected");
            }

            if(pair.hasValue() && pair.getValue().length > VALUE_MAX_LEN)
            {
                throw new IllegalArgumentException("Length too long detected");
            }

            if(bytesUsed.get() >= MAP_SZ) {
                throw new IllegalStateException("Server should be empty on startup");
            }

            mapLock.readLock().lock();

            map.compute(new KeyWrapper(pair.getKey()), (key, value) -> {
                //delete operation

                if(!pair.hasValue())
                {
                    if (value != null) bytesUsed.addAndGet(-value.getValue().length);
                    return null;
                }
                else if (pair.getInsertionTime() >= (value == null ? 0 : value.getInsertTime())) // put operation
                {
                    int oldlen = value == null ? 0 : value.getValue().length;
                    bytesUsed.addAndGet(pair.getValue().length - oldlen);
                    return new ValueWrapper(pair.getValue(), pair.getVersion(), pair.getInsertionTime());
                }
                //keep old value
                else {
                    return value;
                }
            });

            mapLock.readLock().unlock();
        }
    }

    /**
     * Helper function that responds to get requests
     * @param scaf Scaffold builder for Request CacheValue that is partially filled in (with IP / port etc.)
     * @param payload Payload from the client
     * @return The packet sent
     */
    private DatagramPacket handleGet(RequestCacheValue.Builder scaf, UnwrappedPayload payload) {
        if((!payload.hasKey()) || payload.hasValue() || payload.hasVersion())
        {
            RequestCacheValue res = scaf.setResponseType(INVALID_OPTIONAL).build();
            return generateAndSend(res);
        }

        //defensive design to reject 0 length keys
        if(payload.getKey().length == 0 || payload.getKey().length > KEY_MAX_LEN)
        {
            RequestCacheValue res = scaf.setResponseType(INVALID_KEY).build();
            return generateAndSend(res);
        }

        //atomically get and respond
        mapLock.readLock().lock();
        ValueWrapper v = map.get(new KeyWrapper(payload.getKey()));
        mapLock.readLock().unlock();


        RequestCacheValue res;
        if (v == null) {
            res = scaf.setResponseType(NO_KEY).build();
        } else {
            res = scaf
                    .setResponseType(VALUE)
                    .setValue(v)
                    .build();
        }
        return generateAndSend(res);
    }

    /**
     * Helper function that responds delete requests
     * @param scaf Scaffold builder for Request CacheValue that is partially filled in (with IP / port etc.)
     * @param payload Payload from the client
     * @return The packet sent
     */
    private DatagramPacket handleDelete(RequestCacheValue.Builder scaf, UnwrappedPayload payload) {
        if((!payload.hasKey()) || payload.hasValue() || payload.hasVersion())
        {
            RequestCacheValue res = scaf.setResponseType(INVALID_OPTIONAL).build();
            return generateAndSend(res);
        }

        //defensive design to reject 0 length keys
        if(payload.getKey().length == 0 || payload.getKey().length > KEY_MAX_LEN)
        {
            RequestCacheValue res = scaf.setResponseType(INVALID_KEY).build();
            return generateAndSend(res);
        }

        boolean forwardOK = forwardToReplica(payload, System.currentTimeMillis());
        if(!forwardOK)
        {
            RequestCacheValue res = scaf.setResponseType(OVERLOAD_THREAD).build();
            return generateAndSend(res);
        }

        //atomically del and respond
        AtomicReference<DatagramPacket> pkt = new AtomicReference<>();
        mapLock.readLock().lock();
        ValueWrapper v = map.remove(new KeyWrapper(payload.getKey()));
        mapLock.readLock().unlock();

        RequestCacheValue res;

        if (v == null) {
            res = scaf.setResponseType(NO_KEY).build();
        } else {
            bytesUsed.addAndGet(-v.getValue().length);
            res = scaf.setResponseType(DEL).build();
        }

        return generateAndSend(res);
    }

    /**
     * Helper function that responds to wipeout requests
     * @param scaf Scaffold builder for Request CacheValue that is partially filled in (with IP / port etc.)
     * @param payload Payload from the client
     * @return The packet sent
     */
    private DatagramPacket handleWipeout(RequestCacheValue.Builder scaf, UnwrappedPayload payload){
        if(payload.hasValue() || payload.hasVersion() || payload.hasKey())
        {
            RequestCacheValue res = scaf.setResponseType(INVALID_OPTIONAL).build();
            return generateAndSend(res);
        }


        //atomically wipe and respond
        mapLock.writeLock().lock();
        map.clear();
        bytesUsed.set(0); //concurrently correct, because we are only thread with access to map
        RequestCacheValue res = scaf.setResponseType(WIPEOUT).build();
        DatagramPacket pkt = generateAndSend(res);
        mapLock.writeLock().unlock();

        System.gc();

        return pkt;
    }

    /**
     * Death request handler that updates the status of the ring
     * @param scaf: response object builder
     * @param payload: the payload from the request
     * @return the return packet sent back to the sender
     */
    private DatagramPacket handleDeathUpdate(RequestCacheValue.Builder scaf, UnwrappedPayload payload)
    {
        /* retrieve the list of obituaries that the sender knows */
        List<ServerEntry> deadServers = payload.getServerRecord();
        List<Integer> serverStatusCodes = getDeathCodes(deadServers, self);

        DatagramPacket pkt = null;
        ValueWrapper value = null;

        /* create response packet for receiving news */
        RequestCacheValue response = scaf.setResponseType(OBITUARIES).setServerStatusCodes(serverStatusCodes).build();
        pkt = generateAndSend(response);
        return pkt;
    }

    // TODO: needs to be changed back to private after testing
    public List<Integer> getDeathCodes(List<ServerEntry> deadServers, ServerRecord us) {
        List<Integer> serverStatusCodes = new ArrayList<>();
        boolean serverRingUpdated = false;
        for (ServerEntry server: deadServers) {
            //verify that the server in the death update is not us
            if (!us.equals(server) && !selfLoopback.equals(server)) {
                /* retrieve server address and port */
                boolean stateChanged = serverRing.updateServerState((ServerRecord) server);
                serverStatusCodes.add(stateChanged ? STAT_CODE_NEW : STAT_CODE_OLD);

                if(stateChanged)
                {
                    serverRingUpdated = true;
                }
            }
            //the server update is about us
            else {
                if (server.getCode() == CODE_DED) {
                    ServerRecord r = ((ServerRecord) server);
                    if(self.getInformationTime() > r.getInformationTime() + 10_000)
                    {
                        //do nothing
                    }
                    else
                    {
                        r.setAliveAtTime(r.getInformationTime() + 10_000);
                        serverRing.updateServerState(r);
                    }
                    serverStatusCodes.add(STAT_CODE_OLD);
                } else {
                    //continue propagating the message
                    serverStatusCodes.add(STAT_CODE_NEW);
                }
            }
        }

        /* Key transfer after ring state is up-to-date */
        if(serverRingUpdated && keyUpdateRequested.tryAcquire())
        {
            transferKeys();
        }

        return serverStatusCodes;
    }

    /**
     * Finds keys for which current server is successor and transfers them to the new server
     * Should be executed after the map state is updated.
     */

    private void transferKeys() {
         timer.schedule(new KeyTransferHandler(mapLock, map, bytesUsed, serverRing, pendingRecordDeaths, keyUpdateRequested), SETTLE_TIME);

    }

    // Custom Exceptions

    static class InvalidChecksumException extends Exception {}
}
