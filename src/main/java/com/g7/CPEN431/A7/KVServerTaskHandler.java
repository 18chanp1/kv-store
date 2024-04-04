package com.g7.CPEN431.A7;

import com.g7.CPEN431.A7.cache.RequestCacheKey;
import com.g7.CPEN431.A7.cache.RequestCacheValue;
import com.g7.CPEN431.A7.cache.ResponseType;
import com.g7.CPEN431.A7.client.KVClient;
import com.g7.CPEN431.A7.client.ServerResponse;
import com.g7.CPEN431.A7.consistentMap.ConsistentMap;
import com.g7.CPEN431.A7.consistentMap.ForwardList;
import com.g7.CPEN431.A7.consistentMap.ServerRecord;
import com.g7.CPEN431.A7.map.KeyWrapper;
import com.g7.CPEN431.A7.map.ValueWrapper;
import com.g7.CPEN431.A7.newProto.KVMsg.KVMsg;
import com.g7.CPEN431.A7.newProto.KVMsg.KVMsgFactory;
import com.g7.CPEN431.A7.newProto.KVMsg.KVMsgSerializer;
import com.g7.CPEN431.A7.newProto.KVRequest.*;
import com.g7.CPEN431.A7.newProto.KVResponse.KVResponse;
import com.g7.CPEN431.A7.wrappers.PB_ContentType;
import com.g7.CPEN431.A7.wrappers.PublicBuffer;
import com.g7.CPEN431.A7.wrappers.UnwrappedMessage;
import com.g7.CPEN431.A7.wrappers.UnwrappedPayload;
import com.google.common.cache.Cache;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.*;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;

import static com.g7.CPEN431.A7.KVServer.*;
import static com.g7.CPEN431.A7.cache.ResponseType.*;
import static com.g7.CPEN431.A7.consistentMap.ServerRecord.CODE_ALI;
import static com.g7.CPEN431.A7.consistentMap.ServerRecord.CODE_DED;

public class KVServerTaskHandler implements Runnable {
    /* Thread parameters */
    private final AtomicLong lastReqTime;
    private final AtomicInteger bytesUsed;
    private final DatagramPacket iPacket;
    private final Cache<RequestCacheKey, DatagramPacket> requestCache;

    /**
     * This is synchronized. You must obtain the maplock's readlock (See KVServer for full explanation)
     * if you wish to modify it. Wipeout obtains the writelock.
     */
    private final ConcurrentMap<KeyWrapper, ValueWrapper> map;
    private final ReadWriteLock mapLock;
    private boolean responseSent = false;
    private PublicBuffer incomingPublicBuffer;
    /**
     * Consistent map is thread safe. (Internally synchronized with R/W lock)
     */
    private ConsistentMap serverRing;

    final private ConcurrentLinkedQueue<byte[]> bytePool;  //this is thread safe
    final private boolean isOverloaded;
    final private ConcurrentLinkedQueue<DatagramPacket> outbound;
    final private ConcurrentLinkedQueue<ServerRecord>pendingRecordDeaths;
    ExecutorService threadPool;
    KVClient sender;

    private Set<ServerRecord> myBackupServers;
    private Set<ServerRecord> backupServersFor;


    /* Constants */
    public final static int KEY_MAX_LEN = 32;
    public final static int VALUE_MAX_LEN = 10_000;
    public final static int CACHE_OVL_WAIT_TIME = 80;   // Temporarily unused since cache doesn't overflow
    public final static int THREAD_OVL_WAIT_TIME = 80;

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
    public final static int REQ_CODE_BKP = 0x101;
    public final static int REQ_CODE_PRI = 0x102;



    public final static int STAT_CODE_OK = 0x00;
    public final static int STAT_CODE_OLD = 0x01;
    public final static int STAT_CODE_NEW = 0x02;


    public KVServerTaskHandler(DatagramPacket iPacket,
                               Cache<RequestCacheKey, DatagramPacket> requestCache,
                               ConcurrentMap<KeyWrapper, ValueWrapper> map,
                               ReadWriteLock mapLock,
                               AtomicInteger bytesUsed,
                               ConcurrentLinkedQueue<byte[]> bytePool,
                               boolean isOverloaded,
                               ConcurrentLinkedQueue<DatagramPacket> outbound,
                               ConsistentMap serverRing,
                               ConcurrentLinkedQueue<ServerRecord> pendingRecordDeaths,
                               ExecutorService threadPool,
                               AtomicLong lastReqTime) throws IOException, KVClient.MissingValuesException, KVClient.ServerTimedOutException, InterruptedException {
        this.iPacket = iPacket;
        this.requestCache = requestCache;
        this.map = map;
        this.mapLock = mapLock;
        this.bytesUsed = bytesUsed;
        this.bytePool = bytePool;
        this.isOverloaded = isOverloaded;
        this.outbound = outbound;
        this.serverRing = serverRing;
        this.pendingRecordDeaths = pendingRecordDeaths;
        this.threadPool = threadPool;
        this.lastReqTime = lastReqTime;
        this.sender = new KVClient(null, 0, new DatagramSocket(),new byte[16384]);

        /* Assign our replica servers */
        this.myBackupServers = serverRing.assignInitialBackupServers(self);
        this.backupServersFor = new HashSet<>();

//        //Server ring should have initialized my backup servers
//        //Let the backup servers know that we are the primary
//        for (int retry = 0; retry < 2; retry++) {
//            try {
//                for (ServerRecord serverRecord : self.getMyBackupServers()) {
//                    sender.setDestination(serverRecord.getAddress(), serverRecord.getPort());
//                    sender.isPrimary(self);
//                }
//                return;
//            } catch (KVClient.MissingValuesException e) {
//                System.out.println("Backup timed out while trying to notify them we are primary");
//                e.printStackTrace();
//            }
//        }
//        System.out.println("Failed to notify backups that we are primary");
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
        this.outbound = null;
        this.serverRing = serverRing;
        this.pendingRecordDeaths = pendingRecordDeaths;
        this.lastReqTime = new AtomicLong();
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
        this.outbound = null;
        this.serverRing = null;
        this.pendingRecordDeaths = null;
        this.lastReqTime = new AtomicLong();
    }

    //empty constructor for testing ProcessDeadBackupTest
    public KVServerTaskHandler(ConcurrentMap<KeyWrapper, ValueWrapper> map, ReadWriteLock mapLock, ConsistentMap serverRing) {
        this.iPacket = null;
        this.requestCache = null;
        this.map = map;
        this.mapLock = mapLock;
        this.bytesUsed = null;
        this.bytePool = null;
        this.isOverloaded = false;
        this.outbound = null;
        this.serverRing = serverRing;
        this.pendingRecordDeaths = null;
        this.lastReqTime = new AtomicLong();
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
            bytePool.offer(iPacket.getData());
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
            payload = unpackPayload(incomingPublicBuffer);
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
                ServerRecord destination = serverRing.getServer(key);

                //Primary server is dead, I am a replica, and its just a get request
                //If handleWithReplica true, then we will handle the request
                //Else, it gets forwarded to primary
                boolean handleWithReplica = !destination.isAlive() && payload.getCommand() == REQ_CODE_GET && backupServersFor.contains(destination);

                if((!destination.equals(self)) && (!destination.equals(selfLoopback)) && !handleWithReplica)
                {
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
                    unwrappedMessage.getMessageID(),
                    incomingPublicBuffer);
        } catch (UnknownHostException e) {
            System.err.println("Could not parse the forwarding address. Doing nothing");
            return;
        }



        /* Requests here can be handled locally */
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
            IOException, KVClient.MissingValuesException, KVClient.ServerTimedOutException, InterruptedException {
        //verify overload condition
        if(isOverloaded) {
            //System.out.println("Cache overflow. Delay Requested");
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
            case REQ_CODE_BKP: res = handleIsBackup(scaf, payload); break;
            case REQ_CODE_PRI: res = handleIsPrimary(scaf, payload); break;

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

        incomingPublicBuffer = new PublicBuffer(iPacket.getData(), PB_ContentType.PACKET, iPacket.getLength());

        KVMsg deserialized = KVMsgSerializer.parseFrom(new KVMsgFactory(),
                incomingPublicBuffer.readPacketFromPB());

        if(!deserialized.hasMessageID() || !deserialized.hasPayload() || !deserialized.hasCheckSum())
        {
            throw new IOException("Message does not have required elements, skipping handling");
        }

        byte[] id = deserialized.getMessageID();
        byte[] pl = deserialized.getPayload();

        incomingPublicBuffer.writeIDToPB().write(id);
        incomingPublicBuffer.writePayloadToPBAfterID().write(pl);

        //verify checksum
        long actualCRC = incomingPublicBuffer.getCRCFromBody();
        if (actualCRC != deserialized.getCheckSum()) throw new InvalidChecksumException();

        return (UnwrappedMessage) deserialized;
    }

    /**
     * unpacks the payload into an accesible format
     * @param payload
     * @return The unpacked object
     * @throws IOException If there was a problem unpacking it from the public buffer;
     */
    private UnwrappedPayload unpackPayload(PublicBuffer payload) throws
            IOException{

        KVRequest deserialized = KVRequestSerializer.parseFrom(new KVRequestFactory(), payload.readPayloadFromPBBody());
        return (UnwrappedPayload) deserialized;
    }

    /**
     * Enqueues the packet to be sent by sender thread;
     * @param d Packet to send
     */
    private void sendResponse(DatagramPacket d)  {
        if (responseSent) throw new IllegalStateException();

        responseSent = true;
        outbound.offer(d);
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
    private DatagramPacket handlePut(RequestCacheValue.Builder scaf, UnwrappedPayload payload) throws IOException, KVClient.MissingValuesException, KVClient.ServerTimedOutException, InterruptedException {
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
            RequestCacheValue res = scaf.setResponseType(NO_MEM).build();
            //TODO UNSAFE, but shitty client so whatever...
            mapLock.writeLock().lock();
            map.clear();
            bytesUsed.set(0);
            mapLock.writeLock().unlock();
            return generateAndSend(res);
        }

        mapLock.readLock().lock();
        AtomicReference<DatagramPacket> pkt = new AtomicReference<>();
        ServerRecord primaryServer = (ServerRecord) payload.getPrimaryServer();

        //We are the primary server
        if (primaryServer == null) {
            //atomically put and respond, `tis thread safe.
            AtomicReference<IOException> ioexception= new AtomicReference<>();
            map.compute(new KeyWrapper(payload.getKey()), (key, value) -> {
                RequestCacheValue res = scaf.setResponseType(PUT).build();
                pkt.set(generateAndSend(res));
                bytesUsed.addAndGet(payload.getValue().length);
                return new ValueWrapper(payload.getValue(), payload.getVersion(), payload.getPrimaryServer());
            });
            mapLock.readLock().unlock();

            for (ServerRecord backup : myBackupServers) {
                sender.setDestination(backup.getAddress(), backup.getPort());
                updateBackupServer(payload, self);
            }

        //Primary is updating a replica server
        } else if(primaryServer.equals(payload.getSender())) {
            //Put this primary in our list of servers we are doing back up for
            backupServersFor.add((ServerRecord) payload.getSender());

            AtomicReference<IOException> ioexception= new AtomicReference<>();
            map.compute(new KeyWrapper(payload.getKey()), (key, value) -> {
                RequestCacheValue res = scaf.setResponseType(PUT).build();
                pkt.set(generateAndSend(res));
                bytesUsed.addAndGet(payload.getValue().length);
                return new ValueWrapper(payload.getValue(), payload.getVersion(), payload.getPrimaryServer());
            });
            mapLock.readLock().unlock();
        }
        else {
            System.out.println("Non-primary server is handling a put request, smething is wrong");
            //redirect to primary server
            sender.setDestination(primaryServer.getAddress(), primaryServer.getPort());
            try {
                sender.put(payload.getKey(), payload.getValue(), payload.getVersion(), null);
                RequestCacheValue res = scaf.setResponseType(PUT).build();
                pkt.set(generateAndSend(res));
            } catch(KVClient.ServerTimedOutException e) {
                RequestCacheValue res = scaf.setResponseType(TIMEOUT).build();
                pkt.set(generateAndSend(res));
            }
        }


        return pkt.get();
    }

    /**
     * Handle a bulk put operation, response contains a server status code list which indicates the status of each individual put
     * @param scaf Scaffold builder for Request CacheValue that is partially filled in (with IP / port etc.)
     * @param payload Payload from the client
     * @return The packet sent
     */
    private DatagramPacket handleBulkPut (RequestCacheValue.Builder scaf, UnwrappedPayload payload) throws IOException, KVClient.MissingValuesException, KVClient.ServerTimedOutException, InterruptedException {
        if(!payload.hasPutPair())
        {
            RequestCacheValue res = scaf.setResponseType(INVALID_OPTIONAL).build();
            return generateAndSend(res);
        }

        //We are primary - replica is sending us our values after we came alive
        if (payload.getPrimaryServer() == null) {
            //Do nothing

        //We are a new replica server - primary is sending us values to backup
        } else {
            backupServersFor.add((ServerRecord) payload.getSender());
        }

        bulkPutHelper(payload);
        RequestCacheValue res = scaf.setResponseType(ISALIVE).build();

        return generateAndSend(res);
    }

    //TODO fix later
    public void bulkPutHelper(UnwrappedPayload payload){
        assert map != null;
        assert mapLock != null;
        assert bytesUsed != null;

        List<PutPair> pairs = payload.getPutPair();
        for (PutPair pair: pairs) {
            if(!pair.hasKey() || !pair.hasValue())
            {
                throw new IllegalArgumentException("Pair does not have fields");
            }

            //defensive design to reject 0 length keys
            if(pair.getKey().length == 0 || pair.getKey().length > KEY_MAX_LEN)
            {
                throw new IllegalArgumentException("0 Length keys detected");
            }

            if(pair.getValue().length > VALUE_MAX_LEN)
            {
                throw new IllegalArgumentException("Length too long detected");
            }

            if(bytesUsed.get() >= MAP_SZ) {
                throw new IllegalStateException("Server should be empty on startup");
            }

            mapLock.readLock().lock();

            map.compute(new KeyWrapper(pair.getKey()), (key, value) -> {
                bytesUsed.addAndGet(pair.getValue().length);
                return new ValueWrapper(pair.getValue(), pair.getVersion(), payload.getPrimaryServer());
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
        AtomicReference<DatagramPacket> pkt = new AtomicReference<>();
        map.compute(new KeyWrapper(payload.getKey()), (key, value) -> {
            RequestCacheValue res;
            if (value == null) {
                res = scaf.setResponseType(NO_KEY).build();
            } else {
                res = scaf
                        .setResponseType(VALUE)
                        .setValue(value)
                        .build();
            }
            pkt.set(generateAndSend(res));
            return value;
        });
        mapLock.readLock().unlock();

        return pkt.get();
    }

    /**
     * Helper function that responds delete requests
     * @param scaf Scaffold builder for Request CacheValue that is partially filled in (with IP / port etc.)
     * @param payload Payload from the client
     * @return The packet sent
     */
    private DatagramPacket handleDelete(RequestCacheValue.Builder scaf, UnwrappedPayload payload) throws IOException, KVClient.MissingValuesException, KVClient.ServerTimedOutException, InterruptedException {
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

        //atomically del and respond
        AtomicReference<DatagramPacket> pkt = new AtomicReference<>();
        mapLock.readLock().lock();
        ServerRecord primaryServer = (ServerRecord) payload.getPrimaryServer();

        if (primaryServer == null) {
            map.compute(new KeyWrapper(payload.getKey()), (key, value) -> {
                if (value == null) {
                    RequestCacheValue res = scaf.setResponseType(NO_KEY).build();
                    pkt.set(generateAndSend(res));
                } else {
                    bytesUsed.addAndGet(-value.getValue().length);
                    RequestCacheValue res = scaf.setResponseType(DEL).build();
                    pkt.set(generateAndSend(res));
                }
                return null;
            });
            mapLock.readLock().unlock();
            for (ServerRecord backup : myBackupServers) {
                sender.setDestination(backup.getAddress(), backup.getPort());
                updateBackupServer(payload, null);
            }
        } else {
            sender.setDestination(primaryServer.getAddress(), primaryServer.getPort());
            try {
                sender.delete(payload.getKey());
                RequestCacheValue res = scaf.setResponseType(DEL).build();
                pkt.set(generateAndSend(res));
            } catch (KVClient.ServerTimedOutException e) {
                RequestCacheValue res = scaf.setResponseType(TIMEOUT).build();
                pkt.set(generateAndSend(res));
            }
        }


        return pkt.get();
    }

    /**
     * Helper function that responds to wipeout requests
     * @param scaf Scaffold builder for Request CacheValue that is partially filled in (with IP / port etc.)
     * @param payload Payload from the client
     * @return The packet sent
     */
    private DatagramPacket handleWipeout(RequestCacheValue.Builder scaf, UnwrappedPayload payload) {
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
    private DatagramPacket handleDeathUpdate(RequestCacheValue.Builder scaf, UnwrappedPayload payload) throws KVClient.MissingValuesException, IOException, KVClient.ServerTimedOutException, InterruptedException {
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
    public List<Integer> getDeathCodes(List<ServerEntry> deadServers, ServerRecord us) throws IOException, KVClient.MissingValuesException, KVClient.ServerTimedOutException, InterruptedException {
        List<Integer> serverStatusCodes = new ArrayList<>();
        assert pendingRecordDeaths != null;
        boolean serverRingUpdated = false;
        for (ServerEntry server: deadServers) {

            ServerRecord serverRecord = (ServerRecord) server;
            //verify that the server in the death update is not us
            if (!us.equals(server) && !selfLoopback.equals(server)) {
                /* retrieve server address and port */
                boolean updated = serverRing.updateServerState(serverRecord);
                serverStatusCodes.add(updated ? STAT_CODE_NEW : STAT_CODE_OLD);

                if (updated)
                {
                    // if its news that a server is dead
                    if (server.getCode() == CODE_DED) {
                        /* check is dead server is one of out backup servers. if it is,
                       remove the dead server from our backup list, get a new backup server
                       and update our backup server list. also update the backupServerFor of the
                       new backup server.
                       */
                        if (myBackupServers.contains(serverRecord)) {
                            // get a new backup server and handle backup lists
                            ServerRecord newBackupServer = processDeadBackupServer(serverRecord, us);

                            //get our put-pairs to send to the new backup server
                            List<PutPair> ourPutPairs = getOurPutPairs();

                            // send the list of put pairs in our KVStore to the new backup server using bulkPut
                            sender.setDestination(newBackupServer.getAddress(), newBackupServer.getPort());
                            // change primary server value
                            sender.bulkPut(ourPutPairs, self);
                        }
                    } else {
                        // news that a server that's not us is alive
                        //check if it is a server that we are a backup for
                        if (backupServersFor.contains(serverRecord)) {
                            // get the primary server's put pairs to send back
                            List<PutPair> putPairsOfNewAliveServer = getPutPairsOfPrimaryServer(serverRecord);

                            // send the list of put pairs in our KVStore to the new backup server using bulkPut, and declare that we are one of their backup servers
                            sender.setDestination(serverRecord.getAddress(), serverRecord.getPort());
                            sender.bulkPut(putPairsOfNewAliveServer, null);
                            sender.isBackup(self);
                        }
                    }

                    serverRingUpdated = true;
                    pendingRecordDeaths.add(serverRecord);
                }
            }
            //the server update is about us
            else {
                if (server.getCode() == CODE_DED) {
                    ServerRecord r = ((ServerRecord) server);
                    if(self.getInformationTime() > r.getInformationTime() + 10_000)
                    {
                        //do nothing
                        pendingRecordDeaths.add(self);
                    }
                    else
                    {
                        r.setAliveAtTime(r.getInformationTime() + 10_000);
                        serverRing.updateServerState(r);
                        pendingRecordDeaths.add(r);
                    }
                    serverStatusCodes.add(STAT_CODE_OLD);
                } else {
                    //continue propagating the message
                    serverStatusCodes.add(STAT_CODE_NEW);
                }
            }
        }

        /* Key transfer after ring state is up-to-date */
        if(serverRingUpdated)
        {
            transferKeys();
        }

        return serverStatusCodes;
    }

    /**
     * This function should be called upon receiving a write operation. It updates the backup
     * servers by using the client to send the change detailed in the payload.
     * For non-write operations (e.g. GET), does nothing.
     * @param payload The payload with the write operation (PUT, WIPEOUT, DELETE, BULKPUT)
     * @param primaryServer Optional. For bulk put operation
     */
    public void updateBackupServer(UnwrappedPayload payload, ServerRecord primaryServer) throws KVClient.MissingValuesException, IOException, KVClient.ServerTimedOutException, InterruptedException {
            switch(payload.getCommand())
            {
                case REQ_CODE_PUT:
                    sender.put(payload.getKey(), payload.getValue(), payload.getVersion(), primaryServer); break;
                case REQ_CODE_DEL:
                    sender.delete(payload.getKey()); break;
                case REQ_CODE_WIP:
                    sender.wipeout();  break;
                case REQ_CODE_BULKPUT:
                    sender.bulkPut(payload.getPutPair(), primaryServer); break;

                default: {
                    System.out.println("Req code " + payload.getCommand() + " received, doesn't require updating backups");
                }
            }
    }

    /**
     * Helper function to handle a dead server that is our backup server.
     * @param deadServer the dead backup server
     * @param us current server
     * @return returns the new backup server
     */
    public ServerRecord processDeadBackupServer(ServerRecord deadServer, ServerRecord us) throws KVClient.MissingValuesException, IOException, KVClient.ServerTimedOutException, InterruptedException {
        //remove dead server from primary server's backup list
        myBackupServers.remove(deadServer);
        // find new backup server
        ServerRecord newBackupServer = serverRing.findBackupServer(myBackupServers, us);

        // update primary server's backup list
        myBackupServers.add(newBackupServer);

        // bulk send our current values to this new replica - this is done in getDeathCodes()

        return newBackupServer;
    }

    /**
     * put pairs in our KVstore that are we are the primary server of
     * @return list of pairs
     */
    public List<PutPair> getOurPutPairs(){
        assert map != null;
        List<PutPair> ourPutPairs = new ArrayList<>();
        for (Map.Entry<KeyWrapper, ValueWrapper> wrapperEntry: map.entrySet())  {
            // check if the KV pair is our (not a backup copy for a different server)
            if (wrapperEntry.getValue().getPrimaryServer() == null) {
                PutPair pair = new KVPair(wrapperEntry.getKey().getKey(), wrapperEntry.getValue().getValue(), wrapperEntry.getValue().getVersion());
                ourPutPairs.add(pair);
            }
        }
        return ourPutPairs;
    }

    /**
     * Helper function for handling a server that comes back alive and we are its backup server
     * @param alivePrimaryServer the server that came back alive
     * @return list of pairs
     */
    public List<PutPair> getPutPairsOfPrimaryServer(ServerRecord alivePrimaryServer) throws IOException, KVClient.MissingValuesException, KVClient.ServerTimedOutException, InterruptedException {
        List<PutPair> putPairsOfNewAliveServer = new ArrayList<>();

        assert map != null;
        for (Map.Entry<KeyWrapper, ValueWrapper> wrapperEntry: map.entrySet())  {
            if (wrapperEntry.getValue().getPrimaryServer() == alivePrimaryServer) {
                PutPair pair = new KVPair(wrapperEntry.getKey().getKey(), wrapperEntry.getValue().getValue(), wrapperEntry.getValue().getVersion());
                putPairsOfNewAliveServer.add(pair);
            }
        }

        return putPairsOfNewAliveServer;
    }

    private DatagramPacket handleIsBackup(RequestCacheValue.Builder scaf, UnwrappedPayload payload){
        if(!payload.hasSender()){
            RequestCacheValue res = scaf.setResponseType(INVALID_OPTIONAL).build();
            return generateAndSend(res);
        }

        myBackupServers.add((ServerRecord) payload.getSender());
        RequestCacheValue res = scaf.setResponseType(IS_BACKUP).build();
        return generateAndSend(res);
    }

    private DatagramPacket handleIsPrimary(RequestCacheValue.Builder scaf, UnwrappedPayload payload){
        if(!payload.hasSender()){
            RequestCacheValue res = scaf.setResponseType(INVALID_OPTIONAL).build();
            return generateAndSend(res);
        }

        backupServersFor.add((ServerRecord) payload.getSender());
        RequestCacheValue res = scaf.setResponseType(IS_PRIMARY).build();
        return generateAndSend(res);
    }


    /**
     * Finds keys for which current server is successor and transfers them to the new server
     * Should be executed after the map state is updated.
     */

    private void transferKeys() {
        threadPool.submit(new KeyTransferHandler(mapLock, map, bytesUsed, serverRing, pendingRecordDeaths));
    }

    // Custom Exceptions

    static class InvalidChecksumException extends Exception {}
}
