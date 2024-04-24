package com.g7.CPEN431.A12;

import com.g7.CPEN431.A12.cache.RequestCacheKey;
import com.g7.CPEN431.A12.consistentMap.ConsistentMap;
import com.g7.CPEN431.A12.consistentMap.ServerRecord;
import com.g7.CPEN431.A12.map.KeyWrapper;
import com.g7.CPEN431.A12.map.ValueWrapper;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import net.openhft.chronicle.map.ChronicleMap;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.time.Instant;
import java.util.Timer;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Hello world!
 *
 */
public class KVServer
{
    /* Default port, overwritten by cmd line */
    static int PORT = 13788;
    static final int PACKET_MAX = 16384;
    final static long CACHE_EXPIRY = 1;
    final static int MEMORY_SAFETY = 2_097_152;
    final static int AVG_KEY_SZ = 32;
    static int MAP_SZ;
    static int N_THREADS;
    final static int AVG_VAL_SZ = 500;
    final static String SERVER_LIST = "servers.txt";
    final static int VNODE_COUNT = 4;
    final static int GOSSIP_INTERVAL = 500;
    final static int GOSSIP_WAIT_INIT = 15_000;
    public final static int BULKPUT_MAX_SZ = 9000;
    public static ServerRecord self;
    public static ServerRecord selfLoopback;
    public static int N_REPLICAS = 4;
    public final static int INTERNODE_TIMEOUT = 60;
    public final static int SETTLE_TIME = 10_000;


    public static void main( String[] args )
    {
        try
        {
            System.out.println("PID: " + ProcessHandle.current().pid());

            MAP_SZ = Integer.parseInt(args[1]);
            int MAP_ENTRIES = MAP_SZ / AVG_VAL_SZ;


             N_THREADS = Integer.parseInt(args[2]);
             N_REPLICAS = Integer.parseInt(args[3]);

            PORT = Integer.parseInt(args[0]);
            self = new ServerRecord(InetAddress.getByAddress(InetAddress.getLocalHost().getAddress()), PORT);
            selfLoopback = new ServerRecord(InetAddress.getLoopbackAddress(), PORT);



            DatagramSocket server = new DatagramSocket(PORT);
            /* Eliminated in single thread */
            ExecutorService executor = Executors.newCachedThreadPool();

            ConcurrentMap<KeyWrapper, ValueWrapper> map
                    = ChronicleMap
                    .of(KeyWrapper.class, ValueWrapper.class)
                    .name("KVStore")
                    .averageKeySize(AVG_KEY_SZ)
                    .entries(MAP_ENTRIES)
                    .averageValueSize(AVG_VAL_SZ)
                    .create();

            /*
            * Explanation of the mapLock.
            *
            * Read lock can be accessed by multiple parties at once. This is used to
            * control access to the map (which is thread safe in itself, and code is also written to be thread safe),
            * so we can support concurrent accesses and mutations. However, a read lock cannot be obtained at the same
            * time a write lock is obtained by some other thread. Used by handleget, handleDelete, handlePut
            *
            * Write lock can only be accessed by one party (so it guarantees exclusive access to the map, no other
            * threads will write). This is used to clear the map and send the response atomically to ensure correctness.
            * Used only by handleWipeout
            *
            * */
            AtomicInteger bytesUsed = new AtomicInteger(0);
            ReadWriteLock mapLock = new ReentrantReadWriteLock();

            @SuppressWarnings("UnstableApiUsage") Cache<RequestCacheKey, DatagramPacket> requestCache = CacheBuilder.newBuilder()
                    .expireAfterWrite(CACHE_EXPIRY, TimeUnit.SECONDS)
                    //.maximumSize(131072)
                    .build();

            /* Setup pool of byte arrays - single thread implementation only has 1 */
            /* A simpler approach to keeping track of byte arrays*/
            BlockingQueue<byte[]> bytePool = new LinkedBlockingQueue<>();
            for(int i = 0; i < N_THREADS; i++) {
                bytePool.add(new byte[PACKET_MAX]);
            }

            Semaphore keyUpdateRequested = new Semaphore(1);



            /* Set up the list of servers */
            ConsistentMap serverRing = new ConsistentMap(VNODE_COUNT, SERVER_LIST);

            /* Set up obituary list */
            ConcurrentLinkedQueue<ServerRecord> pendingRecordDeaths = new ConcurrentLinkedQueue();

            /* Set up last update time */
            AtomicLong lastReqTime = new AtomicLong(Instant.now().toEpochMilli() + GOSSIP_WAIT_INIT + GOSSIP_INTERVAL);

            /* set up the delayedStatusUpdateTimer */

            Timer deathRegistrarTimer = new Timer();
            Timer delayedStatusUpdateTimer = new Timer();
            deathRegistrarTimer.schedule(new DeathRegistrar(pendingRecordDeaths,
                    serverRing,
                    lastReqTime,
                    map,
                    mapLock,
                    keyUpdateRequested,
                    bytesUsed,
                    delayedStatusUpdateTimer,
                    executor), GOSSIP_WAIT_INIT, GOSSIP_INTERVAL);

            //setup the status handler
            BlockingQueue statusHandlerBytepool = new LinkedBlockingQueue();
            for(int i = 0; i < 2; i++)
            {
                statusHandlerBytepool.add(new byte[16384]);
            }
            DatagramSocket statusSocket = new DatagramSocket(self.getPort() + 1000);
            executor.submit(new StatusHandler(
                    statusSocket,
                    requestCache,
                    map,
                    mapLock,
                    bytesUsed,
                    statusHandlerBytepool,
                    false,
                    serverRing,
                    pendingRecordDeaths,
                    executor,
                    lastReqTime,
                    keyUpdateRequested,
                    delayedStatusUpdateTimer,
                    statusSocket
            ));

            //setup the bulkput handler

//            BlockingQueue<byte[]> bulkputPool = new LinkedBlockingQueue();
//            for(int i = 0; i < 16 ; i++)
//            {
//                bulkputPool.add(new byte[16384]);
//            }

//            DatagramSocket bulkputSocket = new DatagramSocket(self.getPort() + 500);
//            executor.submit(new BulkPutServer(
//                    bulkputSocket,
//                    requestCache,
//                    map,
//                    mapLock,
//                    bytesUsed,
//                    bulkputPool,
//                    false,
//                    serverRing,
//                    pendingRecordDeaths,
//                    executor,
//                    lastReqTime,
//                    keyUpdateRequested,
//                    delayedStatusUpdateTimer,
//                    bulkputSocket
//            ));

            //SocketPool
            BlockingQueue<DatagramSocket> socketPool = new LinkedBlockingQueue<>();
            for(int i = 0; i < N_THREADS; i++)
            {
                socketPool.add(new DatagramSocket());
            }


            while(true){
                if(N_REPLICAS > 1)
                {
                    Thread.sleep(0,32000);
                }

                Runtime r = Runtime.getRuntime();
                long remainingMemory  = r.maxMemory() - (r.totalMemory() - r.freeMemory());
                boolean isOverloaded = remainingMemory < MEMORY_SAFETY;

                byte[] iBuf = bytePool.take();

                DatagramPacket iPacket = new DatagramPacket(iBuf, iBuf.length);
                server.receive(iPacket);

                DatagramSocket selectedSocket = socketPool.take();
                socketPool.add(selectedSocket);


                /* Run it directly instead of via executor service. */
                executor.execute(new KVServerTaskHandler(
                        iPacket,
                        requestCache,
                        map,
                        mapLock,
                        bytesUsed,
                        bytePool,
                        isOverloaded,
                        serverRing,
                        pendingRecordDeaths,
                        executor,
                        lastReqTime,
                        keyUpdateRequested,
                        delayedStatusUpdateTimer,
                        selectedSocket
                        ));

                /* Executed here in single thread impl. */
//                while(!outbound.isEmpty())
//                {
//                    server.send(outbound.poll());
//                }
            }

        } catch (SocketException e) {
            //System.err.println("Server socket setup exception");
            throw new RuntimeException(e);
        } catch (IOException e) {
            //System.err.println("Server IO exception.");
            throw new RuntimeException(e);
        } catch (Exception e) {
//            System.err.println("Bytepool exception");
            throw new RuntimeException(e);
        }

    }
}
