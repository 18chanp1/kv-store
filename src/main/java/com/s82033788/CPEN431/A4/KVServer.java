package com.s82033788.CPEN431.A4;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.s82033788.CPEN431.A4.cache.RequestCacheKey;
import com.s82033788.CPEN431.A4.cache.RequestCacheValue;
import com.s82033788.CPEN431.A4.map.KeyWrapper;
import com.s82033788.CPEN431.A4.map.ValueWrapper;
import net.openhft.chronicle.map.ChronicleMap;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Hello world!
 *
 */
public class KVServer
{
    final static int PORT = 13788;
    final static int N_THREADS = 8 ; //TODO tune by profiler
    static final int PACKET_MAX = 16384;
    final static long  CACHE_SZ = 65536;//TODO tune by profiler
    final static long CACHE_EXPIRY = 5;

    public static void main( String[] args )
    {

        try
        {
            //TODO get rid of magic numbers
            DatagramSocket server = new DatagramSocket(PORT);
            ExecutorService executor = Executors.newCachedThreadPool(); //TODO tune profiler

            ConcurrentMap<KeyWrapper, ValueWrapper> map
                    = ChronicleMap
                    .of(KeyWrapper.class, ValueWrapper.class)
                    .name("KVStore")
                    .averageKeySize(32)
                    .entries(146_800)
                    .averageValueSize(500)
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
            Lock bytesUsedLock = new ReentrantLock();
            ReadWriteLock mapLock = new ReentrantReadWriteLock();
            byte[] iBuf;
            @SuppressWarnings("UnstableApiUsage") Cache<RequestCacheKey, RequestCacheValue> requestCache = CacheBuilder.newBuilder()
                    .expireAfterWrite(CACHE_EXPIRY, TimeUnit.SECONDS)
                    //.maximumSize(131072)
                    .build();



            while(true){
                iBuf = new byte[PACKET_MAX];
                DatagramPacket iPacket = new DatagramPacket(iBuf, iBuf.length);
                server.receive(iPacket);

                ThreadPoolExecutor tpe = (ThreadPoolExecutor) executor;

                executor.execute(new KVServerTaskHandler(
                        iPacket,
                        server,
                        requestCache,
                        map,
                        mapLock,
                        tpe,
                        bytesUsed,
                        bytesUsedLock));

            }

        } catch (SocketException e) {
            //System.err.println("Server socket setup exception");
            throw new RuntimeException(e);
        } catch (IOException e) {
            //System.err.println("Server IO exception.");
            throw new RuntimeException(e);
        }

    }
}
