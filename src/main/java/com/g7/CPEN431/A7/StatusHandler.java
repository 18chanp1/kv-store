package com.g7.CPEN431.A7;

import com.g7.CPEN431.A7.cache.RequestCacheKey;
import com.g7.CPEN431.A7.client.KVClient;
import com.g7.CPEN431.A7.consistentMap.ConsistentMap;
import com.g7.CPEN431.A7.consistentMap.ServerRecord;
import com.google.common.cache.Cache;
import com.sun.net.httpserver.Request;

import javax.xml.crypto.Data;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;

public class StatusHandler implements Runnable {
    BlockingQueue<byte[]> bytePool;
    DatagramSocket server;
    ExecutorService executor;
    Cache<RequestCacheKey, DatagramPacket> requestCache;
    ConcurrentMap map;
    ReadWriteLock mapLock;
    AtomicInteger bytesUsed;
    BlockingQueue<DatagramPacket> outbound;
    ConsistentMap serverRing;
    BlockingQueue<ServerRecord> pendingRecordDeaths;
    AtomicLong lastReqTime;
    BlockingQueue<KVClient> clientPool;
    AtomicBoolean waitingForIncomingTransfer;

    public StatusHandler(BlockingQueue<byte[]> bytePool,
                         DatagramSocket server, ExecutorService executor, Cache requestCache, ConcurrentMap map, ReadWriteLock mapLock, AtomicInteger bytesUsed, BlockingQueue outbound, ConsistentMap serverRing, BlockingQueue pendingRecordDeaths, AtomicLong lastReqTime, BlockingQueue<KVClient> clientPool, AtomicBoolean waitingForIncomingTransfer) {
        this.bytePool = new LinkedBlockingQueue<>();
        this.server = server;
        this.executor = executor;
        this.requestCache = requestCache;
        this.map = map;
        this.mapLock = mapLock;
        this.bytesUsed = bytesUsed;
        this.outbound = outbound;
        this.serverRing = serverRing;
        this.pendingRecordDeaths = pendingRecordDeaths;
        this.lastReqTime = lastReqTime;
        this.clientPool = clientPool;
        this.waitingForIncomingTransfer = waitingForIncomingTransfer;
    }

    @Override
    public void run()  {
        bytePool.add(new byte[KVServer.PACKET_MAX]);
        while (true)
        {
            byte[] iBuf = bytePool.remove();
            Runtime r = Runtime.getRuntime();
            long remainingMemory  = r.maxMemory() - (r.totalMemory() - r.freeMemory());
            boolean isOverloaded = remainingMemory < KVServer.MEMORY_SAFETY;

            DatagramPacket iPacket = new DatagramPacket(iBuf, iBuf.length);
            try {
                server.receive(iPacket);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            BlockingQueue<DatagramPacket> actualOutbound = new LinkedBlockingQueue<>() {
            };

            /* Run it directly instead of via executor service. */
            new KVServerTaskHandler(
                    iPacket,
                    requestCache,
                    map,
                    mapLock,
                    bytesUsed,
                    bytePool,
                    isOverloaded,
                    actualOutbound,
                    serverRing,
                    pendingRecordDeaths,
                    executor,
                    lastReqTime,
                    clientPool,
                    waitingForIncomingTransfer).run();

            DatagramPacket out;
            out = actualOutbound.poll();

            if(out == null) continue;

            try {
                server.send(out);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        }
    }
}
