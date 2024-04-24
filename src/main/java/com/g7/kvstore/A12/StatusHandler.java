package com.g7.CPEN431.A12;

import com.g7.CPEN431.A12.cache.RequestCacheKey;
import com.g7.CPEN431.A12.consistentMap.ConsistentMap;
import com.g7.CPEN431.A12.consistentMap.ServerRecord;
import com.g7.CPEN431.A12.map.KeyWrapper;
import com.g7.CPEN431.A12.map.ValueWrapper;
import com.google.common.cache.Cache;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.Timer;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;


public class StatusHandler implements Runnable {
    DatagramSocket socket;
    Cache<RequestCacheKey, DatagramPacket> requestCache;
    ConcurrentMap<KeyWrapper, ValueWrapper> map;
    ReadWriteLock mapLock;
    AtomicInteger bytesUsed;
    BlockingQueue<byte[]> bytePool;
    boolean isOverloaded;
    ConsistentMap serverRing;
    ConcurrentLinkedQueue<ServerRecord> pendingRecordDeaths;
    ExecutorService threadPool;
    AtomicLong lastReqTime;
    Semaphore keyUpdateRequested;
    Timer timer;
    DatagramSocket outboundSocket;


    public StatusHandler(DatagramSocket socket, Cache<RequestCacheKey, DatagramPacket> requestCache, ConcurrentMap<KeyWrapper, ValueWrapper> map, ReadWriteLock mapLock, AtomicInteger bytesUsed, BlockingQueue<byte[]> bytePool, boolean isOverloaded, ConsistentMap serverRing, ConcurrentLinkedQueue<ServerRecord> pendingRecordDeaths, ExecutorService threadPool, AtomicLong lastReqTime, Semaphore keyUpdateRequested, Timer time, DatagramSocket outboundSocket) {
        this.socket = socket;
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
        this.timer = time;
        this.outboundSocket = outboundSocket;
    }

    @Override
    public void run() {
        BlockingQueue<DatagramPacket> vipOutbound = new LinkedBlockingQueue<>();
        Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
        while(true)
        {
            DatagramPacket rp;
            try {
                rp = new DatagramPacket(bytePool.take(), 16384);
                socket.receive(rp);
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }


            new KVServerTaskHandler(
                    rp,
                    requestCache,
                    map,
                    mapLock,
                    bytesUsed,
                    bytePool,
                    isOverloaded,
                    serverRing,
                    pendingRecordDeaths,
                    threadPool,
                    lastReqTime,
                    keyUpdateRequested,
                    timer,
                    outboundSocket
                    ).run();
        }
    }
}
