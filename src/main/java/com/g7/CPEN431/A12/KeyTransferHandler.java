package com.g7.CPEN431.A12;

import com.g7.CPEN431.A12.client.KVClient;
import com.g7.CPEN431.A12.consistentMap.ConsistentMap;
import com.g7.CPEN431.A12.consistentMap.ForwardList;
import com.g7.CPEN431.A12.consistentMap.ServerRecord;
import com.g7.CPEN431.A12.map.KeyWrapper;
import com.g7.CPEN431.A12.map.ValueWrapper;
import com.g7.CPEN431.A12.newProto.KVRequest.KVPair;
import com.g7.CPEN431.A12.newProto.KVRequest.PutPair;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;

import static com.g7.CPEN431.A12.KVServer.BULKPUT_MAX_SZ;
import static com.g7.CPEN431.A12.KVServer.self;

public class KeyTransferHandler  extends TimerTask{
    ReadWriteLock mapLock;
    ConcurrentMap<KeyWrapper, ValueWrapper> map;
    AtomicInteger bytesUsed;
    ConsistentMap serverRing;
    ConcurrentLinkedQueue pendingRecordDeaths;
    Semaphore pendingUpdate;

    public KeyTransferHandler(ReadWriteLock mapLock,
                              ConcurrentMap<KeyWrapper, ValueWrapper> map,
                              AtomicInteger bytesUsed, ConsistentMap serverRing,
                              ConcurrentLinkedQueue pendingRecordDeaths,
                              Semaphore pendingUpdate) {
        this.mapLock = mapLock;
        this.map = map;
        this.bytesUsed = bytesUsed;
        this.serverRing = serverRing;
        this.pendingRecordDeaths = pendingRecordDeaths;
        this.pendingUpdate = pendingUpdate;
    }

    @Override
    public void run() {
        transferKeys();
    }


    private void transferKeys() {
        System.out.println("Starting key transfer. @" + self.getPort());

        mapLock.writeLock().lock();

        Collection<ForwardList> toBeForwarded = serverRing.getEntriesToBeForwarded(this.map.entrySet());
        pendingUpdate.release();

        mapLock.writeLock().unlock();

        DatagramSocket s;
        try {
            s = new DatagramSocket();
        } catch (SocketException e) {
            throw new RuntimeException(e);
        }


        byte[] byteArr = new byte[16384];
        Set<KVPair> toDelete = new HashSet<>();
        AtomicBoolean clearFlag = new AtomicBoolean(false);

        toBeForwarded.forEach((forwardList -> {
            ServerRecord target = forwardList.getDestination();
            try {
                List<PutPair> temp = new ArrayList<>();
                int currPacketSize = 0;
                for (PutPair pair : forwardList.getKeyEntries()) {
                    //take an "engineering" approximation, because serialization is expensive
                    int pairLen = pair.getKey().length + pair.getValue().length + Integer.BYTES;

                    //clear the outgoing buffer and send the packet
                    if(currPacketSize + pairLen >= BULKPUT_MAX_SZ)
                    {
                        byte[] pairs = KVClient.bulkPutPumpStatic(temp);
                        DatagramPacket p = new DatagramPacket(pairs, pairs.length, target.getAddress(), target.getPort());
                        s.send(p);
                        temp.clear();
                        currPacketSize = 0;
                        try {
                            Thread.sleep(4);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    //add to the buffer.
                    temp.add(pair);
                    if(serverRing.getRtype(pair.getKey()) == ConsistentMap.RTYPE.UNR) toDelete.add((KVPair) pair);
                    currPacketSize += pairLen;

                }
                //clear the buffer.
                if(temp.size() > 0)
                {
                    byte[] pairs = KVClient.bulkPutPumpStatic(temp);
                    DatagramPacket p = new DatagramPacket(pairs, pairs.length, target.getAddress(), target.getPort());
                    s.send(p);
                }

            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        }));

        toDelete.forEach((pair) ->
        {
            ValueWrapper v = map.remove(new KeyWrapper(pair.getKey()));
            if(v != null) bytesUsed.addAndGet(-v.getValue().length);
        });

        System.out.println("ending key transfer. @" + self.getPort());

    }
}
