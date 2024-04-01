package com.g7.CPEN431.A7;

import com.g7.CPEN431.A7.client.KVClient;
import com.g7.CPEN431.A7.client.ServerResponse;
import com.g7.CPEN431.A7.consistentMap.ConsistentMap;
import com.g7.CPEN431.A7.consistentMap.ForwardList;
import com.g7.CPEN431.A7.consistentMap.ServerRecord;
import com.g7.CPEN431.A7.map.KeyWrapper;
import com.g7.CPEN431.A7.map.ValueWrapper;
import com.g7.CPEN431.A7.newProto.KVRequest.KVPair;
import com.g7.CPEN431.A7.newProto.KVRequest.PutPair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;

import static com.g7.CPEN431.A7.KVServer.BULKPUT_MAX_SZ;

public class KeyTransferHandler implements Runnable {
    ReadWriteLock mapLock;
    ConcurrentMap<KeyWrapper, ValueWrapper> map;
    AtomicInteger bytesUsed;
    ConsistentMap serverRing;
    ConcurrentLinkedQueue pendingRecordDeaths;

    public KeyTransferHandler(ReadWriteLock mapLock,
                              ConcurrentMap<KeyWrapper, ValueWrapper> map,
                              AtomicInteger bytesUsed, ConsistentMap serverRing,
                              ConcurrentLinkedQueue pendingRecordDeaths) {
        this.mapLock = mapLock;
        this.map = map;
        this.bytesUsed = bytesUsed;
        this.serverRing = serverRing;
        this.pendingRecordDeaths = pendingRecordDeaths;
    }

    @Override
    public void run() {
        transferKeys();
    }

    private void floodRecords()
    {
        Collection<ServerRecord> allServers = serverRing.getAllRecords();
        for(ServerRecord server: allServers)
        {
            pendingRecordDeaths.add(server);
        }
    }

    private void transferKeys() {

        mapLock.writeLock().lock();

        Collection<ForwardList> toBeForwarded = serverRing.getEntriesToBeForwarded(this.map.entrySet());


        byte[] byteArr = new byte[16384];
        KVClient sender = new KVClient(byteArr);
        List<KVPair> toDelete = new ArrayList<>();

        toBeForwarded.forEach((forwardList -> {
            ServerRecord target = forwardList.getDestination();
            sender.setDestination(target.getAddress(), target.getPort());
            try {
                System.out.println("transferring out " + forwardList.getKeyEntries().size() + "keys");
                List<PutPair> temp = new ArrayList<>();
                int currPacketSize = 0;
                for (KVPair pair : forwardList.getKeyEntries()) {
                    //take an "engineering" approximation, because serialization is expensive
                    int pairLen = pair.getKey().length + pair.getValue().length + Integer.BYTES;

                    //clear the outgoing buffer and send the packet
                    if(currPacketSize + pairLen >= BULKPUT_MAX_SZ)
                    {
                        System.out.println("sending" + temp.size() + "pairs");
                        sender.setDestination(target.getAddress(), target.getPort());
                        ServerResponse res = sender.bulkPut(temp);
                        temp.clear();
                        currPacketSize = 0;
                    }
                    //add to the buffer.
                    temp.add(pair);
                    if(pair.isDelete()) toDelete.add(pair);
                    currPacketSize += pairLen;
                }
                //clear the buffer.
                System.out.println("sending" + temp.size() + "pairs");
                if(temp.size() > 0) sender.bulkPut(temp);
            } catch (KVClient.ServerTimedOutException e) {
                // TODO: Probably a wise idea to redirect the keys someplace else, but that is a problem for future me.
                System.out.println("Bulk transfer timed out. Marking recipient as dead.");
                mapLock.writeLock().unlock();
                return;
            } catch (KVClient.MissingValuesException e) {
                mapLock.writeLock().unlock();
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                mapLock.writeLock().unlock();
                throw new RuntimeException(e);
            } catch (IOException e) {
                mapLock.writeLock().unlock();
                throw new RuntimeException(e);
            }

            /* remove entries in our own server */
            toDelete.forEach((pair) ->
            {
                map.remove(new KeyWrapper(pair.getKey()));
                bytesUsed.addAndGet(-pair.getValue().length);
            });
        }));

        mapLock.writeLock().unlock();
    }
}
