package com.g7.CPEN431.A9;

import com.g7.CPEN431.A9.client.KVClient;
import com.g7.CPEN431.A9.client.ServerResponse;
import com.g7.CPEN431.A9.consistentMap.ConsistentMap;
import com.g7.CPEN431.A9.consistentMap.ForwardList;
import com.g7.CPEN431.A9.consistentMap.ServerRecord;
import com.g7.CPEN431.A9.map.KeyWrapper;
import com.g7.CPEN431.A9.map.ValueWrapper;
import com.g7.CPEN431.A9.newProto.KVRequest.PutPair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;

import static com.g7.CPEN431.A9.KVServer.BULKPUT_MAX_SZ;

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

        toBeForwarded.forEach((forwardList -> {
            ServerRecord target = forwardList.getDestination();
            sender.setDestination(target.getAddress(), target.getPort());
            try {
                System.out.println("transferring out " + forwardList.getKeyEntries().size() + "keys");
                List<PutPair> temp = new ArrayList<>();
                int currPacketSize = 0;
                for (PutPair pair : forwardList.getKeyEntries()) {
                    //take an "engineering" approximation, because serialization is expensive
                    int pairLen = pair.getKey().length + pair.getValue().length + Integer.BYTES;

                    //clear the outgoing buffer and send the packet
                    if(currPacketSize + pairLen >= BULKPUT_MAX_SZ)
                    {
                        sender.setDestination(target.getAddress(), target.getPort());
                        ServerResponse res = sender.bulkPut(temp);
                        temp.clear();
                        currPacketSize = 0;
                    }
                    //add to the buffer.
                    temp.add(pair);
                    currPacketSize += pairLen;

                }
                //clear the buffer.
                if(temp.size() > 0) sender.bulkPut(temp);
            } catch (KVClient.ServerTimedOutException e) {
                // TODO: Probably a wise idea to redirect the keys someplace else, but that is a problem for future me.
                System.out.println("Bulk transfer timed out. Marking recipient as dead.");
                serverRing.setServerDeadNow(target);
                pendingRecordDeaths.add(target);
                serverRing.removeServer(target);
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
            forwardList.getKeyEntries().forEach((entry) ->
            {
                map.remove(new KeyWrapper(entry.getKey()));
                bytesUsed.addAndGet(-entry.getValue().length);
            });
        }));

        mapLock.writeLock().unlock();
    }
}
