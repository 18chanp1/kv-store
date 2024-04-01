package com.g7.CPEN431.A7;

import com.g7.CPEN431.A7.client.KVClient;
import com.g7.CPEN431.A7.client.ServerResponse;
import com.g7.CPEN431.A7.consistentMap.ConsistentMap;
import com.g7.CPEN431.A7.consistentMap.ForwardList;
import com.g7.CPEN431.A7.consistentMap.ServerRecord;
import com.g7.CPEN431.A7.map.KeyWrapper;
import com.g7.CPEN431.A7.map.ValueWrapper;
import com.g7.CPEN431.A7.newProto.KVRequest.PutPair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;

import static com.g7.CPEN431.A7.KVServer.BULKPUT_MAX_SZ;

public class RawPutHandler implements Callable<List<RawPutHandler.STATUS>> {
    ForwardList forwardInstructions;
    KVClient client;

    public RawPutHandler(ForwardList forwardInstructions,
                         KVClient client) {
        this.forwardInstructions = forwardInstructions;
        this.client = client;
    }

    @Override
    public List<STATUS> call() throws Exception {
        return forwardPut();
    }


    private List<STATUS> forwardPut() {
        ServerRecord target = forwardInstructions.getDestination();
        client.setDestination(target.getAddress(), target.getPort());
        List<STATUS> results = new ArrayList<>();
        try {
            List<PutPair> temp = new ArrayList<>();
            int currPacketSize = 0;

            for (Iterator<PutPair> it = forwardInstructions.getKeyEntries().iterator(); it.hasNext();) {
                //take an "engineering" approximation, because serialization is expensive
                PutPair pair = it.next();
                boolean isLast = (!it.hasNext());
                int pairLen = pair.getKey().length + pair.getValue().length + Integer.BYTES;

                //clear the outgoing buffer and send the packet
                if (currPacketSize + pairLen >= BULKPUT_MAX_SZ) {
                    client.setDestination(target.getAddress(), target.getPort());
                    if(isLast)
                    {
                        ServerResponse res = client.bulkPutDone(temp);
                        results.add(res.getErrCode() == KVServerTaskHandler.RES_CODE_SUCCESS ? STATUS.OK: STATUS.FAIL);
                    }
                    else
                    {
                        ServerResponse res = client.bulkPut(temp);
                        results.add(res.getErrCode() == KVServerTaskHandler.RES_CODE_SUCCESS ? STATUS.OK: STATUS.FAIL);
                    }
                    temp.clear();
                    currPacketSize = 0;
                }
                //add to the buffer.
                temp.add(pair);
                currPacketSize += pairLen;

            }
            //clear the buffer.
            if (temp.size() > 0) {
               ServerResponse res = client.bulkPutDone(temp);
               results.add(res.getErrCode() == KVServerTaskHandler.RES_CODE_SUCCESS ? STATUS.OK: STATUS.FAIL);
            }

        } catch (KVClient.ServerTimedOutException e) {
            // TODO: Probably a wise idea to redirect the keys someplace else, but that is a problem for future me.
            System.out.println("Bulk transfer timed out. Marking recipient as dead.");
            results.add(STATUS.TIMEOUT);
        } catch (KVClient.MissingValuesException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return results;
    }

    public static enum STATUS
    {
        OK,
        TIMEOUT,
        FAIL,
    }
}
