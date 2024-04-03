package com.g7.CPEN431.A7;

import com.g7.CPEN431.A7.client.KVClient;
import com.g7.CPEN431.A7.client.ServerResponse;
import com.g7.CPEN431.A7.consistentMap.ConsistentMap;
import com.g7.CPEN431.A7.consistentMap.ServerRecord;
import com.g7.CPEN431.A7.newProto.KVRequest.ServerEntry;

import java.io.IOException;
import java.net.DatagramSocket;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.g7.CPEN431.A7.KVServer.*;
import static com.g7.CPEN431.A7.consistentMap.ServerRecord.CODE_DED;

public class DeathRegistrar extends TimerTask {
    Map<ServerRecord, ServerRecord> broadcastQueue;
    ConcurrentLinkedQueue<ServerRecord> pendingRecords;
    ConsistentMap ring;
    KVClient sender;
    Random random;
    AtomicLong lastReqTime;

    long previousPingSendTime;
    final static int SUSPENDED_THRESHOLD = 5000;
    final static int K = 10;

    public DeathRegistrar(ConcurrentLinkedQueue<ServerRecord> pendingRecords, ConsistentMap ring, AtomicLong lastReqTime)
    throws IOException {
        this.broadcastQueue = new HashMap<>();
        this.pendingRecords = pendingRecords;
        this.ring = ring;
        this.sender = new KVClient(null, 0, new DatagramSocket(), new byte[16384]);
        this.random = new Random();
        this.previousPingSendTime = -1;
        this.lastReqTime = lastReqTime;
    }

    @Override
    public synchronized void run() {
        updateBroadcastQueue();
        checkSelfSuspended();
        //check self suspended clears the queue, since anything in there is probably outdated.
        checkIsAlive();
        gossip();

        previousPingSendTime = Instant.now().toEpochMilli();
    }

    private void updateBroadcastQueue()
    {
        ServerRecord n;
        while((n = pendingRecords.poll()) != null)
        {
            ServerRecord existing = broadcastQueue.get(n);

            if((existing != null && !existing.hasInformationTime()) || !n.hasInformationTime())
            {
                throw new IllegalStateException("Broadcast message has no information time");
            }

            if(existing != null && existing.getInformationTime() > n.getInformationTime())
            {
                continue;
            }

            broadcastQueue.put(n,n);
        }
    }

    private void gossip()
    {
        ServerRecord target;
        try {
            target = ring.getRandomServer();
        } catch (ConsistentMap.NoServersException e) {
            System.err.println("no servers to gossip with");
            return;
        }


        if(!target.hasServerAddress() || !target.hasServerPort())
        {
            throw new IllegalStateException();
        }

        if(broadcastQueue.isEmpty() || target.equals(self) || target.equals(selfLoopback))
        {
            return;
        }

        sender.setDestination(target.getAddress(), target.getServerPort());
        //update myself every time I gossip
        self.setAliveAtTime(System.currentTimeMillis());
        ring.updateServerState(self);
        List<ServerEntry> l = ring.getFullRecord();
        ServerResponse r;
        try {
            r = sender.isDead(l);
        } catch (KVClient.ServerTimedOutException e)
        {
            System.out.println("gossip sending failed");
//            System.out.println("Server declared dead by gossip response");
//            System.out.println("Port: " + target.getAddress().toString() + ":" +target.getPort());
//            ring.setServerDeadNow(target);
//            ring.removeServer(target);
//            broadcastQueue.put(target, target);
            return;
        } catch (Exception e) {
            System.out.println("Spreading gossip failed");
            e.printStackTrace();
            return;
        }

        if(r.getErrCode() != KVServerTaskHandler.RES_CODE_SUCCESS)
        {
            System.err.println("Gossip recipient returned error: " + r.getErrCode());
            return;
        }

        if(!r.hasServerStatusCode())
        {
            System.err.println("Gossip recipient did not return list of status codes");
            return;
        }

        List<Integer> responses = r.getServerStatusCode();

        if(responses.size() != l.size())
        {
            System.err.println("Gossip recipient's status size is not the same");
            return;
        }


//        for(int i = 0; i < l.size(); i++)
//        {
//            if(responses.get(i) == KVServerTaskHandler.STAT_CODE_OLD && random.nextInt(K) == 0)
//            {
//                //delete the response
//                broadcastQueue.remove( (ServerRecord) l.get(i));
//            }
//        }
    }

    /**
     * This function picks a random node from the server list and sends an isAlive request
     * to it. If there is no response after retries, we update the server list to account for
     * the "dead" server. Otherwise, do nothing.
     */
    private void checkIsAlive() {
        ServerRecord target = null;


        try {
            target = ring.getNextServer();

            /* omit this round if next server is equal to self */
            if(target.equals(selfLoopback) || target.equals(self) ||
                   Instant.now().toEpochMilli() - target.getInformationTime() < 10_000)
            {
                return;
            }

            sender.setDestination(target.getAddress(), target.getServerPort());
            sender.isAlive();
//            ring.setServerAlive(target);
        } catch (ConsistentMap.NoServersException | IOException | KVClient.MissingValuesException |
                 InterruptedException e) {
            throw new RuntimeException(e);
        } catch (KVClient.ServerTimedOutException e) {
            // Update server death time to current time, then add to list of deaths
            target.setLastSeenDeadAt(System.currentTimeMillis());
            ring.updateServerState(target);
        }
    }

    /**
     * This function checks if the current server has been suspended, i.e. the time since it last sent
     * a ping is greater than 800 ms. If so, updates the server to be alive so that it can be broadcasted
     * next time.
     *
     * This function does nothing if the current server has not yet sent a ping before.
     */
    private void checkSelfSuspended() {
        long currentTime = Instant.now().toEpochMilli();
        previousPingSendTime = previousPingSendTime == -1 ? currentTime : previousPingSendTime;
        if (ring.getServerCount() != 1 && currentTime - previousPingSendTime > GOSSIP_INTERVAL + SUSPENDED_THRESHOLD) {
            System.out.println("Suspension detected");
            // TODO: check for self loopback
            broadcastQueue.clear();
            self.setAliveAtTime(System.currentTimeMillis());
            ring.updateServerState(self);
            broadcastQueue.put(self, self);


            //send one by one
            for(ServerEntry server : ring.getFullRecord())
            {
                sender.setDestination(((ServerRecord) server).getAddress(), server.getServerPort());
                try {
                    sender.isDead(ring.getFullRecord());
                } catch (KVClient.MissingValuesException e) {
                    throw new RuntimeException(e);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                } catch (KVClient.ServerTimedOutException e) {
                    System.out.println("Server is down");
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        previousPingSendTime = currentTime;

    }

}
