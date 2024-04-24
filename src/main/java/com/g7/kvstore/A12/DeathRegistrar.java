package com.g7.CPEN431.A12;

import com.g7.CPEN431.A12.client.KVClient;
import com.g7.CPEN431.A12.client.ServerResponse;
import com.g7.CPEN431.A12.consistentMap.ConsistentMap;
import com.g7.CPEN431.A12.consistentMap.ServerRecord;
import com.g7.CPEN431.A12.map.KeyWrapper;
import com.g7.CPEN431.A12.map.ValueWrapper;
import com.g7.CPEN431.A12.newProto.KVRequest.ServerEntry;

import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;

import static com.g7.CPEN431.A12.KVServer.*;

public class DeathRegistrar extends TimerTask {
    Map<ServerRecord, ServerRecord> broadcastQueue;
    ConcurrentLinkedQueue<ServerRecord> pendingRecords;
    ConsistentMap ring;
    KVClient sender;
    Random random;
    AtomicLong lastReqTime;
    ConcurrentMap<KeyWrapper, ValueWrapper> map;
    ReadWriteLock maplock;
    Semaphore updateRequested;
    AtomicInteger bytesUsed;
    Timer timer;
    ExecutorService threadpool;
    static BlockingQueue<KVClient> clientPool;


    long previousPingSendTime;
    final static int SUSPENDED_THRESHOLD = 10000;
    final static int clients = 10;

    static
    {
        clientPool = new LinkedBlockingQueue<>();
        for(int i = 0; i < clients; i++)
        {
            clientPool.add(new KVClient(new byte[16384], 60, 4));
        }
    }

    public DeathRegistrar(ConcurrentLinkedQueue<ServerRecord> pendingRecords, ConsistentMap ring, AtomicLong lastReqTime,
                          ConcurrentMap<KeyWrapper, ValueWrapper> map, ReadWriteLock maplock, Semaphore updateRequested,
                          AtomicInteger bytesUsed, Timer timer, ExecutorService threadpool)
    throws IOException {
        this.broadcastQueue = new HashMap<>();
        this.pendingRecords = pendingRecords;
        this.ring = ring;
        this.sender = new KVClient(new byte[16384], 500, 4);
        this.random = new Random();
        this.previousPingSendTime = -1;
        this.lastReqTime = lastReqTime;
        this.map = map;
        this.maplock = maplock;
        this.updateRequested = updateRequested;
        this.bytesUsed = bytesUsed;
        this.timer = timer;
        this.threadpool = threadpool;
    }


    @Override
    public synchronized void run() {
        updateBroadcastQueue();
        checkSelfSuspended();
        //check self suspended clears the queue, since anything in there is probably outdated.
//        checkIsAlive();
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
            target = ring.getNextServer();
        } catch (ConsistentMap.NoServersException e) {
            return;
        }


        if(!target.hasServerAddress() || !target.hasServerPort())
        {
            throw new IllegalStateException();
        }

        sender.setDestination(target.getAddress(), target.getServerPort() + 1000);
        //update myself every time I gossip
        long start = System.currentTimeMillis();
        self.setAliveAtTime(start);
        ring.updateServerState(self);


        List<ServerEntry> l = ring.getFullRecord();
        ServerResponse r;
        try {
            r = sender.isDead(l);
        } catch (KVClient.ServerTimedOutException e)
        {
            System.out.println("gossip sending failed");
            target.setLastSeenDeadAt(start);
            ring.updateServerState(target);
            //call for an update
            System.out.println("considering an update" + updateRequested.availablePermits());
            boolean requiresUpdate = updateRequested.tryAcquire();
            if(requiresUpdate)
            {
                System.out.println("requesting update from gossip");
                timer.schedule(new KeyTransferHandler(maplock, map, bytesUsed, ring, pendingRecords, updateRequested), SETTLE_TIME);
            }


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
            target = ring.getRandomServer();

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
            //call for an update
            boolean requiresUpdate = updateRequested.tryAcquire();
            if(requiresUpdate)
            {
                timer.schedule(new KeyTransferHandler(maplock, map, bytesUsed, ring, pendingRecords, updateRequested), SETTLE_TIME);
            }
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
            System.out.println("Suspension detected " + self.getPort());
            lastReqTime.set(System.currentTimeMillis() + 10_000);
            // TODO: check for self loopback
            maplock.writeLock().lock();
            map.clear();
            maplock.writeLock().unlock();
            ring.resetMap();
            broadcastQueue.clear();
            self.setAliveAtTime(System.currentTimeMillis() + 10_000);
            ring.updateServerState(self);
            broadcastQueue.put(self, self);
            ArrayList<ServerEntry> bc = new ArrayList<>();
            bc.add(self);


            //send one by one
            Set<Future> s = new HashSet<>();
            for(ServerEntry server : ring.getFullRecord())
            {
                KVClient cl;
                try {
                    cl = clientPool.take();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

                s.add(threadpool.submit(() ->
                {
                    cl.setDestination(((ServerRecord) server).getAddress(), server.getServerPort());
                    try {
                        cl.isDead(bc);
                        clientPool.add(cl);
                    } catch (KVClient.MissingValuesException e) {
                        clientPool.add(cl);
                        throw new RuntimeException(e);
                    } catch (IOException e) {
                        clientPool.add(cl);
                        throw new RuntimeException(e);
                    } catch (KVClient.ServerTimedOutException e) {
                        clientPool.add(cl);
                    } catch (InterruptedException e) {
                        clientPool.add(cl);
                        throw new RuntimeException(e);
                    }
                }));
            }

            s.forEach((future) ->
            {
                try {
                    future.get();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } catch (ExecutionException e) {
                    throw new RuntimeException(e);
                }
            });

        }

        previousPingSendTime = currentTime;

    }

}
