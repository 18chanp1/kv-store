package com.g7.CPEN431.A12.consistentMap;

import com.g7.CPEN431.A12.map.KeyWrapper;
import com.g7.CPEN431.A12.map.ValueWrapper;
import com.g7.CPEN431.A12.newProto.KVRequest.ServerEntry;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.g7.CPEN431.A12.KVServer.*;

/**
 * A map API for a consistent hashing scheme.
 */
public class ConsistentMap {
    private final TreeMap<Integer, VNode> ring;
    private final int VNodes;
    private final ReentrantReadWriteLock lock;
    private int current = 0;
    private String serverPathName;
    private static final int MIN_UPDATE_PERIOD =  5000;
    Map<ServerRecord, ServerRecord> allRecords;
    ConcurrentLinkedQueue<ServerRecord> allRecordQ = new ConcurrentLinkedQueue<>();


    /**
     *
     * @param vNodes number of vnodes in the consistent hashing scheme
     * @param serverPathName path to txt file containing server IP addresses + port
     * @throws IOException if cannot read txt file.
     */
    public ConsistentMap(int vNodes, String serverPathName) {
        this.ring = new TreeMap<>();
        this.VNodes = vNodes;
        this.lock = new ReentrantReadWriteLock();
        this.serverPathName = serverPathName;
        this.allRecords = new HashMap<>();

        // Parse the txt file with all servers.
        Path path = Paths.get(serverPathName);
        try {
            List<String> serverList = Files.readAllLines(path , StandardCharsets.UTF_8);
            for(String server : serverList)
            {
                String[] serverNPort = server.split(":");
                InetAddress addr = InetAddress.getByName(serverNPort[0]);
                int port = serverNPort.length == 2 ? Integer.parseInt(serverNPort[1]): 13788;

                ServerRecord serverRecord = addServerPrivate(addr, port);

                /* only activated during initialization, initializes current ptr */
                if(serverRecord.equals(self) || serverRecord.equals(selfLoopback))
                {
                    this.current = new VNode(self, 0).getHash() + 1;
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        //shuffle the thing around until i'm at the front
        if (ring.size() > 2 * VNodes)
        {
            ServerRecord curr = allRecordQ.remove();
            while(!curr.equals(self) && !curr.equals(selfLoopback))
            {
                allRecordQ.add(curr);
                curr = allRecordQ.remove();
            }

            allRecordQ.add(curr);
        }
    }


    /**
     * Do not use this in public, since invariants are broken, leading to concurrency guarantees failing
     * @param address IP address of server to add
     * @param port of the server to add
     * @return The actual server record in the ring
     */
    private ServerRecord addServerPrivate(InetAddress address, int port)
    {
        ServerRecord newServer = new ServerRecord(address, port);
        this.allRecords.put(newServer, newServer);
        allRecordQ.add(newServer);

        lock.writeLock().lock();
        for(int i = 0; i < VNodes; i++)
        {
            VNode vnode = new VNode(newServer, i);
            ring.put(vnode.getHash(), vnode);
        }
        lock.writeLock().unlock();

        return newServer;
    }

    /**
     *
     * @param key - byte array containing the key of the KV pair that will need to be mapped to a server
     * @return A copy of the server
     */
    public ServerRecord getServer(byte[] key) {
        lock.readLock().lock();
        if(ring.isEmpty())
        {
            lock.readLock().unlock();
            throw new NoServersException();
        }

        int hashcode = getHash(key);

        Map.Entry<Integer, VNode> server = ring.ceilingEntry(hashcode);
        /* Deal with case where the successor of the key is past "0" */
        server = (server == null) ? ring.firstEntry(): server;

        lock.readLock().unlock();

        return server.getValue().getServerRecordClone();
    }

    public VNode getVnode(int hashcode) {
        lock.readLock().lock();
        if(ring.isEmpty())
        {
            lock.readLock().unlock();
            throw new NoServersException();
        }


        Map.Entry<Integer, VNode> server = ring.ceilingEntry(hashcode);
        /* Deal with case where the successor of the key is past "0" */
        server = (server == null) ? ring.firstEntry(): server;

        lock.readLock().unlock();

        return server.getValue();
    }

        /**
         *
         * @return A random server in the ring.
         * @throws NoServersException - If there are no servers
         */
    public ServerRecord getRandomServer()
    {
        lock.readLock().lock();
        if(ring.size() <= 1)
        {
            lock.readLock().unlock();
            throw new NoServersException();
        }

        Map.Entry<Integer, VNode> server;

        do
        {
            int hashcode = new Random().nextInt();

            server = ring.ceilingEntry(hashcode);
            /* Deal with case where the successor of the key is past "0" */
            server = (server == null) ? ring.firstEntry(): server;

            lock.readLock().unlock();
        }
        while(server.equals(self) || server == selfLoopback);


        return server.getValue().getServerRecordClone();

    }

    /**
     *
     * @return The next server, goes round robin starting from self.
     * @throws NoServersException If there are no servers in the ring
     */
    public ServerRecord getNextServer() throws NoServersException {
        //1 or less server
        if(ring.size() <= VNodes)
        {
            throw new NoServersException();
        }

        lock.readLock().lock();
        ServerRecord next;
        for(int i = 0; i < allRecordQ.size(); i++)
        {
            next = allRecordQ.remove();
            allRecordQ.add(next);
            if(!next.equals(self) && !next.equals(selfLoopback) && next.isAlive())
            {
                //return the item
                lock.readLock().unlock();
                return new ServerRecord(next);
            }
        }

        lock.readLock().unlock();

        throw new RuntimeException();
    }

    /**
     * Updates the server state (dead or alive) if the record r is newer than the preexisting record.
     * @param r The new server record
     * @return true if updated, false if not
     */
    public boolean updateServerState(ServerRecord r)
    {
        lock.writeLock().lock();
        ServerRecord actualRecord = allRecords.get(r);
        boolean updated = false;

        if(actualRecord == null)
        {
            lock.writeLock().unlock();
            throw new IllegalStateException("All servers not initially added");
        }

        //check the information time
        if(r.getInformationTime() > actualRecord.getInformationTime())
        {

            //from alive to dead
            if(actualRecord.isAlive() && !r.isAlive())
            {
                //remove from the ring
                for(int i = 0; i < VNodes; i++)
                {
                    VNode v = new VNode(actualRecord, i);
                    ring.remove(v.getHash());
                }
                updated = true;
                System.out.println("from" + self.getPort() + " " + getServerCount());
            }
            // resurrection
            else if (!actualRecord.isAlive() && r.isAlive())
            {
                for(int i = 0; i < VNodes; i++)
                {
                    VNode v = new VNode(actualRecord, i);
                    ring.put(v.getHash(), v);
                }
                updated = true;
                System.out.println("from" + self.getPort() + " " + getServerCount());
            }
            //otherwise, it must be in the same state, thus ring does not need to be changed.

            //sync the code and information time
            actualRecord.setCode(r.getCode());
            actualRecord.setInformationTime(r.getInformationTime());

        }
        lock.writeLock().unlock();
        return updated;
    }

    /**
     * Helper function to hash any byte array to int
     * @param key The byte array
     * @return An integer hash
     * @throws NoSuchAlgorithmException
     */
    private int getHash(byte[] key) {
        MessageDigest md5;
        try {
            md5 = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        byte[] dig = md5.digest(key);

        return (
                (dig[3] & 0xFF) << 24 |
                (dig[2] & 0xFF) << 16 |
                (dig[1] & 0xFF) << 8 |
                (dig[0] & 0xFF)
                );
    }

    /**
     * returns whether the server exist in the ring
     * @param addr: the ip address of the server
     * @param port: the port of the server
     * @return whether the server exist in the ring
     */
    public boolean hasServer(InetAddress addr, int port){
        int hashcode = new VNode(new ServerRecord(addr, port), 0).getHash();
        lock.readLock().lock();
        boolean hasKey = ring.containsKey(hashcode);
        lock.readLock().unlock();
        return hasKey;
    }

    /**
     * Get number of servers in the ring
     * @return number of servers
     */
    public int getServerCount() {
        int count;
        lock.readLock().lock();
        count = ring.size() / VNodes;
        lock.readLock().unlock();
        return count;
    }

    public Collection<ForwardList> getEntriesToBeForwarded(Set<Map.Entry<KeyWrapper, ValueWrapper>> entries)
    {
        lock.readLock().lock();
        if(ring.isEmpty())
        {
            lock.readLock().unlock();
            throw new NoServersException();
        }

        Map<ServerRecord, ForwardList> m = new HashMap<>();

        entries.forEach((entry) ->
        {
            /* normally it is not ok to call our own functions because of the risk of deadlock, but the getServer
            function only uses the readlock, so it is fine.
             */
            byte[] k = entry.getKey().getKey();
            List<ServerRecord> getReplicas = getReplicas(k);

            getReplicas.remove(self);
            getReplicas.remove(selfLoopback);

            for(ServerRecord s : getReplicas)
            {
                m.compute(s, (key, value) ->
                {
                    ForwardList forwardList;
                    if(value == null) forwardList = new ForwardList(s);
                    else forwardList = value;

                    forwardList.addToList(entry);

                    return forwardList;
                });
            }
        });
        lock.readLock().unlock();
        return m.values();
    }

    public Collection<ServerRecord> getAllRecords()
    {
        lock.readLock().lock();
        Set<ServerRecord> allServers = new HashSet<>();
        for(VNode vnode : ring.values())
        {
            allServers.add(vnode.getServerRecordClone());
        }
        lock.readLock().unlock();
        return allServers;
    }

    public List<ServerEntry> getFullRecord()
    {
        lock.readLock().lock();
        List<ServerEntry> allServers = new ArrayList<>();
        for(ServerRecord r: allRecords.values())
        {
            allServers.add(new ServerRecord(r));
        }
        lock.readLock().unlock();
        return allServers;
    }

    public List<ServerRecord> getReplicas(byte[] key)
    {
        lock.readLock().lock();
        assert ring.size() >= N_REPLICAS * VNodes;

        List<ServerRecord> replicas = new ArrayList<>();

        int hash = getHash(key);
        VNode curr = getVnode(hash);
        replicas.add(curr.getServerRecordClone());
        hash = curr.hash + 1;


        for(int i = 1; i < N_REPLICAS; i++)
        {
            do
            {
                curr = getVnode(hash);
                hash = curr.hash + 1;
            } while(replicas.contains(curr.serverRecord));
            replicas.add(curr.getServerRecordClone());
        }

        lock.readLock().unlock();

        return replicas;
    }

    public RTYPE getRtype(byte[] key)
    {
        List<ServerRecord> replicas = getReplicas(key);
        if(replicas.get(0).equals(self) || replicas.get(0).equals(selfLoopback)) return RTYPE.PRI;
        else if (replicas.contains(self) || replicas.contains(selfLoopback)) return RTYPE.BAC;
        else return RTYPE.UNR;
    }

    public void resetMap()
    {
        lock.writeLock().lock();
        for(ServerRecord r : allRecords.values())
        {
            r.setInformationTime(-1);
        }
        lock.writeLock().unlock();
    }

    public static class NoServersException extends IllegalStateException {}
    class ServerDoesNotExistException extends Exception {};


    /**
     * A virtual node representing a physical server
     */
    static class VNode {
        private ServerRecord serverRecord;
        private int vnodeID;
        private int hash;

        /**
         * @param physicalServer - The server that the vnode wraps
         * @param vnodeID - An arbitrary int that differentiates vnodes of the same server apart
         */
        public VNode(ServerRecord physicalServer, int vnodeID)
        {
            this.serverRecord = physicalServer;
            this.vnodeID = vnodeID;

            /* Compute the hash */
            this.hash = genHashFromServer(physicalServer, vnodeID);
        }

        /**
         *
         * @return A clone of the server record wrapped.
         */
        public ServerRecord getServerRecordClone() {
            return new ServerRecord(serverRecord);
        }

        /**
         * Helper function to generate an integer hash from a server's ip + vnode id + port
         * @param record The wrapped physical server
         * @param vnodeID Unique ID representing vnode
         * @return integer hash
         */
        private int genHashFromServer(ServerRecord record, int vnodeID)
        {
            int adrLen = record.getAddress().getAddress().length;
            ByteBuffer hashBuf = ByteBuffer.allocate(adrLen + (Integer.BYTES * 2));
            hashBuf.put(record.getAddress().getAddress());
            hashBuf.putInt(record.getPort());
            hashBuf.putInt(vnodeID);
            hashBuf.flip();
            return genHash(hashBuf.array());
        }

        /**
         *
         * @return the unique hash of this vnode
         */
        public int getHash() {
            return hash;
        }

        /**
         * Helper function to generate integer hash from byte array using MD5
         * @param key byte array
         * @return integer hash
         */
        private int genHash(byte[] key) {
            MessageDigest md5;
            try {
                md5 = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
            md5.reset();

            byte[] dig = md5.digest(key);

            return hash = (
                    (dig[3] & 0xFF) << 24 |
                            (dig[2] & 0xFF) << 16 |
                            (dig[1] & 0xFF) << 8 |
                            (dig[0] & 0xFF)
            );
        }
    }
    public static enum RTYPE
    {
        PRI,
        BAC,
        UNR
    }
}

