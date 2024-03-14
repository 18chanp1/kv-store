package com.g7.CPEN431.A9.consistentMap;

import com.g7.CPEN431.A9.map.KeyWrapper;
import com.g7.CPEN431.A9.map.ValueWrapper;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.g7.CPEN431.A9.KVServer.self;
import static com.g7.CPEN431.A9.KVServer.selfLoopback;

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
    }

    /**
     *
     * @param address IP address of server to add
     * @param port of the server to add
     */
    public void addServer(InetAddress address, int port)
    {
        addServerPrivate(address, port);
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
     * Has no effects if server does not exist
     * @param address IP address of server to remove
     * @param port of server to remove
     *
     */
    public void removeServer(InetAddress address, int port)
    {
        ServerRecord r = new ServerRecord(address, port);
        removeServer(r);
    }

    /**
     * This is now the standard method to remove servers
     * @param r server record
     */
    public void removeServer(ServerRecord r)
    {
        lock.writeLock().lock();
        for(int i = 0; i < VNodes; i++)
        {
            int hashcode = new VNode(r, i).getHash();
            ring.remove(hashcode);
        }


        lock.writeLock().unlock();
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

    /**
     *
     * @return A random server in the ring.
     * @throws NoServersException - If there are no servers
     */
    public ServerRecord getRandomServer()
    {
        lock.readLock().lock();
        if(ring.isEmpty())
        {
            lock.readLock().unlock();
            throw new NoServersException();
        }

        int hashcode = new Random().nextInt();

        Map.Entry<Integer, VNode> server = ring.ceilingEntry(hashcode);
        /* Deal with case where the successor of the key is past "0" */
        server = (server == null) ? ring.firstEntry(): server;

        lock.readLock().unlock();

        return server.getValue().getServerRecordClone();
    }

    /**
     *
     * @return The next server, goes round robin starting from self.
     * @throws NoServersException If there are no servers in the ring
     */
    public ServerRecord getNextServer() throws NoServersException {
        lock.readLock().lock();
        if(ring.isEmpty())
        {
            lock.readLock().unlock();
            throw new NoServersException();
        }


        Map.Entry<Integer, VNode> server = ring.ceilingEntry(current);
        /* Deal with case where the successor of the key is past "0" */
        server = (server == null) ? ring.firstEntry(): server;

        /* Set the ptr so that next ceiling entry will be the following node in the ring */
        current = server.getKey() + 1;

        lock.readLock().unlock();

        return server.getValue().getServerRecordClone();
    }

    /**
     * Sets the corresponding physical server's record time to the current time,
     * and marks it as alive. If it does not exist, the record will be added.
     * A minimum time of MAX_UPDATE_PERIOD between updates is enforced.
     * @param r Server record (can be clone) who's record is to be amended.
     */
    public boolean setServerAlive(ServerRecord r) {
        lock.writeLock().lock();
        int hashcode = new VNode(r, 0).getHash();
        VNode vnode = ring.get(hashcode);
        boolean updated = true;


        if(vnode == null)
        {
            r.setLastSeenNow();
            addServer(r.getAddress(), r.getPort());
            updated = true;
        }
        else if (Instant.now().toEpochMilli() - vnode.serverRecord.getInformationTime() > MIN_UPDATE_PERIOD)
        {
            r.setLastSeenNow();
            vnode.serverRecord.setLastSeenNow();
            updated = true;
        }
        else
        {
            updated = false;
        }

        lock.writeLock().unlock();

        return updated;
    }

    /**
     * Sets the corresponding server as dead. If the server does not exist, there are no side effects.
     * There is a minimum time of MIN_UPDATE_PERIOD between any updates to the information time.
     * @param r
     */
    public void setServerDeadNow(ServerRecord r)
    {
        lock.writeLock().lock();
        int hashcode = new VNode(r, 0).getHash();
        VNode vnode = ring.get(hashcode);

        r.setLastSeenDeadNow();

        if(vnode != null && Instant.now().toEpochMilli() - vnode.serverRecord.getInformationTime() > MIN_UPDATE_PERIOD) {
            vnode.serverRecord.setLastSeenDeadNow();
        };

        lock.writeLock().unlock();
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

    public boolean hasServer(ServerRecord r)
    {
        int hashcode = new VNode(r, 0).getHash();
        lock.readLock().lock();
        boolean hasKey = ring.containsKey(hashcode);
        lock.readLock().unlock();
        return hasKey;
    }

    /**
     * get server by address and port
     * @param addr
     * @param port
     * @return
     */
    public ServerRecord getServerByAddress(InetAddress addr, int port){
        Integer hashcode = new VNode(new ServerRecord(addr, port), 0).getHash();
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

        return server.getValue().getServerRecordClone();
    }

    /**
     * Updates the server record to the latest one of the current server record
     * in the ring, and incomingRecord. A minimum waiting period of MIN_UPDATE_PERIOD
     * is required between updates.
     * @param incomingRecord - incoming incomingRecord that will be considered for update.
     * @return true if incomingRecord was updated, false if not updated.
     */
    public boolean updateServerRecord(ServerRecord incomingRecord)
    {
        ServerRecord realCopy = new ServerRecord(incomingRecord);

        lock.writeLock().lock();
        if(ring.isEmpty())
        {
            lock.writeLock().unlock();
            throw new NoServersException();
        }

        /* Compare the times of the server incomingRecord */
        int hashcode = new VNode(realCopy, 0).getHash();

        /* Check if it exists */
        VNode existingVnode = ring.get(hashcode);

        /* If it does not exist, or is older, overwrite */
        if(existingVnode == null ||
            incomingRecord.getInformationTime() > existingVnode.serverRecord.getInformationTime() + MIN_UPDATE_PERIOD)
        {
            System.out.println("server time updated");
            for(int i = 0; i < VNodes; i++)
            {
                VNode vnode = new VNode(realCopy, i);
                ring.put(vnode.getHash(), vnode);
            }

            lock.writeLock().unlock();
            return true;
        }
        /* If the incoming incomingRecord is older, do nothing */
        else
        {
            lock.writeLock().unlock();
            return false;
        }
    }

    /**
     * Removes the server represented by incomingRecord if it exists, and
     * the incomingRecord's later than the server record in the ring. A waiting
     * period of MIN_UPDATE_PERIOD is required.
     * @param incomingRecord
     * @return true if the ring was updated, false otherwise.
     */
    public boolean removeServersAtomic(ServerRecord incomingRecord)
    {
        ServerRecord realCopy = new ServerRecord(incomingRecord);

        lock.writeLock().lock();
        if(ring.isEmpty())
        {
            lock.writeLock().unlock();
            throw new NoServersException();
        }

        /* Compare the times of the server incomingRecord */
        int hashcode = new VNode(realCopy, 0).getHash();

        /* Check if it exists */
        VNode existingVnode = ring.get(hashcode);

        /* If it does not exist, or is older, exit*/
        if(existingVnode == null ||
                existingVnode.serverRecord.getInformationTime() > incomingRecord.getInformationTime() + MIN_UPDATE_PERIOD)
        {
            lock.writeLock().unlock();
            return false;
        }
        /* If it exists and the information is newer, remove */
        else
        {
            System.out.println("server record removed");
            removeServer(incomingRecord);
            lock.writeLock().unlock();
            return true;
        }
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

    public boolean wasRecentlyUpdated(ServerRecord r)
    {
        lock.readLock().lock();
        VNode actualVnode = ring.get(new VNode(r, 0).getHash());
        if(actualVnode == null)
        {
            lock.readLock().unlock();
            return true;
        }
        long infoTime = ring.get(new VNode(r, 0).getHash()).serverRecord.getInformationTime();
        lock.readLock().unlock();
        return Instant.now().toEpochMilli() - infoTime <= MIN_UPDATE_PERIOD;
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

            int hashcode = getHash(entry.getKey().getKey());

            Map.Entry<Integer, VNode> server = ring.ceilingEntry(hashcode);
            /* Deal with case where the successor of the key is past "0" */
            server = (server == null) ? ring.firstEntry(): server;

            /* Do not forward keys that belong to myself */
            if(server.getValue().serverRecord.equals(self) || server.getValue().serverRecord.equals(selfLoopback)) return;

            ServerRecord cloneRecord = server.getValue().getServerRecordClone();
            m.compute(server.getValue().serverRecord, (key, value) ->
            {
                ForwardList forwardList;
                if(value == null) forwardList = new ForwardList(cloneRecord);
                else forwardList = value;

                forwardList.addToList(entry);

                return forwardList;
            });
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

    public List<ServerRecord> resetMap()
    {
        lock.writeLock().lock();

        List<ServerRecord> serverlist = new ArrayList<>();

        ring.clear();

        Path path = Paths.get(serverPathName);
        try {
            List<String> serverList = Files.readAllLines(path , StandardCharsets.UTF_8);
            for(String server : serverList)
            {
                String[] serverNPort = server.split(":");
                InetAddress addr = InetAddress.getByName(serverNPort[0]);
                int port = serverNPort.length == 2 ? Integer.parseInt(serverNPort[1]): 13788;

                //reentrancy required
                ServerRecord serverRecord = addServerPrivate(addr, port);
                serverlist.add(new ServerRecord(serverRecord));

                /* only activated during initialization, initializes current ptr */
                if(serverRecord.equals(self) || serverRecord.equals(selfLoopback))
                {
                    this.current = new VNode(self, 0).getHash() + 1;
                    serverlist.removeLast();
                }
            }
        } catch (IOException e) {
            lock.writeLock().unlock();
            throw new RuntimeException(e);
        }

        lock.writeLock().unlock();

        return serverlist;
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

}

