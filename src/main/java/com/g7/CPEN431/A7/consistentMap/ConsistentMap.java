package com.g7.CPEN431.A7.consistentMap;

import com.g7.CPEN431.A7.map.KeyWrapper;
import com.g7.CPEN431.A7.map.ValueWrapper;
import com.g7.CPEN431.A7.newProto.KVRequest.ServerEntry;

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
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.g7.CPEN431.A7.KVServer.self;
import static com.g7.CPEN431.A7.KVServer.selfLoopback;

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
    private final int nReplicas;
    private Set<ServerRecord> successors;
    boolean isLoopback = false;


    /**
     *
     * @param vNodes number of vnodes in the consistent hashing scheme
     * @param serverPathName path to txt file containing server IP addresses + port
     * @throws IOException if cannot read txt file.
     */
    public ConsistentMap(int vNodes, int nReplicas, String serverPathName) {
        this.ring = new TreeMap<>();
        this.VNodes = vNodes;
        this.lock = new ReentrantReadWriteLock();
        this.serverPathName = serverPathName;
        this.allRecords = new HashMap<>();
        this.nReplicas = nReplicas;


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
                    isLoopback = serverRecord.equals(selfLoopback);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        //get myself
        successors = getCurrentSuccessors();
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

    /**
     * Lock MUST be obtained prior to calling this function. Additionally,
     * the ring must be checked beforehand to ensure there are sufficient amount
     * of vnodes to return the actual value.
     * @param hashcode - hashcode
     * @return Actual serverRecord corresponding to the code
     */
    private ServerRecord getServerWithHashcode(int hashcode)
    {
        lock.readLock().lock();
        Map.Entry<Integer, VNode> server = ring.ceilingEntry(hashcode);
        /* Deal with case where the successor of the key is past "0" */
        server = (server == null) ? ring.firstEntry(): server;
        lock.readLock().unlock();

        return server.getValue().serverRecord;
    }

    public List<ServerRecord> getNReplicas(byte[] key)
    {
        lock.readLock().lock();
        if(ring.size() < nReplicas * VNodes)
        {
            lock.readLock().unlock();
            System.err.println("Insufficient servers in ring for replica");
            throw new NoServersException();
        }

        List<ServerRecord> result = new ArrayList<>();
        int hashcode = getHash(key);
        Map.Entry<Integer, VNode> serverEntry = ring.ceilingEntry(hashcode);
        /* Deal with case where the successor of the key is past "0" */
        serverEntry = (serverEntry == null) ? ring.firstEntry(): serverEntry;

        ServerRecord firstServer = serverEntry.getValue().serverRecord;

        int serverCode = hashcode;
        result.add(new ServerRecord(firstServer));


        //add replicas
        for(int i = 1; i < nReplicas; i++)
        {
            ServerRecord curr;
            do
            {
                serverCode++;
                curr = getServerWithHashcode(serverCode);
                serverCode = ring.ceilingEntry(serverCode) == null ? ring.firstEntry().getKey() : ring.ceilingEntry(serverCode).getKey();
            } while(result.contains(curr));

            //curr is now the subsequent replica
            result.add(new ServerRecord(curr));
        }

        lock.readLock().unlock();
        return result;
    }

    private List<ServerRecord> getNReplicaWithHashCode(int hashcode)
    {
        lock.readLock().lock();
        if(ring.size() < nReplicas * VNodes)
        {
            lock.readLock().unlock();
            System.err.println("Insufficient servers in ring for replica");
            throw new NoServersException();
        }

        List<ServerRecord> result = new ArrayList<>();
        Map.Entry<Integer, VNode> serverEntry = ring.ceilingEntry(hashcode);
        /* Deal with case where the successor of the key is past "0" */
        serverEntry = (serverEntry == null) ? ring.firstEntry(): serverEntry;

        ServerRecord firstServer = serverEntry.getValue().serverRecord;

        //add main's clone
        int serverCode = hashcode;
        result.add(new ServerRecord(firstServer));


        //add replicas
        for(int i = 1; i < nReplicas; i++)
        {
            ServerRecord curr;
            do
            {
                serverCode++;
                curr = getServerWithHashcode(serverCode);
                serverCode = ring.ceilingEntry(serverCode) == null ? ring.firstEntry().getKey() : ring.ceilingEntry(serverCode).getKey();
            } while(result.contains(curr));

            //curr is now the subsequent replica
            result.add(new ServerRecord(curr));
        }

        lock.readLock().unlock();
        return result;
    }

    private Set<ServerRecord> getCurrentSuccessors()
    {
        Set<ServerRecord> successors = new HashSet<>();
        for(int i = 0; i < VNodes; i++)
        {
            List<ServerRecord> successorsOfVnode = getNReplicaWithHashCode(new VNode(isLoopback ? selfLoopback: self, i).getHash());
            successorsOfVnode.remove(0);
            successors.addAll(successorsOfVnode);
        }
        return successors;
    }

    public REPLICA_TYPE isReplica(byte[] key)
    {
        List<ServerRecord> replicas = getNReplicas(key);

        for(int i = 0; i < nReplicas; i++)
        {
            if(replicas.get(i).equals(self) || replicas.get(i).equals(selfLoopback))
            {
                return i == 0 ? REPLICA_TYPE.PRIMARY : REPLICA_TYPE.BACKUP;
            }
        }
        return REPLICA_TYPE.UNRELATED;
    }

    public REPLICA_TYPE isReplica(List<ServerRecord> list)
    {
        assert list.size() == nReplicas;

        for(int i = 0; i < nReplicas; i++)
        {
            if(list.get(i).equals(self) || list.get(i).equals(selfLoopback))
            {
                return i == 0 ? REPLICA_TYPE.PRIMARY : REPLICA_TYPE.BACKUP;
            }
        }
        return REPLICA_TYPE.UNRELATED;

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
        //if there is only 1 server (aka myself), remaining, crash.
        if(ring.size() <= VNodes)
        {
            lock.readLock().unlock();
            throw new NoServersException();
        }

        Map.Entry<Integer, VNode> server;
        do
        {
            server = ring.ceilingEntry(current);
            /* Deal with case where the successor of the key is past "0" */
            server = (server == null) ? ring.firstEntry(): server;

            /* Set the ptr so that next ceiling entry will be the following node in the ring */
            current = server.getKey() + 1;
        } while (server.getValue().serverRecord.equals(self) || server.getValue().serverRecord.equals(selfLoopback));

        lock.readLock().unlock();

        return server.getValue().getServerRecordClone();
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
                System.out.println(r.getPort());
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
                System.out.println(r.getPort());
            }
            //otherwise, it must be in the same state, thus ring does not need to be changed.

            //sync the code and information time
            actualRecord.setCode(r.getCode());
            actualRecord.setInformationTime(r.getInformationTime());

            if(updated) System.out.println(getServerCount());
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
        Set<ServerRecord> diff = getCurrentSuccessors();

        entries.forEach((entry) ->
        {
            /* normally it is not ok to call our own functions because of the risk of deadlock, but the getServer
            function only uses the readlock, so it is fine.
             */


            /* forward keys for replica repair (I am main, and successors changed */
            byte[] pairKey = entry.getKey().getKey();
            REPLICA_TYPE type = isReplica(pairKey);

            if (type == REPLICA_TYPE.UNRELATED)
            {
                ServerRecord prim = new ServerRecord(getServer(pairKey));
                m.compute(getServer(pairKey), (k, v) ->
                {
                    ForwardList forwardList;
                    if(v == null) forwardList = new ForwardList(prim);
                    else forwardList = v;

                    forwardList.addToList(entry, true);

                    return forwardList;
                });
            }
            else if(type == REPLICA_TYPE.PRIMARY)
            {
                for(ServerRecord diffServer : diff)
                {
                    ServerRecord clone = new ServerRecord(diffServer);
                    m.compute(diffServer, (k, v) ->
                    {
                        ForwardList forwardList;
                        if(v == null) forwardList = new ForwardList(clone);
                        else forwardList = v;

                        forwardList.addToList(entry, false);

                        return forwardList;
                    });
                }
            }

        });

        successors = getCurrentSuccessors();
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

    public static enum REPLICA_TYPE
    {
        PRIMARY,
        BACKUP,
        UNRELATED
    }

}

