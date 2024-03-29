package com.g7.CPEN431.A7.tests;

import com.g7.CPEN431.A7.KVServerTaskHandler;
import com.g7.CPEN431.A7.client.KVClient;
import com.g7.CPEN431.A7.consistentMap.ConsistentMap;
import com.g7.CPEN431.A7.consistentMap.ServerRecord;
import com.g7.CPEN431.A7.map.KeyWrapper;
import com.g7.CPEN431.A7.map.ValueWrapper;
import com.g7.CPEN431.A7.newProto.KVRequest.KVPair;
import com.g7.CPEN431.A7.newProto.KVRequest.PutPair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.g7.CPEN431.A7.consistentMap.ServerRecord.REPLICATION_FACTOR;
import static org.junit.jupiter.api.Assertions.*;

public class ProcessDeadBackupTest {
    static ConsistentMap ring;
    private static ConcurrentMap<KeyWrapper, ValueWrapper> map;
    private static ReadWriteLock mapLock;
    private static KVServerTaskHandler taskHandler;
    static final int num_vnode = 7;
    static ServerRecord self;

    @BeforeEach
    @DisplayName("setup")
    public void setup() throws UnknownHostException {

        //initialize the primary server
        mapLock = new ReentrantReadWriteLock();
        map = new ConcurrentHashMap<>();

        for (int i = 0; i < 10; i++) {
            KeyWrapper key = new KeyWrapper(new byte[]{(byte) i});
            ValueWrapper value = new ValueWrapper(new byte[]{(byte) i}, 0, null);
            map.put(key, value);
        }

        ring = new ConsistentMap(num_vnode, "servers_test.txt");
        self = ring.getRandomServer();
        taskHandler = new KVServerTaskHandler(map, mapLock, ring);

    }

    @Test
    @DisplayName("Test process dead backup servers")
    public void ProcessDeadBackupServerTest() throws KVClient.MissingValuesException, IOException, KVClient.ServerTimedOutException, InterruptedException {
        List<ServerRecord> initialBackupServers = self.getMyBackupServers();
        ServerRecord deadServer = initialBackupServers.get(0);

        assertEquals(initialBackupServers.size(), REPLICATION_FACTOR-1);

        for (int i = 0; i < initialBackupServers.size(); i++) {
            List<ServerRecord> backupServerFor = initialBackupServers.get(i).getBackupServersFor();
            assertFalse(initialBackupServers.isEmpty());
            assertTrue(backupServerFor.contains(self));
        }

        ServerRecord newBackupServer = taskHandler.processDeadBackupServer(deadServer, self);

        // check that new backup server is in our backups list, and that dead server is not
        List<ServerRecord> newBackupServers = self.getMyBackupServers();
        assertEquals(newBackupServers.size(), REPLICATION_FACTOR-1);
        assertTrue(newBackupServers.contains(newBackupServer));
        assertFalse(newBackupServers.contains(deadServer));

        List<ServerRecord> newServerBackupFor = newBackupServer.getBackupServersFor();
        assertFalse(newServerBackupFor.isEmpty());
        assertTrue(newServerBackupFor.contains(self));


        List<PutPair> ourPutPairs = taskHandler.getOurPutPairs();

        List<PutPair> currentPairs = new ArrayList<>();
        for (Map.Entry<KeyWrapper, ValueWrapper> wrapperEntry: map.entrySet()) {
            currentPairs.add(new KVPair(wrapperEntry.getKey().getKey(), wrapperEntry.getValue().getValue(), wrapperEntry.getValue().getVersion()));
        }

        assertEquals(ourPutPairs.size(), currentPairs.size());

        for (PutPair pair: ourPutPairs) {
            assertTrue(currentPairs.contains(pair));
        }
    }
}
