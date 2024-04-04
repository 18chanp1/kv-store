//package com.g7.CPEN431.A7.tests;
//
//import com.g7.CPEN431.A7.KVServerTaskHandler;
//import com.g7.CPEN431.A7.client.KVClient;
//import com.g7.CPEN431.A7.consistentMap.ConsistentMap;
//import com.g7.CPEN431.A7.consistentMap.ServerRecord;
//import com.g7.CPEN431.A7.map.KeyWrapper;
//import com.g7.CPEN431.A7.map.ValueWrapper;
//import com.g7.CPEN431.A7.newProto.KVRequest.PutPair;
//import com.g7.CPEN431.A7.wrappers.UnwrappedPayload;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.DisplayName;
//import org.junit.jupiter.api.Test;
//
//import java.io.IOException;
//import java.net.UnknownHostException;
//import java.util.ArrayList;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.ConcurrentMap;
//import java.util.concurrent.locks.ReadWriteLock;
//import java.util.concurrent.locks.ReentrantReadWriteLock;
//
//import static com.g7.CPEN431.A7.client.KVClient.*;
//import static org.mockito.ArgumentMatchers.eq;
//import static org.mockito.Mockito.*;
//
//public class UpdateBackupTest {
//    static ConsistentMap ring;
//    private static ConcurrentMap<KeyWrapper, ValueWrapper> map;
//    private static ReadWriteLock mapLock;
//    private static KVServerTaskHandler taskHandler;
//    static final int num_vnode = 7;
//    static ServerRecord self;
//    private ArrayList<ServerRecord> backupServers;
//    KVClient mockClient;
//    private byte[] key;
//    private byte[] value;
//    private int version;
//    private ArrayList<PutPair> putPairs;
//    private UnwrappedPayload putPayload;
//    private UnwrappedPayload bulkPutPayload;
//    private UnwrappedPayload wipeoutPayload;
//    private UnwrappedPayload deletePayload;
//    private UnwrappedPayload nonWritePayload;
//    @BeforeEach
//    @DisplayName("setup")
//    public void setup() throws UnknownHostException {
//        //initialize the primary server
//        mapLock = new ReentrantReadWriteLock();
//        map = new ConcurrentHashMap<>();
//
//        for (int i = 0; i < 10; i++) {
//            KeyWrapper key = new KeyWrapper(new byte[]{(byte) i});
//            ValueWrapper value = new ValueWrapper(new byte[]{(byte) i}, 0, null);
//            map.put(key, value);
//        }
//
//        ring = new ConsistentMap(num_vnode, "servers_test.txt");
//        self = ring.getRandomServer();
//        taskHandler = new KVServerTaskHandler(map, mapLock, ring);
//
//        // Set up backup servers
//        backupServers = new ArrayList<>();
//
//        for (int i = 0; i < 3; i++) {
//            ServerRecord backup = ring.getRandomServer();
//
//            if (backup != self || !backupServers.contains(backup)) {
//                backupServers.add(backup);
//            }
//        }
//
//        self.setMyBackupServers(backupServers);
//        mockClient = mock();
//
//        key = new byte[]{0x01, 0x02, 0x03, 0x04};
//        value = new byte[]{0x05, 0x06, 0x07, 0x08};
//        version = 123;
//
//        putPayload = new UnwrappedPayload();
//        putPayload.setCommand(REQ_CODE_PUT);
//        putPayload.setKey(key);
//        putPayload.setValue(value);
//        putPayload.setVersion(version);
//
//        bulkPutPayload = new UnwrappedPayload();
//        bulkPutPayload.setCommand(REQ_CODE_BULKPUT);
//        putPairs = new ArrayList<>();
//        bulkPutPayload.setPutPair(putPairs);
//        bulkPutPayload.setPrimaryServer(self);
//
//        wipeoutPayload = new UnwrappedPayload();
//        wipeoutPayload.setCommand(REQ_CODE_WIP);
//
//        deletePayload = new UnwrappedPayload();
//        deletePayload.setCommand(REQ_CODE_DEL);
//        deletePayload.setKey(key);
//
//        nonWritePayload = new UnwrappedPayload();
//        nonWritePayload.setCommand(REQ_CODE_ALI);
//    }
//    @Test
//    public void updateBackupServersPut() throws KVClient.MissingValuesException, IOException, KVClient.ServerTimedOutException, InterruptedException {
//        taskHandler.updateBackupServer(putPayload, null);
//        verify(mockClient, times(1)).put(eq(putPayload.getKey()), eq(putPayload.getValue()), eq(putPayload.getVersion()));
//        verify(mockClient, times(0)).bulkPut(eq(bulkPutPayload.getPutPair()), eq(self));
//        verify(mockClient, times(0)).delete(eq(deletePayload.getKey()));
//        verify(mockClient, times(0)).wipeout();
//    }
//
//    @Test
//    public void updateBackupServersBulkPut() throws KVClient.MissingValuesException, IOException, KVClient.ServerTimedOutException, InterruptedException {
//        taskHandler.updateBackupServer(bulkPutPayload, self);
//        verify(mockClient, times(0)).put(eq(putPayload.getKey()), eq(putPayload.getValue()), eq(putPayload.getVersion()));
//        verify(mockClient, times(1)).bulkPut(eq(bulkPutPayload.getPutPair()), eq(self));
//        verify(mockClient, times(0)).delete(eq(deletePayload.getKey()));
//        verify(mockClient, times(0)).wipeout();
//    }
//
//    @Test
//    public void updateBackupServersDelete() throws KVClient.MissingValuesException, IOException, KVClient.ServerTimedOutException, InterruptedException {
//        taskHandler.updateBackupServer(deletePayload, null);
//        verify(mockClient, times(0)).put(eq(putPayload.getKey()), eq(putPayload.getValue()), eq(putPayload.getVersion()));
//        verify(mockClient, times(0)).bulkPut(eq(bulkPutPayload.getPutPair()), eq(self));
//        verify(mockClient, times(1)).delete(eq(deletePayload.getKey()));
//        verify(mockClient, times(0)).wipeout();
//    }
//
//    @Test
//    public void updateBackupServersWipeout() throws KVClient.MissingValuesException, IOException, KVClient.ServerTimedOutException, InterruptedException {
//        taskHandler.updateBackupServer(wipeoutPayload, null);
//        verify(mockClient, times(0)).put(eq(putPayload.getKey()), eq(putPayload.getValue()), eq(putPayload.getVersion()));
//        verify(mockClient, times(0)).bulkPut(eq(bulkPutPayload.getPutPair()), eq(self));
//        verify(mockClient, times(0)).delete(eq(deletePayload.getKey()));
//        verify(mockClient, times(1)).wipeout();
//    }
//
//    @Test
//    public void updateBackupServersNonWriteOperation() throws KVClient.MissingValuesException, IOException, KVClient.ServerTimedOutException, InterruptedException {
//        taskHandler.updateBackupServer(nonWritePayload, null);
//        verify(mockClient, times(0)).isAlive();
//    }
//}
