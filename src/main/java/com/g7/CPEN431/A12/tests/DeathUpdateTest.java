//package com.g7.CPEN431.A7.tests;
//
//import com.g7.CPEN431.A7.KVServerTaskHandler;
//import com.g7.CPEN431.A7.consistentMap.ConsistentMap;
//import com.g7.CPEN431.A7.consistentMap.ServerRecord;
//import com.g7.CPEN431.A7.newProto.KVRequest.ServerEntry;
//import com.g7.CPEN431.A7.wrappers.UnwrappedPayload;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.DisplayName;
//import org.junit.jupiter.api.Test;
//
//import java.io.IOException;
//import java.net.InetAddress;
//import java.net.UnknownHostException;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.concurrent.ConcurrentLinkedQueue;
//
//import static com.g7.CPEN431.A7.KVServerTaskHandler.*;
//import static com.g7.CPEN431.A7.consistentMap.ServerRecord.CODE_ALI;
//import static com.g7.CPEN431.A7.consistentMap.ServerRecord.CODE_DED;
//import static org.junit.jupiter.api.Assertions.*;
//
//public class DeathUpdateTest {
//    static UnwrappedPayload payload = new UnwrappedPayload();
//    static ConsistentMap ring;
//    static final int num_vnode = 7;
//    static List<InetAddress> server_addrs = new ArrayList<>();
//
//    private static KVServerTaskHandler taskHandler;
//
//    static ConcurrentLinkedQueue<ServerRecord> pendingRecordDeaths;
//
//    static ServerRecord self;
//
//    List<ServerEntry> servers;
//
//    @BeforeEach
//    @DisplayName("setup")
//    public void setup() throws UnknownHostException {
//        ring = new ConsistentMap(num_vnode, "servers.txt");
//        servers = new ArrayList<>();
//        pendingRecordDeaths = new ConcurrentLinkedQueue<>();
//        taskHandler = new KVServerTaskHandler(ring, pendingRecordDeaths);
//        self = new ServerRecord(InetAddress.getByAddress(new byte[]{127, 127, 50, 50}), 200);
//        self.setInformationTime(System.currentTimeMillis());
//        ring.addServer(self.getAddress(), 200);
//
//        for (int i = 0; i < num_vnode; i++) {
//            InetAddress addr = InetAddress.getByAddress(new byte[]{127, 0, 0, (byte) i});
//            server_addrs.add(addr);
//            if (i != 1) ring.addServer(addr, 200);
//        }
//    }
//
//    @Test
//    @DisplayName("Test stat code NEW for dead server")
//    public void deathUpdateNews() throws UnknownHostException {
//        List<ServerEntry> servers = new ArrayList<>();
//        ServerEntry s1 = new ServerRecord(server_addrs.get(0), 200);
//        s1.setInformationTime(System.currentTimeMillis() + 1000);
//        s1.setCode(CODE_DED);
//        servers.add(s1);
//        payload.setServerRecord(servers);
//
//        List<Integer> codes = taskHandler.getDeathCodes(servers, self);
//
//        assertEquals(codes.get(0), STAT_CODE_NEW);
//        assertFalse(ring.hasServer(server_addrs.get(0), 200));
//        assert pendingRecordDeaths.peek() != null;
//        assertEquals(pendingRecordDeaths.peek(), s1);
//    }
//
//    @Test
//    @DisplayName("Test stat code OLD NEWS when dead server not on ring")
//    public void deathUpdateOldNotOnRing() throws UnknownHostException {
//
//        ServerEntry s1 = new ServerRecord(server_addrs.get(1), 200);
//        s1.setCode(CODE_DED);
//        servers.add(s1);
//        payload.setServerRecord(servers);
//
//        List<Integer> codes = taskHandler.getDeathCodes(servers, self);
//        assertEquals(codes.get(0), STAT_CODE_OLD);
//        assertFalse(ring.hasServer(server_addrs.get(1), 200));
//        assertEquals(pendingRecordDeaths.size(), 0);
//    }
//
//    @Test
//    @DisplayName("Test stat code OLD NEWS when dead server rejoins")
//    public void deathUpdateOldRejoins() throws UnknownHostException {
//        ServerEntry s1 = new ServerRecord(server_addrs.get(2), 200);
//        s1.setCode(CODE_DED);
//        s1.setInformationTime(System.currentTimeMillis() - 1000);
//        servers.add(s1);
//        payload.setServerRecord(servers);
//
//        List<Integer> codes = taskHandler.getDeathCodes(servers, self);
//        assertEquals(codes.get(0), STAT_CODE_OLD);
//        assertTrue(ring.hasServer(server_addrs.get(2), 200));
//        assertEquals(pendingRecordDeaths.size(), 0);
//    }
//
//    @Test
//    @DisplayName("Test dead server rejoin with CODE_ALI")
//    public void deadServerRejoinTest() throws UnknownHostException {
//        ServerEntry s1 = new ServerRecord(server_addrs.get(1), 200);
//        s1.setCode(ServerRecord.CODE_ALI);
//        servers.add(s1);
//        payload.setServerRecord(servers);
//
//        assertFalse(ring.hasServer(server_addrs.get(1), 200));
//        List<Integer> codes = taskHandler.getDeathCodes(servers, self);
//        assertEquals(codes.get(0), STAT_CODE_NEW);
//        assertTrue(ring.hasServer(server_addrs.get(1), 200));
//        assert pendingRecordDeaths.peek() != null;
//        assertEquals(pendingRecordDeaths.peek(), s1);
//    }
//
//    @Test
//    @DisplayName("Test dead server rejoin with CODE_ALI but is on ring already")
//    public void aliveServerRejoinTest() throws UnknownHostException {
//        ServerEntry s1 = new ServerRecord(server_addrs.get(0), 200);
//        s1.setCode(ServerRecord.CODE_ALI);
//        servers.add(s1);
//        payload.setServerRecord(servers);
//
//        assertTrue(ring.hasServer(server_addrs.get(0), 200));
//        List<Integer> codes = taskHandler.getDeathCodes(servers, self);
//        assertEquals(codes.get(0), STAT_CODE_OLD);
//        assertTrue(ring.hasServer(server_addrs.get(0), 200));
//        assertEquals(pendingRecordDeaths.size(), 0);
//    }
//
//    @Test
//    @DisplayName("Test self is on death list")
//    public void selfServerNotifiedDeathTest() throws UnknownHostException {
//        ServerEntry s1 = self;
//        s1.setCode(CODE_DED);
//        servers.add(s1);
//        payload.setServerRecord(servers);
//
//        assertTrue(ring.hasServer(self.getAddress(), 200));
//        List<Integer> codes = taskHandler.getDeathCodes(servers, self);
//        assertEquals(codes.get(0), STAT_CODE_NEW);
//        assertTrue(ring.hasServer(self.getAddress(), 200));
//        assert pendingRecordDeaths.peek() != null;
//        assertEquals(pendingRecordDeaths.peek(), s1);
//    }
//
//    @Test
//    @DisplayName("Test self is on death list with code_ali")
//    public void selfServerNotifiedDeathTestAlive() throws UnknownHostException {
//        ServerEntry s1 = self;
//        servers.add(s1);
//        payload.setServerRecord(servers);
//
//        assertTrue(ring.hasServer(self.getAddress(), 200));
//        List<Integer> codes = taskHandler.getDeathCodes(servers, self);
//        assertEquals(codes.get(0), STAT_CODE_OLD);
//        assertTrue(ring.hasServer(self.getAddress(), 200));
//        assertEquals(pendingRecordDeaths.size(), 0);
//    }
//}
