//package com.g7.CPEN431.A7.tests;
//
//import com.g7.CPEN431.A7.KVServer;
//import com.g7.CPEN431.A7.consistentMap.ConsistentMap;
//import com.g7.CPEN431.A7.consistentMap.ForwardList;
//import com.g7.CPEN431.A7.consistentMap.ServerRecord;
//import com.g7.CPEN431.A7.map.KeyWrapper;
//import com.g7.CPEN431.A7.map.ValueWrapper;
//import org.junit.jupiter.api.BeforeAll;
//import org.junit.jupiter.api.DisplayName;
//import org.junit.jupiter.api.Test;
//
//import java.net.InetAddress;
//import java.net.UnknownHostException;
//import java.util.*;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.ConcurrentMap;
//
//import static org.junit.jupiter.api.Assertions.*;
//
//public class ConsistentMapTest {
//    static ConsistentMap DUT;
//    @BeforeAll
//    public static void setup()
//    {
//        KVServer server = new KVServer();
//        DUT = new ConsistentMap(4, 2, "servers.txt");
//    }
//
//    @Test
//    @DisplayName("BasicTest")
//    public void basicTest(){
//        byte[] buf = new byte[15];
//        new Random().nextBytes(buf);
//        List<ServerRecord> l1=  DUT.getNReplicas(buf);
//        List<ServerRecord> l2 = DUT.getNReplicas(buf);
//
//        assertIterableEquals(l1, l2);
//        assertEquals(l1.size(), 2);
//        assertNotEquals(l1.get(0), l1.get(1));
//        assertEquals(l1.get(0), DUT.getServer(buf));
//    }
//
//    @Test
//    @DisplayName("Remove servers and get entries to be forwarded")
//    public void serverRemovalTest() throws UnknownHostException {
//        byte[] k = new byte[15];
//        byte[] v = new byte[15];
//        new Random().nextBytes(k);
//        new Random().nextBytes(v);
//        Set<Map.Entry<KeyWrapper, ValueWrapper>> m = new HashSet<>();
//        m.add(new Map.Entry<KeyWrapper, ValueWrapper>() {
//            @Override
//            public KeyWrapper getKey() {
//                return new KeyWrapper(k);
//            }
//
//            @Override
//            public ValueWrapper getValue() {
//                return new ValueWrapper(v, 0);
//            }
//
//            @Override
//            public ValueWrapper setValue(ValueWrapper value) {
//                return value;
//            }
//        });
//
//        // mark one of them as dead
//        ServerRecord s = new ServerRecord(InetAddress.getByName("206.12.45.156"), 13788);
//        s.setLastSeenDeadAt(System.currentTimeMillis());
//        DUT.updateServerState(s);
//        Collection<ForwardList> c = DUT.getEntriesToBeForwarded(m);
//    }
//
//    @Test
//    @DisplayName("Invalid amount of replicas")
//    public void getInvalidReplicas(){
//        DUT = new ConsistentMap(4, 30, "servers.txt");
//        byte[] buf = new byte[15];
//        new Random().nextBytes(buf);
//
//        assertThrows(ConsistentMap.NoServersException.class, () -> DUT.getNReplicas(buf));
//    }
//
//
//}
