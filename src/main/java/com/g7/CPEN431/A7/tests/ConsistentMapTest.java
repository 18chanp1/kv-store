package com.g7.CPEN431.A7.tests;

import com.g7.CPEN431.A7.consistentMap.ConsistentMap;
import com.g7.CPEN431.A7.consistentMap.ServerRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

public class ConsistentMapTest {
    static ConsistentMap DUT;
    @BeforeAll
    public static void setup()
    {
        DUT = new ConsistentMap(4, "servers.txt");
    }

    @Test
    @DisplayName("BasicTest")
    public void basicTest(){
        byte[] buf = new byte[15];
        new Random().nextBytes(buf);
        List<ServerRecord> l1=  DUT.getNReplicas(buf,2);
        List<ServerRecord> l2 = DUT.getNReplicas(buf, 2);

        assertIterableEquals(l1, l2);
        assertEquals(l1.size(), 2);
        assertNotEquals(l1.get(0), l1.get(1));
        assertEquals(l1.get(0), DUT.getServer(buf));
    }

    @Test
    @DisplayName("Invalid amount of replicas")
    public void getInvalidReplicas(){
        byte[] buf = new byte[15];
        new Random().nextBytes(buf);

        assertThrows(ConsistentMap.NoServersException.class, () -> DUT.getNReplicas(buf, 30));


    }
}
