//package com.g7.CPEN431.A7.tests;
//
//import com.g7.CPEN431.A7.consistentMap.ServerRecord;
//import com.g7.CPEN431.A7.map.KeyWrapper;
//import com.g7.CPEN431.A7.map.ValueWrapper;
//import com.g7.CPEN431.A7.newProto.KVRequest.KVPair;
//import com.g7.CPEN431.A7.newProto.KVRequest.PutPair;
//import com.g7.CPEN431.A7.newProto.KVRequest.ServerEntry;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.DisplayName;
//import org.junit.jupiter.api.Test;
//
//import java.net.UnknownHostException;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.ConcurrentMap;
//import java.util.concurrent.atomic.AtomicInteger;
//import java.util.concurrent.locks.Lock;
//import java.util.concurrent.locks.ReadWriteLock;
//import java.util.concurrent.locks.ReentrantReadWriteLock;
//import com.g7.CPEN431.A7.KVServerTaskHandler;
//
//
//import static com.g7.CPEN431.A7.KVServerTaskHandler.STAT_CODE_NEW;
//import static com.g7.CPEN431.A7.KVServerTaskHandler.*;
//import static org.junit.jupiter.api.Assertions.*;
//
//public class BulkPutTest {
//    private ConcurrentMap<KeyWrapper, ValueWrapper> map;
//    private ReadWriteLock mapLock;
//    private AtomicInteger bytesUsed;
//    private List<PutPair> pairs;
//
//    private KVServerTaskHandler handler;
//
//    List<Integer> codes;
//
//    @BeforeEach
//    @DisplayName("setup")
//    public void setup(){
//        map = new ConcurrentHashMap<KeyWrapper, ValueWrapper>();
//        pairs = new ArrayList<>();
//        mapLock = new ReentrantReadWriteLock();
//        for(int i = 0; i < 10; i++){
//            PutPair pair = new KVPair();
//            pair.setKey(new byte[]{(byte) i});
//            pair.setValue(new byte[]{(byte) i});
//            pairs.add(pair);
//        }
//        codes = new ArrayList<>();
//        bytesUsed = new AtomicInteger(0);
//        handler = new KVServerTaskHandler(map, mapLock, bytesUsed);
//    }
//
//    @Test
//    @DisplayName("Test bulk put without key or put without value")
//    public void BulkPutWithKeyOrValueNull() throws UnknownHostException {
//        pairs.get(0).setKey(null);
//        pairs.get(5).setValue(null);
//        codes = handler.bulkPutHelper(pairs);
//        assertEquals(codes.size(), 10);
//        for(int i = 0; i < 10; i++)
//        {
//            if(i == 0 || i == 5){
//                assertEquals(codes.get(i), RES_CODE_INVALID_OPTIONAL);
//            } else{
//                assertEquals(codes.get(i), RES_CODE_SUCCESS);
//            }
//        }
//
//        ConcurrentMap<KeyWrapper, ValueWrapper> handler_map = handler.getMap();
//        for(int i = 0; i < 10; i++){
//            KeyWrapper key = new KeyWrapper(new byte[]{(byte) i});
//            if(i == 0 || i == 5) {
//                assertNull(handler_map.get(key));
//            } else{
//                assertEquals(handler_map.get(key), new ValueWrapper(new byte[]{(byte) i}, 0));
//            }
//        }
//    }
//
//    @Test
//    @DisplayName("Test bulk put with key too long or length is 0")
//    public void BulkPutWithKeyTooLongOrZero() throws UnknownHostException {
//        pairs.get(0).setKey(new byte[]{});
//
//        byte[] longKey = new byte[KEY_MAX_LEN + 1];
//        for(int i = 0; i < KEY_MAX_LEN + 1; i++){
//            longKey[i] = (byte)i;
//        }
//
//        pairs.get(5).setKey(longKey);
//
//        codes = handler.bulkPutHelper(pairs);
//
//        assertEquals(codes.size(), 10);
//        for(int i = 0; i < 10; i++)
//        {
//            if(i == 0 || i == 5){
//                assertEquals(codes.get(i), RES_CODE_INVALID_KEY);
//            } else{
//                assertEquals(codes.get(i), RES_CODE_SUCCESS);
//            }
//        }
//
//        ConcurrentMap<KeyWrapper, ValueWrapper> handler_map = handler.getMap();
//        for(int i = 0; i < 10; i++){
//            KeyWrapper key = new KeyWrapper(new byte[]{(byte) i});
//            if(i == 0 || i == 5) {
//                assertNull(handler_map.get(key));
//            } else{
//                assertEquals(handler_map.get(key), new ValueWrapper(new byte[]{(byte) i}, 0));
//            }
//        }
//    }
//
//    @Test
//    @DisplayName("Test bulk put with value too long")
//    public void BulkPutWithValueTooLong() throws UnknownHostException {
//        byte[] longValue = new byte[VALUE_MAX_LEN+1];
//        for(int i = 0; i < VALUE_MAX_LEN + 1; i++){
//            longValue[i] = (byte)i;
//        }
//
//        pairs.get(5).setValue(longValue);
//
//        codes = handler.bulkPutHelper(pairs);
//
//        assertEquals(codes.size(), 10);
//        for(int i = 0; i < 10; i++)
//        {
//            if(i == 5){
//                assertEquals(codes.get(i), RES_CODE_INVALID_VALUE);
//            } else{
//                assertEquals(codes.get(i), RES_CODE_SUCCESS);
//            }
//        }
//
//        ConcurrentMap<KeyWrapper, ValueWrapper> handler_map = handler.getMap();
//        for(int i = 0; i < 10; i++){
//            KeyWrapper key = new KeyWrapper(new byte[]{(byte) i});
//            if(i == 5) {
//                assertNull(handler_map.get(key));
//            } else{
//                assertEquals(handler_map.get(key), new ValueWrapper(new byte[]{(byte) i}, 0));
//            }
//        }
//    }
//
//    @Test
//    @DisplayName("Test bulk put with not enough memory")
//    public void BulkPutWithNotEnoughMemory() throws UnknownHostException {
//        bytesUsed.set(MAP_SZ + 1);
//        codes = handler.bulkPutHelper(pairs);
//
//        assertEquals(codes.size(), 10);
//        for(int i = 0; i < 10; i++)
//        {
//            if(i == 0){
//                assertEquals(codes.get(i), RES_CODE_NO_MEM);
//            } else{
//                assertEquals(codes.get(i), RES_CODE_SUCCESS);
//            }
//        }
//
//        ConcurrentMap<KeyWrapper, ValueWrapper> handler_map = handler.getMap();
//        for(int i = 0; i < 10; i++){
//            KeyWrapper key = new KeyWrapper(new byte[]{(byte) i});
//            if(i == 0) {
//                assertNull(handler_map.get(key));
//            } else{
//                assertEquals(handler_map.get(key), new ValueWrapper(new byte[]{(byte) i}, 0));
//            }
//        }
//    }
//}
