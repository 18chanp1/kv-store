package com.g7.CPEN431.A7;

import com.g7.CPEN431.A7.consistentMap.ServerRecord;
import com.g7.CPEN431.A7.map.KeyWrapper;
import com.g7.CPEN431.A7.map.ValueWrapper;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ParentServerRecord {
    ServerRecord serverRecord;
    ConcurrentMap<KeyWrapper, ValueWrapper> map;
    ReadWriteLock mapLock;

    public ParentServerRecord(ServerRecord serverRecord, ConcurrentMap<KeyWrapper, ValueWrapper> map) {
        this.serverRecord = serverRecord;
        this.map = map;
        this.mapLock = new ReentrantReadWriteLock();
    }

    public ServerRecord getServerRecord() {
        return serverRecord;
    }

    public void setServerRecord(ServerRecord serverRecord) {
        this.serverRecord = serverRecord;
    }

    public ConcurrentMap<KeyWrapper, ValueWrapper> getMap() {
        return map;
    }

    public ReadWriteLock getMapLock() {
        return mapLock;
    }
}
