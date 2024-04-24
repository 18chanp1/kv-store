package com.g7.CPEN431.A12.newProto.KVRequest;

import java.util.Arrays;

public class KVPair implements PutPair {
    byte[] key;
    byte[] value;
    int version = 0;
    long insertionTime;
    public KVPair() {
    }

    public KVPair(byte[] key, byte[] value, int version, long insertionTime) {
        this.key = key;
        this.value = value;
        this.version = version;
        this.insertionTime = insertionTime;
    }

    @Override
    public boolean hasInsertionTime() {
        return true;
    }

    @Override
    public long getInsertionTime() {
        return insertionTime;
    }

    @Override
    public void setInsertionTime(long insertionTime) {
        this.insertionTime = insertionTime;
    }

    @Override
    public boolean hasKey() {
        return key != null;
    }

    @Override
    public byte[] getKey() {
        return key;
    }

    @Override
    public boolean hasVersion() {
        return true;
    }

    @Override
    public int getVersion() {
        return version;
    }

    @Override
    public void setVersion(int version) {
        this.version = version;
    }

    @Override
    public void setKey(byte[] key) {
        this.key = key;
    }

    @Override
    public boolean hasValue() {
        return value != null;
    }

    @Override
    public byte[] getValue() {
        return value;
    }

    @Override
    public void setValue(byte[] value) {
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        KVPair kvPair = (KVPair) o;

        return Arrays.equals(key, kvPair.key);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(key);
    }
}
