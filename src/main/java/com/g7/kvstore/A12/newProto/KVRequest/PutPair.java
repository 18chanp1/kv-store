package com.g7.kvstore.A12.newProto.KVRequest;

public interface PutPair {
    boolean hasKey();
    byte[] getKey();
    void setKey(byte[] key);
    boolean hasValue();
    byte[] getValue();
    void setValue(byte[] value);
    boolean hasVersion();
    int getVersion();
    void setVersion(int version);
    boolean hasInsertionTime();
    long getInsertionTime();
    void setInsertionTime(long insertionTime);
}

