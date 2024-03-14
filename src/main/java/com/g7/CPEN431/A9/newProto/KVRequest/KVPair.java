package com.g7.CPEN431.A9.newProto.KVRequest;

public class KVPair implements PutPair {
    byte[] key;
    byte[] value;
    int version = 0;
    public KVPair() {
    }

    public KVPair(byte[] key, byte[] value, int version) {
        this.key = key;
        this.value = value;
        this.version = version;
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

}
