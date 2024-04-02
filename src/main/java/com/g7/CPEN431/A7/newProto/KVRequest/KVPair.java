package com.g7.CPEN431.A7.newProto.KVRequest;

public class KVPair implements PutPair {
    byte[] key;
    byte[] value;
    int version = 0;
    boolean delete;
    public KVPair() {
    }

    public KVPair(byte[] key, byte[] value, int version, boolean delete) {
        this.key = key;
        this.value = value;
        this.version = version;
        this.delete = delete;
    }

    public KVPair(byte[] key, byte[] value, int version) {
        this.key = key;
        this.value = value;
        this.version = version;
        this.delete = false;
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
    //https://www.geeksforgeeks.org/overriding-equals-method-in-java/
    public boolean equals (Object o) {
        if (o == this) return true;
        if (!(o instanceof PutPair)) return false;
        PutPair p = (PutPair) o;
        return p.getKey()==this.getKey() && p.getValue() == this.getValue() && p.getVersion() == this.getVersion();
    }
    public boolean isDelete() {
        return delete;
    }
}
