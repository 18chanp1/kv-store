package com.g7.CPEN431.A12.map;

import net.openhft.chronicle.bytes.BytesMarshallable;

import java.util.Arrays;

public class ValueWrapper implements BytesMarshallable {


    private final byte[] value;
    private final int version;
    private final long insertTime;

    public ValueWrapper(byte[] value, int version, long insertTime) {
        this.value = value;
        this.version = version;
        this.insertTime = insertTime;
    }
    public byte[] getValue() {
        return value;
    }

    public int getVersion() {
        return version;
    }

    public long getInsertTime() {
        return insertTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ValueWrapper that = (ValueWrapper) o;
        return Arrays.equals(that.getValue(), value) && that.getVersion() == version;
    }

}
