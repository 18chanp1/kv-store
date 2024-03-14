package com.g7.CPEN431.A9.map;

import net.openhft.chronicle.bytes.BytesMarshallable;

import java.util.Arrays;

public class ValueWrapper implements BytesMarshallable {


    private final byte[] value;
    private final int version;

    public ValueWrapper(byte[] value, int version) {
        this.value = value;
        this.version = version;
    }
    public byte[] getValue() {
        return value;
    }

    public int getVersion() {
        return version;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ValueWrapper that = (ValueWrapper) o;
        return Arrays.equals(that.getValue(), value) && that.getVersion() == version;
    }

}
