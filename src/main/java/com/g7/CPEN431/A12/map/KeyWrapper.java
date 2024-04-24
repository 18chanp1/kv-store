package com.g7.CPEN431.A12.map;

import net.openhft.chronicle.bytes.BytesMarshallable;

import java.util.Arrays;

public class KeyWrapper implements BytesMarshallable {
    byte[] key;

    public KeyWrapper(byte[] key) {
        this.key = key;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KeyWrapper that = (KeyWrapper) o;
        return Arrays.equals(key, that.key);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(key);
    }

    public byte[] getKey() {
        return key;
    }
}
