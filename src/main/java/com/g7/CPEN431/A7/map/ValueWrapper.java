package com.g7.CPEN431.A7.map;

import com.g7.CPEN431.A7.newProto.KVRequest.ServerEntry;
import net.openhft.chronicle.bytes.BytesMarshallable;

import java.util.Arrays;

public class ValueWrapper implements BytesMarshallable {


    private final byte[] value;
    private final int version;
    private ServerEntry primaryServer;

    public ValueWrapper(byte[] value, int version, ServerEntry primaryServer) {
        this.value = value;
        this.version = version;
        this.primaryServer = primaryServer;
    }
    public byte[] getValue() {
        return value;
    }

    public int getVersion() {
        return version;
    }
    public ServerEntry getPrimaryServer(){
        return primaryServer;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ValueWrapper that = (ValueWrapper) o;
        return Arrays.equals(that.getValue(), value) && that.getVersion() == version;
    }

}
