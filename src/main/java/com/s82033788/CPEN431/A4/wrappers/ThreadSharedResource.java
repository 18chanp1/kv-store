package com.s82033788.CPEN431.A4.wrappers;

import java.net.DatagramSocket;

public class ThreadSharedResource {
    private final byte[] byteArr;

    private final DatagramSocket socket;

    public ThreadSharedResource(byte[] byteArr, DatagramSocket socket) {
        this.byteArr = byteArr;
        this.socket = socket;
    }

    public byte[] getByteArr() {
        return byteArr;
    }

    public DatagramSocket getSocket() {
        return socket;
    }

    @Override
    public boolean equals(Object o) {
        return this == o;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }
}
