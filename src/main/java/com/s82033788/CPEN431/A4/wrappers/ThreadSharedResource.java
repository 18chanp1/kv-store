package com.s82033788.CPEN431.A4.wrappers;

import java.net.DatagramSocket;

public class ThreadSharedResource {
    private byte[] byteArr;

    private DatagramSocket socket;

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
}
