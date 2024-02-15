package com.s82033788.CPEN431.A4.wrappers;

import com.s82033788.CPEN431.A4.newProto.KVMsg;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class UnwrappedMessage implements KVMsg {

    private byte[] msgID;
    private byte[] payload;
    private long crc;
    private InetAddress sourceIP;
    private int sourcePort;

    public UnwrappedMessage(byte[] msgID, byte[] payload, long crc) {
        this.msgID = msgID;
        this.payload = payload;
        this.crc = crc;
    }

    public UnwrappedMessage() {
    }

    @Override
    public boolean hasMessageID() {
        return msgID != null;
    }

    @Override
    public byte[] getMessageID() {
        return msgID;
    }

    @Override
    public void setMessageID(byte[] messageID) {
        this.msgID = messageID;
    }

    @Override
    public boolean hasPayload() {
        return this.payload != null;
    }

    @Override
    public void setPayload(byte[] payload) {
        this.payload = payload;
    }

    @Override
    public boolean hasCheckSum() {
        return true;
    }

    @Override
    public long getCheckSum() {
        return crc;
    }

    @Override
    public void setCheckSum(long checkSum) {
        crc = checkSum;
    }

    @Override
    public byte[] getPayload() {
        return this.payload;
    }

    @Override
    public boolean hasSourceAddress() {
        return sourceIP != null;
    }

    @Override
    public byte[] getSourceAddress() {
        return sourceIP.getAddress();
    }

    @Override
    public void setSourceAddress(byte[] sourceAddress) throws UnknownHostException {
        sourceIP = InetAddress.getByAddress(sourceAddress);
    }

    @Override
    public boolean hasSourcePort() {
        return hasSourceAddress();
    }

    @Override
    public int getSourcePort() {
        return sourcePort;
    }

    @Override
    public void setSourcePort(int sourcePort) {
        this.sourcePort = sourcePort;
    }
}
