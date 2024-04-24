package com.g7.CPEN431.A12.wrappers;

import com.g7.CPEN431.A12.newProto.KVMsg.KVMsg;
import com.g7.CPEN431.A12.newProto.KVMsg.KVMsgSerializer;
import com.g7.CPEN431.A12.consistentMap.ServerRecord;

import java.net.DatagramPacket;
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

    public void setSourceAddress(InetAddress address)
    {
        sourceIP = address;
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

    public DatagramPacket generatePacket(ServerRecord record)
    {
        byte[] msg = KVMsgSerializer.serialize(this);
        return new DatagramPacket(msg, msg.length, record.getAddress(), record.getPort());
    }

}
