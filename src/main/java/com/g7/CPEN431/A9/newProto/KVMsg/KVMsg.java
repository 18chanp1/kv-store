package com.g7.CPEN431.A9.newProto.KVMsg;

import java.net.UnknownHostException;

public interface KVMsg {
boolean hasMessageID();
byte[] getMessageID();
void setMessageID(byte[] messageID);
boolean hasPayload();
byte[] getPayload();
void setPayload(byte[] payload);
boolean hasCheckSum();
long getCheckSum();
void setCheckSum(long checkSum);
boolean hasSourceAddress();
byte[] getSourceAddress();
void setSourceAddress(byte[] sourceAddress) throws UnknownHostException;
boolean hasSourcePort();
int getSourcePort();
void setSourcePort(int sourcePort);
}
