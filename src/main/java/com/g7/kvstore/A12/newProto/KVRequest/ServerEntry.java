package com.g7.CPEN431.A12.newProto.KVRequest;
public interface ServerEntry {
boolean hasServerAddress();
byte[] getServerAddress();
void setServerAddress(byte[] serverAddress);
boolean hasServerPort();
int getServerPort();
void setServerPort(int serverPort);
boolean hasInformationTime();
long getInformationTime();
void setInformationTime(long informationTime);
boolean hasCode();
int getCode();
void setCode(int code);
}
