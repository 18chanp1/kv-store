package com.g7.CPEN431.A12.newProto.KVResponse;

public interface KVResponse {
    boolean hasErrCode();
    int getErrCode();
    void setErrCode(int errCode);
    boolean hasValue();
    byte[] getValue();
    void setValue(byte[] value);
    boolean hasPid();
    int getPid();
    void setPid(int pid);
    boolean hasVersion();
    int getVersion();
    void setVersion(int version);
    boolean hasOverloadWaitTime();
    int getOverloadWaitTime();
    void setOverloadWaitTime(int overloadWaitTime);
    boolean hasMembershipCount();
    int getMembershipCount();
    void setMembershipCount(int membershipCount);
    boolean hasServerStatusCode();
    java.util.List<Integer> getServerStatusCode();
    void setServerStatusCode(java.util.List<Integer> serverStatusCode);
}
