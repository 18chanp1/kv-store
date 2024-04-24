package com.g7.CPEN431.A12.client;


import com.g7.CPEN431.A12.newProto.KVResponse.KVResponse;

import java.util.List;

public class ServerResponse implements KVResponse {
    private int errcode;
    private byte[] value;
    private int pid;
    private boolean hasPID = false;
    private int version;
    private boolean hasVersion = false;
    private int ovlT;
    private boolean hasovlT = false;
    int members;
    private boolean hasMembers = false;
    List<Integer> serverStatusCode;

    @Override
    public boolean hasErrCode() {
        return true;
    }

    @Override
    public int getErrCode() {
        return errcode;
    }

    @Override
    public void setErrCode(int errCode) {
        this.errcode = errCode;

    }

    @Override
    public boolean hasValue() {
        return value != null;
    }

    @Override
    public byte[] getValue() {
        return value;
    }

    @Override
    public void setValue(byte[] value) {
        this.value = value;

    }

    @Override
    public boolean hasPid() {
        return this.hasPID;
    }

    @Override
    public int getPid() {
        return pid;
    }

    @Override
    public void setPid(int pid) {
        this.pid = pid;
        this.hasPID = true;
    }

    @Override
    public boolean hasVersion() {
        return this.hasVersion;
    }

    @Override
    public int getVersion() {
        return version;
    }

    @Override
    public void setVersion(int version) {
        this.version = version;
        this.hasVersion = true;
    }

    @Override
    public boolean hasOverloadWaitTime() {
        return hasovlT;
    }

    @Override
    public int getOverloadWaitTime() {
        return ovlT;
    }

    @Override
    public void setOverloadWaitTime(int overloadWaitTime) {
        ovlT = overloadWaitTime;
        this.hasovlT = true;
    }

    @Override
    public boolean hasMembershipCount() {
        return hasMembers;
    }

    @Override
    public int getMembershipCount() {
        return members;
    }

    @Override
    public void setMembershipCount(int membershipCount) {
        this.members = membershipCount;
        hasMembers = true;
    }

    @Override
    public boolean hasServerStatusCode() {
        return serverStatusCode != null;
    }

    @Override
    public List<Integer> getServerStatusCode() {
        return serverStatusCode;
    }

    @Override
    public void setServerStatusCode(List<Integer> serverStatusCode) {
        this.serverStatusCode = serverStatusCode;
    }
}
