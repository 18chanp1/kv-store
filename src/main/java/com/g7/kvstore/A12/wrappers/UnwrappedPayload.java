package com.g7.CPEN431.A12.wrappers;

import com.g7.CPEN431.A12.newProto.KVRequest.KVRequest;
import com.g7.CPEN431.A12.newProto.KVRequest.PutPair;
import com.g7.CPEN431.A12.newProto.KVRequest.ServerEntry;

import java.util.List;

/**
 * A class than encapsulates the payload of an incoming requrest. Self Explanatory.
 */
public class UnwrappedPayload implements KVRequest {
    private int command;
    private byte[] key;
    private byte[] value;
    private int version = 0; //defaults to zero anyways, no logic
    private List<ServerEntry> obituaryUpdates;
    private List<PutPair> bulkPutPair;

    private boolean keyExists = false;
    private boolean valueExists = false;
    private boolean versionExists = false;

    public UnwrappedPayload() {
    }

    @Override
    public boolean hasCommand() {
        return true;
    }

    public int getCommand() {
       return command;
    }

    @Override
    public void setCommand(int command) {
        this.command = command;
    }

    @Override
    public boolean hasKey() {
        return keyExists;
    }

    @Override
    public byte[] getKey() {
        return key;
    }

    @Override
    public void setKey(byte[] key) {
        this.key = key;
        keyExists = true;
    }

    @Override
    public boolean hasValue() {
        return valueExists;
    }

    @Override
    public byte[] getValue() {
        return value;
    }

    @Override
    public void setValue(byte[] value) {
        this.value = value;
        valueExists = true;
    }

    @Override
    public boolean hasVersion() {
        return versionExists;
    }

    @Override
    public int getVersion() {
        return version;
    }

    @Override
    public void setVersion(int version) {
        this.version = version;
        versionExists = true;
    }

    @Override
    public boolean hasServerRecord() {
        return obituaryUpdates != null;
    }

    @Override
    public List<ServerEntry> getServerRecord() {
        return obituaryUpdates;
    }

    @Override
    public void setServerRecord(List<ServerEntry> serverRecord) {
        this.obituaryUpdates = serverRecord;
    }

    @Override
    public boolean hasPutPair() {
        return bulkPutPair != null;
    }

    @Override
    public List<PutPair> getPutPair() {
        return bulkPutPair;
    }

    @Override
    public void setPutPair(List<PutPair> putPair) {
        this.bulkPutPair = putPair;
    }
}
