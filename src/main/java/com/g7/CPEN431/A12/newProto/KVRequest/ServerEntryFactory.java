package com.g7.CPEN431.A12.newProto.KVRequest;

import com.g7.CPEN431.A12.consistentMap.ServerRecord;
import com.g7.CPEN431.A12.newProto.shared.MessageFactory;

public class ServerEntryFactory implements MessageFactory {
    public Object create(String fullMessageName) {
        if(fullMessageName.equals("ServerEntry")) return new ServerRecord(fullMessageName);
        throw new IllegalArgumentException("Unknown message name: " + fullMessageName);
    }
}
