package com.g7.CPEN431.A12.newProto.KVRequest;

import com.g7.CPEN431.A12.consistentMap.ServerRecord;
import com.g7.CPEN431.A12.newProto.shared.MessageFactory;
import com.g7.CPEN431.A12.wrappers.UnwrappedPayload;

public class KVRequestFactory implements MessageFactory {
    @Override
    public Object create(String fullMessageName) {
        if(fullMessageName.equals("KVRequest")) return new UnwrappedPayload();
        if(fullMessageName.equals("ServerEntry")) return new ServerRecord(fullMessageName);
        if(fullMessageName.equals("PutPair")) return new KVPair();
        throw new IllegalArgumentException("Unknown message name: " + fullMessageName);
    }
}
