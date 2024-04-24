package com.g7.CPEN431.A12.newProto.KVResponse;

import com.g7.CPEN431.A12.newProto.shared.MessageFactory;
import com.g7.CPEN431.A12.wrappers.UnwrappedPayload;

public class KVResponseFactory implements MessageFactory {
    @Override
    public Object create(String fullMessageName) {
        if(fullMessageName.equals("KVResponse")) return new UnwrappedPayload();
        throw new IllegalArgumentException("Unknown message name: " + fullMessageName);
    }
}