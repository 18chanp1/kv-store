package com.g7.CPEN431.A7.newProto.KVResponse;

import com.g7.CPEN431.A7.client.ServerResponse;
import com.g7.CPEN431.A7.newProto.shared.MessageFactory;

public class ServerResponseFactory implements MessageFactory {
    @Override
    public Object create(String fullMessageName) {
        if(fullMessageName.equals("KVResponse")) return new ServerResponse();
        throw new IllegalArgumentException("Unknown message name: " + fullMessageName);
    }
}