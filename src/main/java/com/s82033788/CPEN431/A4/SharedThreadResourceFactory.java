package com.s82033788.CPEN431.A4;

import com.s82033788.CPEN431.A4.wrappers.ThreadSharedResource;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;

import java.net.DatagramSocket;

public class SharedThreadResourceFactory implements PooledObjectFactory {
    @Override
    public void activateObject(PooledObject pooledObject) throws Exception {
        //nothing to do here
    }

    @Override
    public void destroyObject(PooledObject pooledObject) throws Exception {
        DatagramSocket socket = ((ThreadSharedResource) pooledObject.getObject()).getSocket();
        socket.close();
        //nothing to do here, let GC handle
    }

    @Override
    public PooledObject makeObject() throws Exception {
        return new DefaultPooledObject(new ThreadSharedResource(
                new byte[KVServer.PACKET_MAX],
                new DatagramSocket()));
    }

    @Override
    public void passivateObject(PooledObject pooledObject) throws Exception {
        //do nothing;

    }

    @Override
    public boolean validateObject(PooledObject pooledObject) {
        return true;
    }
}
