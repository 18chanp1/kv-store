package com.g7.CPEN431.A12.consistentMap;

import com.g7.CPEN431.A12.newProto.KVRequest.ServerEntry;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Instant;

public class ServerRecord implements ServerEntry {
    private InetAddress address;
    private int port;
    private long informationTime;
    private int updateCode;


    private boolean portExists = false;
    private boolean informationTimeExists = false;
    private boolean updateCodeExists = false;


    public final static int CODE_ALI = 0x1;
    public final static int CODE_DED = 0x2;


    public ServerRecord(InetAddress address, int port) {
        this.address = address;
        this.port = port;
        this.portExists = true;
        this.informationTime = Instant.now().toEpochMilli();
        this.informationTimeExists = true;
        this.updateCode = 1;
        this.updateCodeExists = true;
    }

    /* Clone a ServerRecord */
    public ServerRecord(ServerRecord r)
    {
        this.address = r.address;
        this.port = r.port;
        this.portExists = r.portExists;
        this.informationTime = r.informationTime;
        this.informationTimeExists = r.informationTimeExists;
        this.updateCode = r.updateCode;
        this.updateCodeExists = r.updateCodeExists;
    }



    /**
     * This is not for you to fucking use. It is for the message parser.
     */
    public ServerRecord(String s) {
        if(!s.equals("ServerEntry")) throw new IllegalArgumentException();
    }

    public InetAddress getAddress() {
        return address;
    }

    public int getPort() {
        return port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ServerRecord that = (ServerRecord) o;

        if (port != that.port) return false;
        return address.equals(that.address);
    }

    @Override
    public int hashCode() {
        int result = address.hashCode();
        result = 31 * result + port;
        return result;
    }

    @Override
    public boolean hasServerAddress() {
        return address != null;
    }

    @Override
    public byte[] getServerAddress() {
        return address.getAddress();
    }

    @Override
    public void setServerAddress(byte[] serverAddress) {
        try {
            this.address = InetAddress.getByAddress(serverAddress);
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public boolean hasServerPort() {
        return this.portExists;
    }

    @Override
    public int getServerPort() {
        return this.port;
    }

    @Override
    public void setServerPort(int serverPort) {
        this.port = serverPort;
        this.portExists = true;
    }

    public void setLastSeenNow()
    {
        setInformationTime(Instant.now().toEpochMilli());
        setCode(CODE_ALI);
    }

    public void setAliveAtTime(long time)
    {
        setInformationTime(time);
        setCode(CODE_ALI);
    }


    public void setLastSeenDeadAt(long time)
    {
        setInformationTime(time);
        setCode(CODE_DED);
    }

    @Override
    public boolean hasInformationTime() {
        return this.informationTimeExists;
    }

    @Override
    public long getInformationTime() {
        return this.informationTime;
    }

    @Override
    public void setInformationTime(long informationTime) {
        this.informationTime = informationTime;
        this.informationTimeExists = true;
    }

    @Override
    public boolean hasCode() {
        return updateCodeExists;
    }

    @Override
    public int getCode() {
        return updateCode;
    }

    @Override
    public void setCode(int code) {
        this.updateCode = code;
        this.updateCodeExists = true;
    }

    public boolean isAlive()
    {
        return this.updateCode == CODE_ALI;
    }

    public class HashNotGeneratedException extends Exception {}


}
