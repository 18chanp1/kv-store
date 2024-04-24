package com.g7.CPEN431.A12.cache;

import com.g7.CPEN431.A12.KVServerTaskHandler;
import com.g7.CPEN431.A12.map.ValueWrapper;
import com.g7.CPEN431.A12.newProto.KVMsg.KVMsgSerializer;
import com.g7.CPEN431.A12.newProto.KVResponse.KVResponse;
import com.g7.CPEN431.A12.newProto.KVResponse.KVResponseSerializer;
import com.g7.CPEN431.A12.wrappers.UnwrappedMessage;

import java.net.DatagramPacket;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.zip.CRC32;

public class RequestCacheValue implements KVResponse {
    private final ResponseType responseType;
    private final int errCode;
    private ValueWrapper value;
    private long pid;
    private int overloadWaitTime;
    private int membershipCount;
    private final byte[] reqID;
    private final long incomingCRC;
    private final InetAddress address;
    private final int port;
    private List<Integer> serverStatusCodes;

    private RequestCacheValue(Builder builder) {
        ResponseType type = builder.b_type;
        if(type == null) throw new IllegalArgumentException();

        this.responseType = type;

        this.address = builder.b_address;
        this.port = builder.b_port;
        this.reqID = builder.b_reqID;
        this.incomingCRC = builder.b_incomingCRC;


        switch (type) {
            case INVALID_KEY:       this.errCode = KVServerTaskHandler.RES_CODE_INVALID_KEY;        break;
            case INVALID_VALUE:     this.errCode = KVServerTaskHandler.RES_CODE_INVALID_VALUE;      break;
            case INVALID_OPCODE:    this.errCode = KVServerTaskHandler.RES_CODE_INVALID_OPCODE;     break;
            case ISALIVE:           this.errCode = KVServerTaskHandler.RES_CODE_SUCCESS;            break;
            case SHUTDOWN:          this.errCode = KVServerTaskHandler.RES_CODE_SUCCESS;            break;
            case PUT:               this.errCode = KVServerTaskHandler.RES_CODE_SUCCESS;            break;
            case DEL:               this.errCode = KVServerTaskHandler.RES_CODE_SUCCESS;            break;
            case WIPEOUT:           this.errCode = KVServerTaskHandler.RES_CODE_SUCCESS;            break;
            case INVALID_OPTIONAL:  this.errCode = KVServerTaskHandler.RES_CODE_INVALID_OPTIONAL;   break;
            case RETRY_NOT_EQUAL:   this.errCode = KVServerTaskHandler.RES_CODE_RETRY_NOT_EQUAL;    break;
            case NO_KEY:            this.errCode = KVServerTaskHandler.RES_CODE_NO_KEY;             break;
            case NO_MEM:            this.errCode = KVServerTaskHandler.RES_CODE_NO_MEM;             break;
            case OBITUARIES:
            {
                this.errCode = KVServerTaskHandler.RES_CODE_SUCCESS;
                this.serverStatusCodes = builder.b_serverStatusCodes;
                if(this.serverStatusCodes == null) throw new IllegalArgumentException();
                break;
            }
            case OVERLOAD_CACHE:
            {
                this.errCode = KVServerTaskHandler.RES_CODE_OVERLOAD;
                this.overloadWaitTime = KVServerTaskHandler.CACHE_OVL_WAIT_TIME;
                break;
            }
            case OVERLOAD_THREAD:
            {
                this.errCode = KVServerTaskHandler.RES_CODE_OVERLOAD;
                this.overloadWaitTime = KVServerTaskHandler.THREAD_OVL_WAIT_TIME;
                break;
            }
            //need to check arguments
            case MEMBERSHIP_COUNT:
            {
                if(!builder.b_membershipCount_set) throw new IllegalArgumentException();

                this.errCode = KVServerTaskHandler.RES_CODE_SUCCESS;
                this.membershipCount = builder.b_membershipCount;
                break;
            }
            case PID:
            {
                if(!builder.b_pid_set) throw new IllegalArgumentException();

                this.errCode = KVServerTaskHandler.RES_CODE_SUCCESS;
                this.pid = builder.b_pid;
                break;
            }
            case VALUE:
            {
                if (builder.b_value == null) throw new IllegalArgumentException();


                this.errCode = KVServerTaskHandler.RES_CODE_SUCCESS;
                this.value = builder.b_value;
                break;
            }
            default: throw new IllegalArgumentException();
        }
    }

    /* Constructors for each type of reply */
    public static class Builder {
        //mandatory params
        private ResponseType b_type;
        //optionals
        private int b_membershipCount;
        private boolean b_membershipCount_set = false;
        private long b_pid;
        private boolean b_pid_set = false;
        private ValueWrapper b_value;
        private final long b_incomingCRC;
        private final InetAddress b_address;
        private final int b_port;
        private final byte[] b_reqID;
        private List<Integer> b_serverStatusCodes;


        public Builder(long incomingCRC, InetAddress adr, int port, byte[] req_id) {
            this.b_incomingCRC = incomingCRC;
            this.b_address = adr;
            this.b_port = port;
            this.b_reqID = req_id;
        }

        public Builder setResponseType(ResponseType type) {
            if (b_type != null) throw new IllegalStateException("Type has been set already");
            this.b_type = type;
            return this;
        }

        public Builder setMembershipCount (int count) {
            if (b_membershipCount_set) throw new IllegalStateException("Membership has been set already");
            this.b_membershipCount = count;
            this.b_membershipCount_set = true;
            return this;
        }

        public Builder setPID (long pid) {
            if (b_pid_set) throw new IllegalStateException("PID set already");
            this.b_pid = pid;
            this.b_pid_set = true;
            return this;
        }

        public Builder setValue(ValueWrapper value) {
            if(b_value != null) throw new IllegalStateException("Value set already");
            this.b_value = value;
            return this;
        }

        public Builder setServerStatusCodes(List<Integer> statusCodes){
            if(statusCodes == null) throw new IllegalArgumentException();
            this.b_serverStatusCodes = statusCodes;
            return this;
        }

        public RequestCacheValue build() {
            return new RequestCacheValue(this);
        }


    }

    public byte[] generatePayload() {
        //first add the ID to the public buffer
        return KVResponseSerializer.serialize(this);
    }

    private long getCRC(byte[] id, byte[] payload)
    {
        ByteBuffer b = ByteBuffer.allocate(id.length + payload.length);
        b.put(id);
        b.put(payload);

        b.flip();

        CRC32 crc32 = new CRC32();
        crc32.update(b.array());
        return crc32.getValue();
    }

    public DatagramPacket generatePacket() {
        //prepare checksum
        byte[] payload = this.generatePayload();
        long msgChecksum = getCRC(reqID,payload);

        byte[] fullMsg;
        //prepare message
        fullMsg = KVMsgSerializer.serialize(new UnwrappedMessage(reqID, payload, msgChecksum));

        return new DatagramPacket(fullMsg, fullMsg.length, address, port);
    }

    @Override
    public boolean hasErrCode() {
        return true;
    }

    @Override
    public int getErrCode() {
        return this.errCode;
    }

    @Override
    public void setErrCode(int errCode) {
        throw new RuntimeException("A response is immutable");
    }

    @Override
    public boolean hasValue() {
        return value != null;
    }

    @Override
    public byte[] getValue() {
        return value.getValue();
    }

    @Override
    public void setValue(byte[] value) {
        //do nothing, a response is immutable
        throw new RuntimeException("A response is immutable");
    }

    @Override
    public boolean hasPid() {
        return responseType == ResponseType.PID;
    }

    @Override
    public int getPid() {
        return (int) pid;
    }

    @Override
    public void setPid(int pid) {
        throw new RuntimeException("Responses are immutable");
    }

    @Override
    public boolean hasVersion() {
        return responseType == ResponseType.VALUE;
    }

    @Override
    public int getVersion() {
        return value.getVersion();
    }

    @Override
    public void setVersion(int version) {
        throw new RuntimeException("Responses are immutable");
    }

    @Override
    public boolean hasOverloadWaitTime() {
        return responseType == ResponseType.OVERLOAD_CACHE || responseType == ResponseType.OVERLOAD_THREAD;
    }

    @Override
    public int getOverloadWaitTime() {
        return overloadWaitTime;
    }

    @Override
    public void setOverloadWaitTime(int overloadWaitTime) {
        throw new RuntimeException("Responses are immutable");
    }

    @Override
    public boolean hasMembershipCount() {
        return responseType == ResponseType.MEMBERSHIP_COUNT;
    }

    @Override
    public int getMembershipCount() {
        return membershipCount;
    }

    @Override
    public void setMembershipCount(int membershipCount) {
        throw new RuntimeException("Responses are immutable");
    }

    @Override
    public boolean hasServerStatusCode() {
        return this.serverStatusCodes != null;
    }

    @Override
    public List<Integer> getServerStatusCode() {
        return this.serverStatusCodes;
    }

    @Override
    public void setServerStatusCode(List<Integer> serverStatusCode) {
        this.serverStatusCodes = serverStatusCode;
    }
}

