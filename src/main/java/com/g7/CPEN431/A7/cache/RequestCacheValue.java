package com.g7.CPEN431.A7.cache;

import com.g7.CPEN431.A7.KVServerTaskHandler;
import com.g7.CPEN431.A7.map.ValueWrapper;
import com.g7.CPEN431.A7.newProto.KVMsg.KVMsgSerializer;
import com.g7.CPEN431.A7.newProto.KVResponse.KVResponse;
import com.g7.CPEN431.A7.newProto.KVResponse.KVResponseSerializer;
import com.g7.CPEN431.A7.wrappers.PublicBuffer;
import com.g7.CPEN431.A7.wrappers.UnwrappedMessage;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.util.List;

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
    private final PublicBuffer pb;
    private List<Integer> serverStatusCodes;

    private RequestCacheValue(Builder builder) {
        ResponseType type = builder.b_type;
        if(type == null) throw new IllegalArgumentException();

        this.responseType = type;

        this.address = builder.b_address;
        this.port = builder.b_port;
        this.reqID = builder.b_reqID;
        this.incomingCRC = builder.b_incomingCRC;
        this.pb = builder.b_pb;


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
        private final PublicBuffer b_pb;
        private List<Integer> b_serverStatusCodes;


        public Builder(long incomingCRC, InetAddress adr, int port, byte[] req_id, PublicBuffer pb) {
            this.b_incomingCRC = incomingCRC;
            this.b_address = adr;
            this.b_port = port;
            this.b_reqID = req_id;
            this.b_pb = pb;
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

        //I'm sorry for adding these but i needed the address
        public InetAddress getAddress() {
            return b_address;
        }

        public int getPort() {
            return b_port;
        }


    }

    public PublicBuffer generatePayload() {
        //first add the ID to the public buffer
        try {
            pb.writeIDToPB().write(reqID);
        } catch (IOException e) {
            throw new RuntimeException("Failed to write to public buffer");
        }

        KVResponseSerializer.serialize(this, pb.writePayloadToPBAfterID());

        return pb;
    }

    public DatagramPacket generatePacket() {
        //prepare checksum
        PublicBuffer pb = this.generatePayload();
        long msgChecksum = pb.getCRCFromBody();

        int pl_len = pb.getLenOfPayload();

        byte[] fullMsg;
        //prepare message
        try {
            fullMsg = KVMsgSerializer.serialize(new UnwrappedMessage(reqID, pb.readPayloadFromPBBody().readNBytes(pl_len), msgChecksum));

        } catch (IOException e) {
            throw new RuntimeException("Failed to write to public buffer");
        }

        pb.returnBackingArrayAndClose();
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

