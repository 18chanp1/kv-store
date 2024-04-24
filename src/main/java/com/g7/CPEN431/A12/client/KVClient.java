package com.g7.CPEN431.A12.client;


import com.g7.CPEN431.A12.newProto.KVMsg.KVMsgFactory;
import com.g7.CPEN431.A12.newProto.KVMsg.KVMsgSerializer;
import com.g7.CPEN431.A12.newProto.KVRequest.KVRequestSerializer;
import com.g7.CPEN431.A12.newProto.KVRequest.PutPair;
import com.g7.CPEN431.A12.newProto.KVRequest.ServerEntry;
import com.g7.CPEN431.A12.newProto.KVResponse.KVResponseSerializer;
import com.g7.CPEN431.A12.newProto.KVResponse.ServerResponseFactory;
import com.g7.CPEN431.A12.wrappers.UnwrappedMessage;
import com.g7.CPEN431.A12.wrappers.UnwrappedPayload;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.List;
import java.util.zip.CRC32;

import static com.g7.CPEN431.A12.KVServer.self;

public class KVClient {
    private InetAddress serverAddress;
    private int serverPort;
    private DatagramSocket socket;
    byte[] publicBuf;
    int testSequence;
    UnwrappedMessage messageOnWire;
    static InetAddress blackHoleInet;
    {
        try {
            blackHoleInet = Inet4Address.getByName("240.0.0.0");
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }
    static byte[] blackHole;
    {
        try {
            blackHole = Inet4Address.getByName("240.0.0.0").getAddress();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }
    byte[] myAddress =  self.getAddress().getAddress();

    private int timeout = 300;
    private int triesMax = 4;

    /* Test Result codes */
    public final static int TEST_FAILED = 0;
    public final static int TEST_PASSED = 1;
    public final static int TEST_UNDECIDED = 2;

    /* Request Codes */
    public final static int REQ_CODE_PUT = 0x01;
    public final static int REQ_CODE_GET = 0X02;
    public final static int REQ_CODE_DEL = 0X03;
    public final static int REQ_CODE_SHU = 0X04;
    public final static int REQ_CODE_WIP = 0X05;
    public final static int REQ_CODE_ALI = 0X06;
    public final static int REQ_CODE_PID = 0X07;
    public final static int REQ_CODE_MEM = 0X08;
    public final static int REQ_CODE_DED = 0x100;
    public final static int REQ_CODE_BULKPUT = 0x200;

    public final static int RES_CODE_SUCCESS = 0x0;
    public final static int RES_CODE_NO_KEY = 0x1;
    public final static int RES_CODE_NO_MEM = 0x2;
    public final static int RES_CODE_OVERLOAD = 0x3;
    public final static int RES_CODE_INTERNAL_ER = 0x4;
    public final static int RES_CODE_INVALID_OPCODE = 0x5;
    public final static int RES_CODE_INVALID_KEY = 0x6;
    public final static int RES_CODE_INVALID_VALUE = 0x7;
    public final static int RES_CODE_INVALID_OPTIONAL = 0x21;
    public final static int RES_CODE_RETRY_NOT_EQUAL = 0x22;

    public KVClient(InetAddress serverAddress, int serverPort, DatagramSocket socket, byte[] publicBuf) throws SocketException {
        this.serverAddress = serverAddress;
        this.serverPort = serverPort;
        this.socket = socket;
        this.publicBuf = publicBuf;

        socket.setSoTimeout(timeout);
    }

    public KVClient(byte[] publicBuf){
        this.publicBuf = publicBuf;
    }

    public KVClient(byte[] publicBuf, int timeout, int tries) {
        this.publicBuf = publicBuf;
        this.timeout = timeout;
        this.triesMax = tries;
    }

    public KVClient(InetAddress serverAddress, int serverPort, DatagramSocket socket, byte[] publicBuf, int testSequence) {
        this.serverAddress = serverAddress;
        this.serverPort = serverPort;
        this.socket = socket;
        this.publicBuf = publicBuf;
        this.testSequence = testSequence;
    }

    public void setDestination(InetAddress serverAddress, int serverPort)
    {
        if(this.socket != null && !this.socket.isClosed())
        {
            this.socket.close();
        }

        //create a new socket to clear the buffer.
        try {
            this.socket = new DatagramSocket();
            this.socket.setSoTimeout(timeout);
        } catch (SocketException e) {
            throw new RuntimeException(e);
        }

        this.serverAddress = serverAddress;
        this.serverPort = serverPort;
    }
    public int administrationTest() throws IOException {
        System.out.println("Administration Test. PID -> ALI -> MEM -> SHU -> ALI");

        /* Generate some random values for key, value, and version */

        ServerResponse p;
        System.out.println("Getting PID");
        try {
            p = getPID();
        } catch (Exception e) {
            return handleException(e);
        }

        if(p.getErrCode() != RES_CODE_SUCCESS){
            System.out.println("PID failed with error code: " + p.getErrCode());
            return TEST_FAILED;
        }

        System.out.println("Checking server is alive");
        ServerResponse a;
        try {
            a = isAlive();
        } catch (Exception e) {
            return handleException(e);
        }

        if(a.getErrCode() != RES_CODE_SUCCESS){
            System.out.println("ALI with error code: " + p.getErrCode());
            return TEST_FAILED;
        }

        System.out.println("Checking Membership Count");
        ServerResponse m;
        try {
            m = getMembershipCount();
        } catch (Exception e) {
            return handleException(e);
        }

        if(m.getErrCode() != RES_CODE_SUCCESS){
            System.out.println("MEM with error code: " + p.getErrCode());
            return TEST_FAILED;
        }

        if(m.getMembershipCount() != 1) {
            System.out.println("MEM count mismatch. Count:" + p.getMembershipCount());
            return TEST_FAILED;
        }

        try {
            shutdown();
        } catch (ServerTimedOutException e) {
            System.out.println("Server shut down");
        } catch (Exception e) {
            return handleException(e);
        }

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            System.out.println("Sleep interrupted");
            return TEST_UNDECIDED;
        }

        //Test isAlive
        try {
            isAlive();
        } catch (ServerTimedOutException e) {
            System.out.println("Server is not alive after shutdown");
            System.out.println("***PASSED***");
            return TEST_PASSED;
        } catch (Exception e) {
            return handleException(e);
        }

        System.out.println("Server is still alive");
        return TEST_FAILED;
    }


   /* Helper functions corresponding to each type of request */
    private void shutdown() throws IOException, InterruptedException, ServerTimedOutException, MissingValuesException {
        /* Generate isAlive Message */
        UnwrappedPayload pl = new UnwrappedPayload();
        pl.setCommand(REQ_CODE_SHU);

        System.out.println("Sleeping for 5s");
        Thread.sleep(5000);


        sendAndReceiveServerResponse(pl);
    }
    private ServerResponse wipeout() throws IOException, ServerTimedOutException, MissingValuesException, InterruptedException {
        /* Generate isAlive Message */
        UnwrappedPayload pl = new UnwrappedPayload();
        pl.setCommand(REQ_CODE_WIP);


        return sendAndReceiveServerResponse(pl);
    }
    private ServerResponse delete(byte[] key) throws IOException, ServerTimedOutException, MissingValuesException, InterruptedException {
        /* Generate isAlive Message */
        UnwrappedPayload pl = new UnwrappedPayload();
        pl.setCommand(REQ_CODE_DEL);
        pl.setKey(key);


        return sendAndReceiveServerResponse(pl);
    }

    private ServerResponse get(byte[] key) throws IOException, MissingValuesException, ServerTimedOutException, InterruptedException {
        /* Generate isAlive Message */
        UnwrappedPayload pl = new UnwrappedPayload();
        pl.setCommand(REQ_CODE_GET);
        pl.setKey(key);


        ServerResponse res = sendAndReceiveServerResponse(pl);

        if(res.getErrCode() == RES_CODE_SUCCESS && !res.hasValue())
            throw new MissingValuesException("Value");

        if(res.getErrCode() == RES_CODE_SUCCESS && !res.hasVersion())
            throw new MissingValuesException("Version");

        return res;
    }

    public ServerResponse bulkPut(List<PutPair> pairs) throws IOException, ServerTimedOutException, MissingValuesException, InterruptedException {
        UnwrappedPayload pl = new UnwrappedPayload();
        assert pairs != null;
        pl.setCommand(REQ_CODE_BULKPUT);
        pl.setPutPair(pairs);
        return sendAndReceiveServerResponse(pl);
    }

    public void bulkPutPump(List<PutPair> pairs) throws IOException, ServerTimedOutException, MissingValuesException, InterruptedException {
        UnwrappedPayload pl = new UnwrappedPayload();
        assert pairs != null;
        pl.setCommand(REQ_CODE_BULKPUT);
        pl.setPutPair(pairs);
        pumpAndDump(pl);
    }

    public static byte[] bulkPutPumpStatic(List<PutPair> pairs)
    {
        UnwrappedPayload pl = new UnwrappedPayload();
        assert pairs != null;
        pl.setCommand(REQ_CODE_BULKPUT);
        pl.setPutPair(pairs);
        return pumpAndDumpStatic(pl);
    }

    private ServerResponse put(byte[] key, byte[] value, int version) throws IOException, ServerTimedOutException, MissingValuesException, InterruptedException {
        /* Generate isAlive Message */
        UnwrappedPayload pl = new UnwrappedPayload();
        pl.setCommand(REQ_CODE_PUT);
        pl.setKey(key);
        pl.setValue(value);
        pl.setVersion(version);

        return sendAndReceiveServerResponse(pl);
    }
    private ServerResponse getMembershipCount() throws IOException, MissingValuesException, ServerTimedOutException, InterruptedException {
        /* Generate isAlive Message */
        UnwrappedPayload pl = new UnwrappedPayload();
        pl.setCommand(REQ_CODE_MEM);

        ServerResponse res = sendAndReceiveServerResponse(pl);

        if(res.getErrCode() == RES_CODE_SUCCESS && !res.hasMembershipCount())
            throw new MissingValuesException("Membership Count");

        return res;
    }
    private ServerResponse getPID() throws IOException, ServerTimedOutException, MissingValuesException, InterruptedException {
        /* Generate isAlive Message */
        UnwrappedPayload pl = new UnwrappedPayload();
        pl.setCommand(REQ_CODE_PID);

        ServerResponse res = sendAndReceiveServerResponse(pl);

        if(res.getErrCode() == RES_CODE_SUCCESS && !res.hasPid())
            throw new MissingValuesException("PID");

        return res;
    }
    public ServerResponse isAlive() throws IOException, ServerTimedOutException, MissingValuesException, InterruptedException {
        /* Generate isAlive Message */
        UnwrappedPayload pl = new UnwrappedPayload();
        pl.setCommand(REQ_CODE_ALI);

        return sendAndReceiveServerResponse(pl);
    }

    public ServerResponse isDead(List<ServerEntry> l) throws MissingValuesException, IOException, ServerTimedOutException, InterruptedException {
        UnwrappedPayload pl = new UnwrappedPayload();
        pl.setCommand(REQ_CODE_DED);
        pl.setServerRecord(l);

        return sendAndReceiveServerResponse(pl);
    }

    /* Utilities to generate and process outgoing and incoming packets following the retry and At most once policy */
    private ServerResponse sendAndReceiveServerResponse(UnwrappedPayload pl) throws
            IOException,
            ServerTimedOutException,
            InterruptedException,
            MissingValuesException {
        ServerResponse res;

        if(this.socket == null || this.serverAddress == null || this.serverPort == 0)
        {
            throw new RuntimeException("Destination not set");
        }

        UnwrappedMessage msg = generateMessage(pl);
        res = sendAndReceiveSingleServerResponse(msg);

       /* Send a new packet after overload time*/
        while(res.getErrCode() == RES_CODE_OVERLOAD){
            if(!res.hasOverloadWaitTime()) throw new MissingValuesException("Overload wait time");
            Thread.sleep(res.getOverloadWaitTime());

            msg = generateMessage(pl);
            res = sendAndReceiveSingleServerResponse(msg);
        }

        return res;
    }
    private void pumpAndDump(UnwrappedPayload pl) throws
            IOException,
            ServerTimedOutException,
            InterruptedException,
            MissingValuesException {
        ServerResponse res;

        if(this.socket == null || this.serverAddress == null || this.serverPort == 0)
        {
            throw new RuntimeException("Destination not set");
        }

        UnwrappedMessage msg = generateMessage(pl);
        msg.setSourcePort(8888);
        msg.setSourceAddress(blackHole);

        byte[] msgb = KVMsgSerializer.serialize(msg);
        DatagramPacket p = new DatagramPacket(msgb, msgb.length, serverAddress, serverPort);
        messageOnWire = msg;


        for(int i = 0; i < triesMax; i++)
        {
            socket.send(p);
        }
    }

    private static byte[] pumpAndDumpStatic(UnwrappedPayload pl)
    {

        UnwrappedMessage msg = generateMessageStatic(pl);
        msg.setSourcePort(8888);
        msg.setSourceAddress(blackHoleInet);

        return KVMsgSerializer.serialize(msg);
    }

     byte[] generateMsgID() throws UnknownHostException {
        byte[] rand = new byte[2];
        try {
            SecureRandom.getInstanceStrong().nextBytes(rand);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }

        ByteBuffer idAndPl = ByteBuffer.allocate(16);
        idAndPl.order(ByteOrder.LITTLE_ENDIAN);
        idAndPl.put(myAddress);
        idAndPl.putShort((short) (socket.getPort() - 32768));
        idAndPl.put(rand);
        idAndPl.putLong(System.nanoTime());

        return idAndPl.array();
    }

    static byte[] generateMsgIDStatic()
    {
        byte[] rand = new byte[2];
        try {
            SecureRandom.getInstanceStrong().nextBytes(rand);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }

        ByteBuffer idAndPl = ByteBuffer.allocate(16);
        idAndPl.order(ByteOrder.LITTLE_ENDIAN);
        idAndPl.put(self.getAddress().getAddress());
        idAndPl.putShort((short) (self.getPort() - 32768));
        idAndPl.put(rand);
        idAndPl.putLong(System.nanoTime());

        return idAndPl.array();
    }

    UnwrappedMessage generateMessage(UnwrappedPayload pl) throws UnknownHostException {
        byte[] plb = KVRequestSerializer.serialize(pl);

        ByteBuffer idAndpl = ByteBuffer.allocate(16 + plb.length);

        byte[] msgID = generateMsgID();

        idAndpl.put(msgID);
        idAndpl.put(plb);
        idAndpl.flip();

        CRC32 crc32 = new CRC32();
        crc32.update(idAndpl.array());
        long checksum = crc32.getValue();

        UnwrappedMessage msg = new UnwrappedMessage();
        msg.setCheckSum(checksum);
        msg.setPayload(plb);
        msg.setMessageID(msgID);
        return msg;
    }

    static UnwrappedMessage generateMessageStatic(UnwrappedPayload pl) {
        byte[] plb = KVRequestSerializer.serialize(pl);

        ByteBuffer idAndpl = ByteBuffer.allocate(16 + plb.length);

        byte[] msgID = generateMsgIDStatic();

        idAndpl.put(msgID);
        idAndpl.put(plb);
        idAndpl.flip();

        CRC32 crc32 = new CRC32();
        crc32.update(idAndpl.array());
        long checksum = crc32.getValue();

        UnwrappedMessage msg = new UnwrappedMessage();
        msg.setCheckSum(checksum);
        msg.setPayload(plb);
        msg.setMessageID(msgID);
        return msg;
    }

    ServerResponse sendAndReceiveSingleServerResponse(UnwrappedMessage req) throws ServerTimedOutException, IOException {
        DatagramPacket rP = new DatagramPacket(publicBuf, publicBuf.length);

        int tries = 0;
        boolean success = false;

        byte[] msgb = KVMsgSerializer.serialize(req);
        DatagramPacket p = new DatagramPacket(msgb, msgb.length, serverAddress, serverPort);
        int initTimeout = socket.getSoTimeout();

        messageOnWire = req;

        UnwrappedMessage res = null;
        while (tries < triesMax && !success)
        {
            socket.send(p);
            try {
                socket.receive(rP);
            } catch (SocketTimeoutException e) {
                tries++;
                continue;
                //do nothing and let it loop
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }

            //verify message ID and CRC
            byte[] trimmed = Arrays.copyOf(rP.getData(), rP.getLength());
            res = (UnwrappedMessage) KVMsgSerializer.parseFrom(new KVMsgFactory(), trimmed);

            boolean msgIDMatch = res.hasMessageID() && Arrays.equals(res.getMessageID(), req.getMessageID());

            ByteBuffer rIDnPL = ByteBuffer.allocate(res.getMessageID().length + res.getPayload().length);
            rIDnPL.put(res.getMessageID());
            rIDnPL.put(res.getPayload());
            rIDnPL.flip();

            CRC32 crc32 = new CRC32();
            crc32.update(rIDnPL.array());
            boolean crc32Match = crc32.getValue() == res.getCheckSum();

            if(!msgIDMatch) continue;

            success = msgIDMatch && crc32Match;
            tries++;
            socket.setSoTimeout(socket.getSoTimeout() * 2);
        }

        socket.setSoTimeout(initTimeout);

        if(tries == triesMax && !success) {
            throw new ServerTimedOutException();
        }

        ServerResponse plr = (ServerResponse) KVResponseSerializer.parseFrom(new ServerResponseFactory(), res.getPayload());

        return plr;
    }

    /* Exception Handler */
    int handleException(Exception e) {
        if (e instanceof ServerTimedOutException) {
            System.out.println("Server timed out. Aborting");
            return TEST_UNDECIDED;
        } else if (e instanceof  MissingValuesException) {
            System.out.println("Server response did not contain value: " + ((MissingValuesException)e).missingItem);
            return TEST_FAILED;
        } else if (e instanceof InterruptedException) {
            System.out.println("Thread interrupted.");
            return TEST_UNDECIDED;
        } else if (e instanceof  IOException)
        {
            System.out.println("Error in parsing.");
            return TEST_FAILED;
        }
        else {
            e.printStackTrace();
            return TEST_UNDECIDED;
        }

    }

    /* Custom exceptions*/
    public class ServerTimedOutException extends Exception{}
    public class MissingValuesException extends Exception{
        public String missingItem;

        public MissingValuesException(String missingItem) {
            this.missingItem = missingItem;
        }
    }

}
