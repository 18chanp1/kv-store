package com.g7.CPEN431.A12.newProto.KVRequest;

import com.g7.CPEN431.A12.newProto.shared.CurrentCursor;
import com.g7.CPEN431.A12.newProto.shared.MessageFactory;
import com.g7.CPEN431.A12.newProto.shared.ProtobufInputStream;
import com.g7.CPEN431.A12.newProto.shared.ProtobufOutputStream;
public final class KVRequestSerializer {
 public static byte[] serialize(KVRequest message) {
  try {
   assertInitialized(message);
   int totalSize = 0;
   if (message.hasCommand()) {
    totalSize += ProtobufOutputStream.computeUint32Size(1, message.getCommand());
   }
   if (message.hasKey()) {
    totalSize += message.getKey().length;
    totalSize += ProtobufOutputStream.computeTagSize(2);
    totalSize += ProtobufOutputStream.computeRawVarint32Size(message.getKey().length);
   }
   if (message.hasValue()) {
    totalSize += message.getValue().length;
    totalSize += ProtobufOutputStream.computeTagSize(3);
    totalSize += ProtobufOutputStream.computeRawVarint32Size(message.getValue().length);
   }
   if (message.hasVersion()) {
    totalSize += ProtobufOutputStream.computeInt32Size(4, message.getVersion());
   }
   byte[] serverRecordBuffer = null;
   if (message.hasServerRecord()) {
    java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
    for( int i=0;i<message.getServerRecord().size();i++) {
     byte[] curMessageData = ServerEntrySerializer.serialize(message.getServerRecord().get(i));
     ProtobufOutputStream.writeMessageTag(100, baos);
     ProtobufOutputStream.writeRawVarint32(curMessageData.length, baos);
     baos.write(curMessageData);
    }
    serverRecordBuffer = baos.toByteArray();
    totalSize += serverRecordBuffer.length;
   }
   byte[] putPairBuffer = null;
   if (message.hasPutPair()) {
    java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
    for( int i=0;i<message.getPutPair().size();i++) {
     byte[] curMessageData = PutPairSerializer.serialize(message.getPutPair().get(i));
     ProtobufOutputStream.writeMessageTag(130, baos);
     ProtobufOutputStream.writeRawVarint32(curMessageData.length, baos);
     baos.write(curMessageData);
    }
    putPairBuffer = baos.toByteArray();
    totalSize += putPairBuffer.length;
   }
   final byte[] result = new byte[totalSize];
   int position = 0;
   if (message.hasCommand()) {
    position = ProtobufOutputStream.writeUint32(1, message.getCommand(), result, position);
   }
   if (message.hasKey()) {
    position = ProtobufOutputStream.writeBytes(2, message.getKey(), result, position);
   }
   if (message.hasValue()) {
    position = ProtobufOutputStream.writeBytes(3, message.getValue(), result, position);
   }
   if (message.hasVersion()) {
    position = ProtobufOutputStream.writeInt32(4, message.getVersion(), result, position);
   }
   if (message.hasServerRecord()) {
    position = ProtobufOutputStream.writeRawBytes(serverRecordBuffer, result, position);
   }
   if (message.hasPutPair()) {
    position = ProtobufOutputStream.writeRawBytes(putPairBuffer, result, position);
   }
   ProtobufOutputStream.checkNoSpaceLeft(result, position);
   return result;
  } catch (Exception e) {
   throw new RuntimeException(e);
  }
 }
 public static void serialize(KVRequest message, java.io.OutputStream os) {
  try {
   assertInitialized(message);
   if (message.hasCommand()) {
    ProtobufOutputStream.writeUint32(1, message.getCommand(), os);
   }
   if (message.hasKey()) {
    ProtobufOutputStream.writeBytes(2, message.getKey(), os);
   }
   if (message.hasValue()) {
    ProtobufOutputStream.writeBytes(3, message.getValue(), os);
   }
   if (message.hasVersion()) {
    ProtobufOutputStream.writeInt32(4, message.getVersion(), os);
   }
   if (message.hasServerRecord()) {
    for( int i=0;i<message.getServerRecord().size();i++) {
     byte[] curMessageData = ServerEntrySerializer.serialize(message.getServerRecord().get(i));
     ProtobufOutputStream.writeMessageTag(100, os);
     ProtobufOutputStream.writeRawVarint32(curMessageData.length, os);
     os.write(curMessageData);
    }
   }
   if (message.hasPutPair()) {
    for( int i=0;i<message.getPutPair().size();i++) {
     byte[] curMessageData = PutPairSerializer.serialize(message.getPutPair().get(i));
     ProtobufOutputStream.writeMessageTag(130, os);
     ProtobufOutputStream.writeRawVarint32(curMessageData.length, os);
     os.write(curMessageData);
    }
   }
  } catch (java.io.IOException e) {
   throw new RuntimeException("Serializing to a byte array threw an IOException (should never happen).", e);
  }
 }
 public static KVRequest parseFrom(MessageFactory factory, byte[] data) throws java.io.IOException {
  CurrentCursor cursor = new CurrentCursor();
  return parseFrom(factory, data, cursor);
 }
 public static KVRequest parseFrom(MessageFactory factory, byte[] data, int offset, int length) throws java.io.IOException {
  CurrentCursor cursor = new CurrentCursor();
  cursor.addToPosition(offset);
  cursor.setProcessUpToPosition(offset + length);
  return parseFrom(factory, data, cursor);
 }
 public static KVRequest parseFrom(MessageFactory factory, byte[] data, CurrentCursor cursor) throws java.io.IOException {
  KVRequest message = (KVRequest)factory.create("KVRequest");
  if( message == null ) {
   throw new java.io.IOException("Factory create invalid message for type: KVRequest");
  }
  while(true) {
   if (ProtobufInputStream.isAtEnd(data, cursor)) {
    return message;
   }
   int varint = ProtobufInputStream.readRawVarint32(data, cursor);
   int tag = ProtobufInputStream.getTagFieldNumber(varint);
   switch(tag) {
    case 0:
     return message;
    default:
     ProtobufInputStream.skipUnknown(varint, data, cursor);
     break;
    case 1:
     message.setCommand(ProtobufInputStream.readUint32(data,cursor));
     break;
    case 2:
     message.setKey(ProtobufInputStream.readBytes(data,cursor));
     break;
    case 3:
     message.setValue(ProtobufInputStream.readBytes(data,cursor));
     break;
    case 4:
     message.setVersion(ProtobufInputStream.readInt32(data,cursor));
     break;
    case 100:
     if( message.getServerRecord() == null || message.getServerRecord().isEmpty()) {
      message.setServerRecord(new java.util.ArrayList<ServerEntry>());
     }
     int lengthServerRecord = ProtobufInputStream.readRawVarint32(data,cursor);
     message.getServerRecord().add(ServerEntrySerializer.parseFrom(factory, data, cursor.getCurrentPosition(), lengthServerRecord));
     cursor.addToPosition(lengthServerRecord);
     break;
    case 130:
     if( message.getPutPair() == null || message.getPutPair().isEmpty()) {
      message.setPutPair(new java.util.ArrayList<PutPair>());
     }
     int lengthPutPair = ProtobufInputStream.readRawVarint32(data,cursor);
     message.getPutPair().add(PutPairSerializer.parseFrom(factory, data, cursor.getCurrentPosition(), lengthPutPair));
     cursor.addToPosition(lengthPutPair);
     break;
   }
  }
 }
 /** Beware! All subsequent messages in stream will be consumed until end of stream (default protobuf behaivour).
  **/public static KVRequest parseFrom(MessageFactory factory, java.io.InputStream is) throws java.io.IOException {
  CurrentCursor cursor = new CurrentCursor();
  return parseFrom(factory, is, cursor);
 }
 public static KVRequest parseFrom(MessageFactory factory, java.io.InputStream is, int offset, int length) throws java.io.IOException {
  CurrentCursor cursor = new CurrentCursor();
  cursor.addToPosition(offset);
  cursor.setProcessUpToPosition(offset + length);
  return parseFrom(factory, is, cursor);
 }
 public static KVRequest parseFrom(MessageFactory factory, java.io.InputStream is, CurrentCursor cursor) throws java.io.IOException {
  KVRequest message = (KVRequest)factory.create("KVRequest");
  if( message == null ) {
   throw new java.io.IOException("Factory create invalid message for type: KVRequest");
  }
  while(true) {
   if( cursor.getCurrentPosition() == cursor.getProcessUpToPosition() ) {
    return message;
   }
   int varint = ProtobufInputStream.readRawVarint32(is, cursor);
   int tag = ProtobufInputStream.getTagFieldNumber(varint);
   if (ProtobufInputStream.isAtEnd(cursor)) {
    return message;
   }
   switch(tag) {
    case 0:
     return message;
    default:
     ProtobufInputStream.skipUnknown(varint, is, cursor);
     break;case 1:
     message.setCommand(ProtobufInputStream.readUint32(is,cursor));
     break;
    case 2:
     message.setKey(ProtobufInputStream.readBytes(is,cursor));
     break;
    case 3:
     message.setValue(ProtobufInputStream.readBytes(is,cursor));
     break;
    case 4:
     message.setVersion(ProtobufInputStream.readInt32(is,cursor));
     break;
    case 100:
     if( message.getServerRecord() == null || message.getServerRecord().isEmpty()) {
      message.setServerRecord(new java.util.ArrayList<ServerEntry>());
     }
     int lengthServerRecord = ProtobufInputStream.readRawVarint32(is,cursor);
     message.getServerRecord().add(ServerEntrySerializer.parseFrom(factory, is, cursor.getCurrentPosition(), lengthServerRecord));
     cursor.addToPosition(lengthServerRecord);
     break;
    case 130:
     if( message.getPutPair() == null || message.getPutPair().isEmpty()) {
      message.setPutPair(new java.util.ArrayList<PutPair>());
     }
     int lengthPutPair = ProtobufInputStream.readRawVarint32(is,cursor);
     message.getPutPair().add(PutPairSerializer.parseFrom(factory, is, cursor.getCurrentPosition(), lengthPutPair));
     cursor.addToPosition(lengthPutPair);
     break;
   }
  }
 }
 private static void assertInitialized(KVRequest message) {
  if( !message.hasCommand()) {
   throw new IllegalArgumentException("Required field not initialized: command");
  }
 }
}
