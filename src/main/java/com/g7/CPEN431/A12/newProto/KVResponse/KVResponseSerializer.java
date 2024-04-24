package com.g7.CPEN431.A12.newProto.KVResponse;

import com.g7.CPEN431.A12.newProto.shared.CurrentCursor;
import com.g7.CPEN431.A12.newProto.shared.MessageFactory;
import com.g7.CPEN431.A12.newProto.shared.ProtobufInputStream;
import com.g7.CPEN431.A12.newProto.shared.ProtobufOutputStream;

public final class KVResponseSerializer {
 public static byte[] serialize(KVResponse message) {
  try {
   assertInitialized(message);
   int totalSize = 0;
   if (message.hasErrCode()) {
    totalSize += ProtobufOutputStream.computeUint32Size(1, message.getErrCode());
   }
   if (message.hasValue()) {
    totalSize += message.getValue().length;
    totalSize += ProtobufOutputStream.computeTagSize(2);
    totalSize += ProtobufOutputStream.computeRawVarint32Size(message.getValue().length);
   }
   if (message.hasPid()) {
    totalSize += ProtobufOutputStream.computeInt32Size(3, message.getPid());
   }
   if (message.hasVersion()) {
    totalSize += ProtobufOutputStream.computeInt32Size(4, message.getVersion());
   }
   if (message.hasOverloadWaitTime()) {
    totalSize += ProtobufOutputStream.computeInt32Size(5, message.getOverloadWaitTime());
   }
   if (message.hasMembershipCount()) {
    totalSize += ProtobufOutputStream.computeInt32Size(6, message.getMembershipCount());
   }
   if (message.hasServerStatusCode()) {
    for(int i=0;i<message.getServerStatusCode().size();i++) {
     totalSize += ProtobufOutputStream.computeUint32Size(100, message.getServerStatusCode().get(i));
    }
   }
   final byte[] result = new byte[totalSize];
   int position = 0;
   if (message.hasErrCode()) {
    position = ProtobufOutputStream.writeUint32(1, message.getErrCode(), result, position);
   }
   if (message.hasValue()) {
    position = ProtobufOutputStream.writeBytes(2, message.getValue(), result, position);
   }
   if (message.hasPid()) {
    position = ProtobufOutputStream.writeInt32(3, message.getPid(), result, position);
   }
   if (message.hasVersion()) {
    position = ProtobufOutputStream.writeInt32(4, message.getVersion(), result, position);
   }
   if (message.hasOverloadWaitTime()) {
    position = ProtobufOutputStream.writeInt32(5, message.getOverloadWaitTime(), result, position);
   }
   if (message.hasMembershipCount()) {
    position = ProtobufOutputStream.writeInt32(6, message.getMembershipCount(), result, position);
   }
   if (message.hasServerStatusCode()) {
    position = ProtobufOutputStream.writeRepeatedUint32(100, message.getServerStatusCode(), result, position);
   }
   ProtobufOutputStream.checkNoSpaceLeft(result, position);
   return result;
  } catch (Exception e) {
   throw new RuntimeException(e);
  }
 }
 public static void serialize(KVResponse message, java.io.OutputStream os) {
  try {
   assertInitialized(message);
   if (message.hasErrCode()) {
    ProtobufOutputStream.writeUint32(1, message.getErrCode(), os);
   }
   if (message.hasValue()) {
    ProtobufOutputStream.writeBytes(2, message.getValue(), os);
   }
   if (message.hasPid()) {
    ProtobufOutputStream.writeInt32(3, message.getPid(), os);
   }
   if (message.hasVersion()) {
    ProtobufOutputStream.writeInt32(4, message.getVersion(), os);
   }
   if (message.hasOverloadWaitTime()) {
    ProtobufOutputStream.writeInt32(5, message.getOverloadWaitTime(), os);
   }
   if (message.hasMembershipCount()) {
    ProtobufOutputStream.writeInt32(6, message.getMembershipCount(), os);
   }
   if (message.hasServerStatusCode()) {
    for( int i=0;i<message.getServerStatusCode().size();i++) {
     ProtobufOutputStream.writeUint32(100, message.getServerStatusCode().get(i), os);
    }
   }
  } catch (java.io.IOException e) {
   throw new RuntimeException("Serializing to a byte array threw an IOException (should never happen).", e);
  }
 }
 public static KVResponse parseFrom(MessageFactory factory, byte[] data) throws java.io.IOException {
  CurrentCursor cursor = new CurrentCursor();
  return parseFrom(factory, data, cursor);
 }
 public static KVResponse parseFrom(MessageFactory factory, byte[] data, int offset, int length) throws java.io.IOException {
  CurrentCursor cursor = new CurrentCursor();
  cursor.addToPosition(offset);
  cursor.setProcessUpToPosition(offset + length);
  return parseFrom(factory, data, cursor);
 }
 public static KVResponse parseFrom(MessageFactory factory, byte[] data, CurrentCursor cursor) throws java.io.IOException {
  KVResponse message = (KVResponse)factory.create("KVResponse");
  if( message == null ) {
   throw new java.io.IOException("Factory create invalid message for type: KVResponse");
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
     message.setErrCode(ProtobufInputStream.readUint32(data,cursor));
     break;
    case 2:
     message.setValue(ProtobufInputStream.readBytes(data,cursor));
     break;
    case 3:
     message.setPid(ProtobufInputStream.readInt32(data,cursor));
     break;
    case 4:
     message.setVersion(ProtobufInputStream.readInt32(data,cursor));
     break;
    case 5:
     message.setOverloadWaitTime(ProtobufInputStream.readInt32(data,cursor));
     break;
    case 6:
     message.setMembershipCount(ProtobufInputStream.readInt32(data,cursor));
     break;
    case 100:
     if( message.getServerStatusCode() == null || message.getServerStatusCode().isEmpty()) {
      message.setServerStatusCode(new java.util.ArrayList<Integer>());
     }
     message.getServerStatusCode().add(ProtobufInputStream.readUint32(data,cursor));
     break;
   }
  }
 }
 /** Beware! All subsequent messages in stream will be consumed until end of stream (default protobuf behaivour).
  **/public static KVResponse parseFrom(MessageFactory factory, java.io.InputStream is) throws java.io.IOException {
  CurrentCursor cursor = new CurrentCursor();
  return parseFrom(factory, is, cursor);
 }
 public static KVResponse parseFrom(MessageFactory factory, java.io.InputStream is, int offset, int length) throws java.io.IOException {
  CurrentCursor cursor = new CurrentCursor();
  cursor.addToPosition(offset);
  cursor.setProcessUpToPosition(offset + length);
  return parseFrom(factory, is, cursor);
 }
 public static KVResponse parseFrom(MessageFactory factory, java.io.InputStream is, CurrentCursor cursor) throws java.io.IOException {
  KVResponse message = (KVResponse)factory.create("KVResponse");
  if( message == null ) {
   throw new java.io.IOException("Factory create invalid message for type: KVResponse");
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
     message.setErrCode(ProtobufInputStream.readUint32(is,cursor));
     break;
    case 2:
     message.setValue(ProtobufInputStream.readBytes(is,cursor));
     break;
    case 3:
     message.setPid(ProtobufInputStream.readInt32(is,cursor));
     break;
    case 4:
     message.setVersion(ProtobufInputStream.readInt32(is,cursor));
     break;
    case 5:
     message.setOverloadWaitTime(ProtobufInputStream.readInt32(is,cursor));
     break;
    case 6:
     message.setMembershipCount(ProtobufInputStream.readInt32(is,cursor));
     break;
    case 100:
     if( message.getServerStatusCode() == null || message.getServerStatusCode().isEmpty()) {
      message.setServerStatusCode(new java.util.ArrayList<Integer>());
     }
     message.getServerStatusCode().add(ProtobufInputStream.readUint32(is,cursor));
     break;
   }
  }
 }
 private static void assertInitialized(KVResponse message) {
  if( !message.hasErrCode()) {
   throw new IllegalArgumentException("Required field not initialized: errCode");
  }
 }
}
