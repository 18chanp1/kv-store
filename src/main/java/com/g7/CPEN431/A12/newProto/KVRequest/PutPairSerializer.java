package com.g7.CPEN431.A12.newProto.KVRequest;

import com.g7.CPEN431.A12.newProto.shared.*;
public final class PutPairSerializer {
 public static byte[] serialize(PutPair message) {
  try {
   assertInitialized(message);
   int totalSize = 0;
   if (message.hasKey()) {
    totalSize += message.getKey().length;
    totalSize += ProtobufOutputStream.computeTagSize(300);
    totalSize += ProtobufOutputStream.computeRawVarint32Size(message.getKey().length);
   }
   if (message.hasValue()) {
    totalSize += message.getValue().length;
    totalSize += ProtobufOutputStream.computeTagSize(301);
    totalSize += ProtobufOutputStream.computeRawVarint32Size(message.getValue().length);
   }
   if (message.hasVersion()) {
    totalSize += ProtobufOutputStream.computeInt32Size(302, message.getVersion());
   }
   if (message.hasInsertionTime()) {
    totalSize += ProtobufOutputStream.computeUint64Size(303, message.getInsertionTime());
   }
   final byte[] result = new byte[totalSize];
   int position = 0;
   if (message.hasKey()) {
    position = ProtobufOutputStream.writeBytes(300, message.getKey(), result, position);
   }
   if (message.hasValue()) {
    position = ProtobufOutputStream.writeBytes(301, message.getValue(), result, position);
   }
   if (message.hasVersion()) {
    position = ProtobufOutputStream.writeInt32(302, message.getVersion(), result, position);
   }
   if (message.hasInsertionTime()) {
    position = ProtobufOutputStream.writeUint64(303, message.getInsertionTime(), result, position);
   }
   ProtobufOutputStream.checkNoSpaceLeft(result, position);
   return result;
  } catch (Exception e) {
   throw new RuntimeException(e);
  }
 }
 public static void serialize(PutPair message, java.io.OutputStream os) {
  try {
   assertInitialized(message);
   if (message.hasKey()) {
    ProtobufOutputStream.writeBytes(300, message.getKey(), os);
   }
   if (message.hasValue()) {
    ProtobufOutputStream.writeBytes(301, message.getValue(), os);
   }
   if (message.hasVersion()) {
    ProtobufOutputStream.writeInt32(302, message.getVersion(), os);
   }
   if (message.hasInsertionTime()) {
    ProtobufOutputStream.writeUint64(303, message.getInsertionTime(), os);
   }
  } catch (java.io.IOException e) {
   throw new RuntimeException("Serializing to a byte array threw an IOException (should never happen).", e);
  }
 }
 public static PutPair parseFrom(MessageFactory factory, byte[] data) throws java.io.IOException {
  CurrentCursor cursor = new CurrentCursor();
  return parseFrom(factory, data, cursor);
 }
 public static PutPair parseFrom(MessageFactory factory, byte[] data, int offset, int length) throws java.io.IOException {
  CurrentCursor cursor = new CurrentCursor();
  cursor.addToPosition(offset);
  cursor.setProcessUpToPosition(offset + length);
  return parseFrom(factory, data, cursor);
 }
 public static PutPair parseFrom(MessageFactory factory, byte[] data, CurrentCursor cursor) throws java.io.IOException {
  PutPair message = (PutPair)factory.create("PutPair");
  if( message == null ) {
   throw new java.io.IOException("Factory create invalid message for type: PutPair");
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
    case 300:
     message.setKey(ProtobufInputStream.readBytes(data,cursor));
     break;
    case 301:
     message.setValue(ProtobufInputStream.readBytes(data,cursor));
     break;
    case 302:
     message.setVersion(ProtobufInputStream.readInt32(data,cursor));
     break;
    case 303:
     message.setInsertionTime(ProtobufInputStream.readUint64(data,cursor));
     break;
   }
  }
 }
 /** Beware! All subsequent messages in stream will be consumed until end of stream (default protobuf behaivour).
  **/public static PutPair parseFrom(MessageFactory factory, java.io.InputStream is) throws java.io.IOException {
  CurrentCursor cursor = new CurrentCursor();
  return parseFrom(factory, is, cursor);
 }
 public static PutPair parseFrom(MessageFactory factory, java.io.InputStream is, int offset, int length) throws java.io.IOException {
  CurrentCursor cursor = new CurrentCursor();
  cursor.addToPosition(offset);
  cursor.setProcessUpToPosition(offset + length);
  return parseFrom(factory, is, cursor);
 }
 public static PutPair parseFrom(MessageFactory factory, java.io.InputStream is, CurrentCursor cursor) throws java.io.IOException {
  PutPair message = (PutPair)factory.create("PutPair");
  if( message == null ) {
   throw new java.io.IOException("Factory create invalid message for type: PutPair");
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
     break;case 300:
     message.setKey(ProtobufInputStream.readBytes(is,cursor));
     break;
    case 301:
     message.setValue(ProtobufInputStream.readBytes(is,cursor));
     break;
    case 302:
     message.setVersion(ProtobufInputStream.readInt32(is,cursor));
     break;
    case 303:
     message.setInsertionTime(ProtobufInputStream.readUint64(is,cursor));
     break;
   }
  }
 }
 private static void assertInitialized(PutPair message) {
  if( !message.hasKey()) {
   throw new IllegalArgumentException("Required field not initialized: key");
  }
  if( !message.hasVersion()) {
   throw new IllegalArgumentException("Required field not initialized: version");
  }
  if( !message.hasInsertionTime()) {
   throw new IllegalArgumentException("Required field not initialized: insertionTime");
  }
 }
}
