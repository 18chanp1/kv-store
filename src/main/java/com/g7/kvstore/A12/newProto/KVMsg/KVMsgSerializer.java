package com.g7.CPEN431.A12.newProto.KVMsg;

import com.g7.CPEN431.A12.newProto.shared.CurrentCursor;
import com.g7.CPEN431.A12.newProto.shared.MessageFactory;
import com.g7.CPEN431.A12.newProto.shared.ProtobufInputStream;
import com.g7.CPEN431.A12.newProto.shared.ProtobufOutputStream;

public final class KVMsgSerializer {
public static byte[] serialize(KVMsg message) {
try {
assertInitialized(message);
int totalSize = 0;
if (message.hasMessageID()) {
totalSize += message.getMessageID().length;
totalSize += ProtobufOutputStream.computeTagSize(1);
totalSize += ProtobufOutputStream.computeRawVarint32Size(message.getMessageID().length);
}
if (message.hasPayload()) {
totalSize += message.getPayload().length;
totalSize += ProtobufOutputStream.computeTagSize(2);
totalSize += ProtobufOutputStream.computeRawVarint32Size(message.getPayload().length);
}
if (message.hasCheckSum()) {
totalSize += ProtobufOutputStream.computeFixed64Size(3, message.getCheckSum());
}
if (message.hasSourceAddress()) {
totalSize += message.getSourceAddress().length;
totalSize += ProtobufOutputStream.computeTagSize(100);
totalSize += ProtobufOutputStream.computeRawVarint32Size(message.getSourceAddress().length);
}
if (message.hasSourcePort()) {
totalSize += ProtobufOutputStream.computeUint32Size(101, message.getSourcePort());
}
final byte[] result = new byte[totalSize];
int position = 0;
if (message.hasMessageID()) {
position = ProtobufOutputStream.writeBytes(1, message.getMessageID(), result, position);
}
if (message.hasPayload()) {
position = ProtobufOutputStream.writeBytes(2, message.getPayload(), result, position);
}
if (message.hasCheckSum()) {
position = ProtobufOutputStream.writeFixed64(3, message.getCheckSum(), result, position);
}
if (message.hasSourceAddress()) {
position = ProtobufOutputStream.writeBytes(100, message.getSourceAddress(), result, position);
}
if (message.hasSourcePort()) {
position = ProtobufOutputStream.writeUint32(101, message.getSourcePort(), result, position);
}
ProtobufOutputStream.checkNoSpaceLeft(result, position);
return result;
} catch (Exception e) {
throw new RuntimeException(e);
}
}
public static void serialize(KVMsg message, java.io.OutputStream os) {
try {
assertInitialized(message);
if (message.hasMessageID()) {
ProtobufOutputStream.writeBytes(1, message.getMessageID(), os);
}
if (message.hasPayload()) {
ProtobufOutputStream.writeBytes(2, message.getPayload(), os);
}
if (message.hasCheckSum()) {
ProtobufOutputStream.writeFixed64(3, message.getCheckSum(), os);
}
if (message.hasSourceAddress()) {
ProtobufOutputStream.writeBytes(100, message.getSourceAddress(), os);
}
if (message.hasSourcePort()) {
ProtobufOutputStream.writeUint32(101, message.getSourcePort(), os);
}
} catch (java.io.IOException e) {
throw new RuntimeException("Serializing to a byte array threw an IOException (should never happen).", e);
}
}
public static KVMsg parseFrom(MessageFactory factory, byte[] data) throws java.io.IOException {
CurrentCursor cursor = new CurrentCursor();
return parseFrom(factory, data, cursor);
}
public static KVMsg parseFrom(MessageFactory factory, byte[] data, int offset, int length) throws java.io.IOException {
CurrentCursor cursor = new CurrentCursor();
cursor.addToPosition(offset);
cursor.setProcessUpToPosition(offset + length);
return parseFrom(factory, data, cursor);
}
public static KVMsg parseFrom(MessageFactory factory, byte[] data, CurrentCursor cursor) throws java.io.IOException {
KVMsg message = (KVMsg)factory.create("KVMsg");
if( message == null ) { 
throw new java.io.IOException("Factory create invalid message for type: KVMsg");
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
message.setMessageID(ProtobufInputStream.readBytes(data,cursor));
break;
case 2: 
message.setPayload(ProtobufInputStream.readBytes(data,cursor));
break;
case 3: 
message.setCheckSum(ProtobufInputStream.readFixed64(data,cursor));
break;
case 100: 
message.setSourceAddress(ProtobufInputStream.readBytes(data,cursor));
break;
case 101: 
message.setSourcePort(ProtobufInputStream.readUint32(data,cursor));
break;
}
}
}
/** Beware! All subsequent messages in stream will be consumed until end of stream (default protobuf behaivour).
  **/public static KVMsg parseFrom(MessageFactory factory, java.io.InputStream is) throws java.io.IOException {
CurrentCursor cursor = new CurrentCursor();
return parseFrom(factory, is, cursor);
}
public static KVMsg parseFrom(MessageFactory factory, java.io.InputStream is, int offset, int length) throws java.io.IOException {
CurrentCursor cursor = new CurrentCursor();
cursor.addToPosition(offset);
cursor.setProcessUpToPosition(offset + length);
return parseFrom(factory, is, cursor);
}
public static KVMsg parseFrom(MessageFactory factory, java.io.InputStream is, CurrentCursor cursor) throws java.io.IOException {
KVMsg message = (KVMsg)factory.create("KVMsg");
if( message == null ) { 
throw new java.io.IOException("Factory create invalid message for type: KVMsg");
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
message.setMessageID(ProtobufInputStream.readBytes(is,cursor));
break;
case 2: 
message.setPayload(ProtobufInputStream.readBytes(is,cursor));
break;
case 3: 
message.setCheckSum(ProtobufInputStream.readFixed64(is,cursor));
break;
case 100: 
message.setSourceAddress(ProtobufInputStream.readBytes(is,cursor));
break;
case 101: 
message.setSourcePort(ProtobufInputStream.readUint32(is,cursor));
break;
}
}
}
private static void assertInitialized(KVMsg message) {
if( !message.hasMessageID()) {
throw new IllegalArgumentException("Required field not initialized: messageID");
}
if( !message.hasPayload()) {
throw new IllegalArgumentException("Required field not initialized: payload");
}
if( !message.hasCheckSum()) {
throw new IllegalArgumentException("Required field not initialized: checkSum");
}
}
}
