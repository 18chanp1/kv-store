package com.g7.CPEN431.A12.newProto.KVRequest;

import com.g7.CPEN431.A12.newProto.shared.CurrentCursor;
import com.g7.CPEN431.A12.newProto.shared.MessageFactory;
import com.g7.CPEN431.A12.newProto.shared.ProtobufInputStream;
import com.g7.CPEN431.A12.newProto.shared.ProtobufOutputStream;

public final class ServerEntrySerializer {
public static byte[] serialize(ServerEntry message) {
try {
assertInitialized(message);
int totalSize = 0;
if (message.hasServerAddress()) {
totalSize += message.getServerAddress().length;
totalSize += ProtobufOutputStream.computeTagSize(200);
totalSize += ProtobufOutputStream.computeRawVarint32Size(message.getServerAddress().length);
}
if (message.hasServerPort()) {
totalSize += ProtobufOutputStream.computeUint32Size(201, message.getServerPort());
}
if (message.hasInformationTime()) {
totalSize += ProtobufOutputStream.computeUint64Size(202, message.getInformationTime());
}
if (message.hasCode()) {
totalSize += ProtobufOutputStream.computeUint32Size(203, message.getCode());
}
final byte[] result = new byte[totalSize];
int position = 0;
if (message.hasServerAddress()) {
position = ProtobufOutputStream.writeBytes(200, message.getServerAddress(), result, position);
}
if (message.hasServerPort()) {
position = ProtobufOutputStream.writeUint32(201, message.getServerPort(), result, position);
}
if (message.hasInformationTime()) {
position = ProtobufOutputStream.writeUint64(202, message.getInformationTime(), result, position);
}
if (message.hasCode()) {
position = ProtobufOutputStream.writeUint32(203, message.getCode(), result, position);
}
ProtobufOutputStream.checkNoSpaceLeft(result, position);
return result;
} catch (Exception e) {
throw new RuntimeException(e);
}
}
public static void serialize(ServerEntry message, java.io.OutputStream os) {
try {
assertInitialized(message);
if (message.hasServerAddress()) {
ProtobufOutputStream.writeBytes(200, message.getServerAddress(), os);
}
if (message.hasServerPort()) {
ProtobufOutputStream.writeUint32(201, message.getServerPort(), os);
}
if (message.hasInformationTime()) {
ProtobufOutputStream.writeUint64(202, message.getInformationTime(), os);
}
if (message.hasCode()) {
ProtobufOutputStream.writeUint32(203, message.getCode(), os);
}
} catch (java.io.IOException e) {
throw new RuntimeException("Serializing to a byte array threw an IOException (should never happen).", e);
}
}
public static ServerEntry parseFrom(MessageFactory factory, byte[] data) throws java.io.IOException {
CurrentCursor cursor = new CurrentCursor();
return parseFrom(factory, data, cursor);
}
public static ServerEntry parseFrom(MessageFactory factory, byte[] data, int offset, int length) throws java.io.IOException {
CurrentCursor cursor = new CurrentCursor();
cursor.addToPosition(offset);
cursor.setProcessUpToPosition(offset + length);
return parseFrom(factory, data, cursor);
}
public static ServerEntry parseFrom(MessageFactory factory, byte[] data, CurrentCursor cursor) throws java.io.IOException {
ServerEntry message = (ServerEntry)factory.create("ServerEntry");
if( message == null ) { 
throw new java.io.IOException("Factory create invalid message for type: ServerEntry");
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
case 200: 
message.setServerAddress(ProtobufInputStream.readBytes(data,cursor));
break;
case 201: 
message.setServerPort(ProtobufInputStream.readUint32(data,cursor));
break;
case 202: 
message.setInformationTime(ProtobufInputStream.readUint64(data,cursor));
break;
case 203: 
message.setCode(ProtobufInputStream.readUint32(data,cursor));
break;
}
}
}
/** Beware! All subsequent messages in stream will be consumed until end of stream (default protobuf behaivour).
  **/public static ServerEntry parseFrom(MessageFactory factory, java.io.InputStream is) throws java.io.IOException {
CurrentCursor cursor = new CurrentCursor();
return parseFrom(factory, is, cursor);
}
public static ServerEntry parseFrom(MessageFactory factory, java.io.InputStream is, int offset, int length) throws java.io.IOException {
CurrentCursor cursor = new CurrentCursor();
cursor.addToPosition(offset);
cursor.setProcessUpToPosition(offset + length);
return parseFrom(factory, is, cursor);
}
public static ServerEntry parseFrom(MessageFactory factory, java.io.InputStream is, CurrentCursor cursor) throws java.io.IOException {
ServerEntry message = (ServerEntry)factory.create("ServerEntry");
if( message == null ) { 
throw new java.io.IOException("Factory create invalid message for type: ServerEntry");
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
 break;case 200: 
message.setServerAddress(ProtobufInputStream.readBytes(is,cursor));
break;
case 201: 
message.setServerPort(ProtobufInputStream.readUint32(is,cursor));
break;
case 202: 
message.setInformationTime(ProtobufInputStream.readUint64(is,cursor));
break;
case 203: 
message.setCode(ProtobufInputStream.readUint32(is,cursor));
break;
}
}
}
private static void assertInitialized(ServerEntry message) {
if( !message.hasServerAddress()) {
throw new IllegalArgumentException("Required field not initialized: serverAddress");
}
if( !message.hasServerPort()) {
throw new IllegalArgumentException("Required field not initialized: serverPort");
}
if( !message.hasInformationTime()) {
throw new IllegalArgumentException("Required field not initialized: informationTime");
}
if( !message.hasCode()) {
throw new IllegalArgumentException("Required field not initialized: code");
}
}
}
