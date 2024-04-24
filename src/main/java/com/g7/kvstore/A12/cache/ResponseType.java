package com.g7.kvstore.A12.cache;

public enum ResponseType {
    INVALID_KEY,
    INVALID_VALUE,
    INVALID_OPCODE,
    MEMBERSHIP_COUNT,
    PID,
    ISALIVE,
    SHUTDOWN,
    INVALID_OPTIONAL,
    RETRY_NOT_EQUAL,
    VALUE,
    PUT,
    DEL,
    WIPEOUT,
    NO_KEY,
    NO_MEM,
    OVERLOAD_CACHE,
    OVERLOAD_THREAD,
    OBITUARIES
}
