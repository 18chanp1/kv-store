package com.s82033788.CPEN431.A4.wrappers;

public class UnwrappedPayload {
    private final int command;
    private final byte[] key;
    private final byte[] value;
    private final int version; //defaults to zero anyways, no logic

    private final boolean keyExists;
    private final boolean valueExists;
    private final boolean versionExists;

    public UnwrappedPayload(int command, byte[] key, byte[] value, int version, boolean keyExists, boolean valueExists, boolean versionExists) {
        this.command = command;
        this.key = key;
        this.value = value;
        this.version = version;
        this.keyExists = keyExists;
        this.valueExists = valueExists;
        this.versionExists = versionExists;
    }

    public int getCommand() {
        return command;
    }

    public byte[] getKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }

    public int getVersion() {
        return version;
    }

    public boolean isKeyExists() {
        return keyExists;
    }

    public boolean isValueExists() {
        return valueExists;
    }

    public boolean isVersionExists() {
        return versionExists;
    }
}
