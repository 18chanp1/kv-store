package com.g7.kvstore.A12;

public class ScheduledRunner {
    KeyTransferHandler kth;
    DeathRegistrar dr;


    public ScheduledRunner(KeyTransferHandler kth) {
        this.kth = kth;
    }

    public ScheduledRunner(DeathRegistrar dr) {
        this.dr = dr;
    }




}
