package com.g7.CPEN431.A7;

import java.util.TimerTask;

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