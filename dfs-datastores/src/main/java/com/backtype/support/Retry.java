package com.backtype.support;

public class Retry {

    private int times;

    public Retry(int times) {
        this.times = times;
    }

    public boolean shouldRetry() {
        if(times > 0)
            return times-- > 0;
        else
            return false;
    }
}