package com.mars.fuzznnrl.data;

/**
 * Maintains the splitting percentages of a data set
 */
public class SplitInfo {
    private final float train;
    private final float dev;
    private final float test;

    public SplitInfo(float train, float dev, float test) {
        this.train = train;
        this.dev = dev;
        this.test = test;
    }

    public float getTrain() {
        return train;
    }

    public float getDev() {
        return dev;
    }

    public float getTest() {
        return test;
    }
}
