package com.mars.fuzznnrl.data;

/**
 * Determines a strategy for splitting a data set into train, dev, test sets.
 */
public interface ISplittingStrategy {
    /**
     * If the data set size/num lines is less or equal (leqXXX) XXX
     *
     * @return
     */
    default SplitInfo leqThousand() {
        return new SplitInfo(.7f, .0f, .3f);
    }

    /**
     * If the data set size/num lines is less or equal (leqXXX) XXX
     *
     * @return
     */
    default SplitInfo leqTenThousand() {
        return new SplitInfo(.6f, .2f, .2f);
    }

    /**
     * If the data set size/num lines is greater or equal to a million
     *
     * @return
     */
    default SplitInfo grtEqMillion() {
        return new SplitInfo(.98f, .1f, .1f);
    }
}
