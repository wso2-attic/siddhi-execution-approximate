/*
* Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
* WSO2 Inc. licenses this file to you under the Apache License,
* Version 2.0 (the "License"); you may not use this file except
* in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied. See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.wso2.extension.siddhi.execution.approximate.distinctcount;

import java.io.Serializable;

/**
 * A probabilistic data structure to calculate the distinctCount of a set.
 * The referred research paper - HyperLogLog: the analysis of a near-optimal distinctCount estimation algorithm
 * by Philippe Flajolet, Éric Fusy, Olivier Gandouet and Frédéric Meunier.
 * http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf
 *
 * @param <E> is the type of objects in the set.
 */
public class HyperLogLog<E> implements Serializable {
    private static final long serialVersionUID = -5295608198644134019L;

    private static final double STANDARD_ERROR = 1.04;
    private static final double POW_2_OF_32 = Math.pow(2, 32);

    private boolean pastCountsEnabled;

    private int noOfBuckets;
    private int lengthOfBucketId;
    private int noOfZeroBuckets;

    private double estimationFactor;
    private double relativeError;
    private double confidence;
    private double harmonicCountSum;

    private long currentCardinality;

    private int[] countArray;
    private CountList[] pastCountsArray = null;

    /**
     * Create a new HyperLogLog by specifying the relative error and confidence of answers
     * being within the error margin.
     * Based on the relative error the array size is calculated.
     *
     * @param relativeError     is a number in the range (0, 1)
     * @param confidence        is a value out of 0.65, 0.95, 0.99
     * @param pastCountsEnabled is a boolean value to mention whether to keep track of past counts or not.
     */
    public HyperLogLog(double relativeError, double confidence, boolean pastCountsEnabled) {
        this.relativeError = relativeError;
        this.confidence = confidence;
        this.pastCountsEnabled = pastCountsEnabled;

//      relativeError = STANDARD_ERROR / sqrt(noOfBuckets) = > noOfBuckets = (STANDARD_ERROR / relativeError) ^ 2
        noOfBuckets = (int) Math.ceil(Math.pow(STANDARD_ERROR / relativeError, 2));

//      noOfBuckets = 2 ^ lengthOfBucketId = >  lengthOfBucketId = log2(noOfBuckets) = ln(noOfBuckets) / ln(2)
        lengthOfBucketId = (int) Math.ceil(Math.log(noOfBuckets) / Math.log(2));

        noOfBuckets = (1 << lengthOfBucketId);

//      HyperLogLog estimations valid only when at least 16 buckets are used.
//      Therefore the minimum length of bucket id = 4
        if (lengthOfBucketId < 4) {
            throw new IllegalArgumentException("a higher relative error of " + relativeError +
                    " cannot be achieved");
        }

        countArray = new int[noOfBuckets];
        if (pastCountsEnabled) {
            pastCountsArray = new CountList[noOfBuckets];
            for (int i = 0; i < noOfBuckets; i++) {
                pastCountsArray[i] = new CountList();
            }
        }
        estimationFactor = getEstimationFactor(lengthOfBucketId, noOfBuckets);

        harmonicCountSum = noOfBuckets;
        noOfZeroBuckets = noOfBuckets;

    }

    /**
     * Calculate the distinctCount(number of unique items in a set)
     * by calculating the harmonic mean of the counts in the buckets.
     * Check for the upper and lower bounds to modify the estimation.
     * n - number of buckets
     * ci - count of the i th bucket
     * harmonic count mean = n / ((1/2)^c1 + (1/2)^c2 + ... + (1/2)^cn)
     * estimated distinctCount = n * estimationFactor * harmonicCountMean
     */
    private void calculateCardinality() {
        double harmonicCountMean;
        long estimatedCardinality;
        long cardinality;

        harmonicCountMean = noOfBuckets / harmonicCountSum;

//      calculate the estimated distinctCount
        estimatedCardinality = (long) Math.ceil(noOfBuckets * estimationFactor * harmonicCountMean);

//      if the estimate E is less than 2.5 * 32 and there are buckets with max-leading-zero count of zero,
//      then instead return −32⋅log(V/32), where V is the number of buckets with max-leading-zero count = 0.
//      threshold of 2.5x comes from the recommended load factor
        if ((estimatedCardinality < 2.5 * noOfBuckets) && noOfZeroBuckets > 0) {
            cardinality = (long) (-noOfBuckets * Math.log((double) noOfZeroBuckets / noOfBuckets));
        } else if (estimatedCardinality > (POW_2_OF_32 / 30.0)) {
//      if E > 2 ^ (32) / 30 : return −2 ^ (32) * log(1 − E / 2 ^ (32))
            cardinality = (long) Math.ceil(-(POW_2_OF_32 * Math.log(1 - (estimatedCardinality / (POW_2_OF_32)))));
        } else {
            cardinality = estimatedCardinality;
        }
        this.currentCardinality = cardinality;
    }

    /**
     * @return the current distinctCount value
     */
    public long getCardinality() {
        return this.currentCardinality;
    }

    /**
     * Calculate the confidence interval for the current distinctCount.
     * The confidence values can be one value out of 0.65, 0.95, 0.99.
     *
     * @return an long array which contain the lower bound and the upper bound of the confidence interval
     * e.g. - {310, 350} for the distinctCount of 330
     */
    public long[] getConfidenceInterval() {
        long[] confidenceInterval = new long[2];

//      sigma = relative error
        if (Math.abs(confidence - 0.65) < 0.0000001) { //      65% sure the answer in the range of sigma
            confidenceInterval[0] = (long) Math.floor(currentCardinality - (currentCardinality * relativeError * 0.5));
            confidenceInterval[1] = (long) Math.ceil(currentCardinality + (currentCardinality * relativeError * 0.5));
        } else if (Math.abs(confidence - 0.95) < 0.0000001) { //      95% sure the answer in the range of (2 * sigma)
            confidenceInterval[0] = (long) Math.floor(currentCardinality - (currentCardinality * relativeError));
            confidenceInterval[1] = (long) Math.ceil(currentCardinality + (currentCardinality * relativeError));
        } else if (Math.abs(confidence - 0.99) < 0.0000001) { //      99% sure the answer in the range of (3 * sigma)
            confidenceInterval[0] = (long) Math.floor(currentCardinality - (currentCardinality * relativeError * 1.5));
            confidenceInterval[1] = (long) Math.ceil(currentCardinality + (currentCardinality * relativeError * 1.5));
        }
        return confidenceInterval;
    }

    /**
     * Adds a new item to the array by hashing and increasing the count of relevant buckets
     *
     * @param item is the item to be inserted
     */
    public void addItem(E item) {
        int hash = getHashValue(item);

//      Shift all the bits to right till only the bucket ID is left
        int bucketId = hash >>> (Integer.SIZE - lengthOfBucketId);

//      Shift all the bits to left till the bucket id is removed
        int remainingValue = hash << lengthOfBucketId;

        int newLeadingZeroCount = Integer.numberOfLeadingZeros(remainingValue) + 1;

//      update the value in the  bucket
        int currentLeadingZeroCount = countArray[bucketId];
        if (pastCountsEnabled) {
            pastCountsArray[bucketId].add(newLeadingZeroCount);
        }
        if (currentLeadingZeroCount < newLeadingZeroCount) {

            harmonicCountSum = harmonicCountSum - (1.0 / (1L << currentLeadingZeroCount))
                    + (1.0 / (1L << newLeadingZeroCount));

            if (currentLeadingZeroCount == 0) {
                noOfZeroBuckets--;
            }
            if (newLeadingZeroCount == 0) {
                noOfZeroBuckets++;
            }

            countArray[bucketId] = newLeadingZeroCount;

            calculateCardinality();
        }
    }

    /**
     * Removes the given item from the array and restore the distinctCount value by using the previous count.
     *
     * @param item is the item to be removed
     */
    public void removeItem(E item) {
        if (pastCountsEnabled) {
            int hash = getHashValue(item);

//      Shift all the bits to right till only the bucket ID is left
            int bucketId = hash >>> (Integer.SIZE - lengthOfBucketId);

//      Shift all the bits to left till the bucket id is removed
            int remainingValue = hash << lengthOfBucketId;

            int currentLeadingZeroCount = Integer.numberOfLeadingZeros(remainingValue) + 1;

            int newLeadingZeroCount = pastCountsArray[bucketId].remove(currentLeadingZeroCount);
            int oldLeadingZeroCount = countArray[bucketId];

//      check the next maximum leading zero count
            if (newLeadingZeroCount >= 0) {

                harmonicCountSum = harmonicCountSum - (1.0 / (1L << oldLeadingZeroCount))
                        + (1.0 / (1L << newLeadingZeroCount));
                if (oldLeadingZeroCount == 0) {
                    noOfZeroBuckets--;
                }
                if (newLeadingZeroCount == 0) {
                    noOfZeroBuckets++;
                }

                countArray[bucketId] = newLeadingZeroCount;

                calculateCardinality();
            }
        } else {
            throw new IllegalAccessError(this.getClass().getCanonicalName() +
                    " : Remove operation is called while the 'pastCountsEnabled' is false");
        }
    }

    /**
     * Compute an integer hash value for a given value
     *
     * @param value to be hashed
     * @return integer hash value
     */
    public int getHashValue(Object value) {
        return MurmurHash.hash(value);
    }

    /**
     * Calculate the {@code estimationFactor} based on the length of bucket id and number of buckets.
     * The used constants are proven values from the research paper.
     *
     * @param lengthOfBucketId is the length of bucket id
     * @param noOfBuckets      is the number of buckets
     * @return {@code estimationFactor}
     */
    private double getEstimationFactor(int lengthOfBucketId, int noOfBuckets) {
        switch (lengthOfBucketId) {
            case 4:
                return 0.673;
            case 5:
                return 0.697;
            case 6:
                return 0.709;
            default:
                return (0.7213 / (1 + 1.079 / noOfBuckets));
        }
    }

    /**
     * Clears all the counts stored in the data structure.
     */
    public void clear() {
        countArray = new int[noOfBuckets];

        if (pastCountsEnabled) {
            pastCountsArray = new CountList[noOfBuckets];
            for (int i = 0; i < noOfBuckets; i++) {
                pastCountsArray[i] = new CountList();
            }
        }
        harmonicCountSum = noOfBuckets;
        noOfZeroBuckets = noOfBuckets;
    }
}


