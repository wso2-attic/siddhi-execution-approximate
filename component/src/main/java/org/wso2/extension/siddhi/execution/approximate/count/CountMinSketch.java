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
package org.wso2.extension.siddhi.execution.approximate.count;

import org.wso2.extension.siddhi.execution.approximate.distinctcount.MurmurHash;

import java.io.Serializable;
import java.security.SecureRandom;
import java.util.ArrayList;

/**
 * A probabilistic data structure to keep count of different items.
 * The referred research paper - Count-Min Sketch by Graham Cormode
 * http://dimacs.rutgers.edu/%7Egraham/pubs/papers/cmencyc.pdf
 *
 * @param <E> is the type of data to be counted
 */
public class CountMinSketch<E> implements Serializable {
    private static final long serialVersionUID = -3359695950348163739L;

    private int depth;
    private int width;

    private long totalNoOfItems;

    //  2D array to store the counts
    private long[][] countArray;

    //  hash coefficients
    private ArrayList<Integer> hashCoefficientsA;
    private ArrayList<Integer> hashCoefficientsB;

    //  Error factor of approximation
    private double relativeError;

    /**
     * Instantiate the count min sketch based on a given relative error and confidence
     *
     * @param relativeError is a positive number less than 1 (e.g. 0.01)
     * @param confidence    is a positive number less than 1 (e.g. 0.01)
     *                      which is the probability of answers being within the relative error
     */
    public CountMinSketch(double relativeError, double confidence) {
        if (!(relativeError < 1 && relativeError > 0) || !(confidence < 1 && confidence > 0)) {
            throw new IllegalArgumentException("confidence and relativeError must be values in the range (0,1)");
        }
        this.totalNoOfItems = 0;
        this.relativeError = relativeError;

//      depth = ln(1 / (1 - confidence))
        this.depth = (int) Math.ceil(Math.log(1 / (1 - confidence)));
//      width = e / relativeError
        this.width = (int) Math.ceil(Math.E / relativeError);

        this.countArray = new long[depth][width];

//      create random hash coefficients
//      using linear hash functions of the form (a*x+b)
//      a,b are chosen independently for each hash function.
        this.hashCoefficientsA = new ArrayList<>(depth);
        this.hashCoefficientsB = new ArrayList<>(depth);
        SecureRandom random = new SecureRandom();
        random.setSeed(123);
        for (int i = 0; i < depth; i++) {
            hashCoefficientsA.add(random.nextInt(Integer.MAX_VALUE));
            hashCoefficientsB.add(random.nextInt(Integer.MAX_VALUE));
        }
    }

    /**
     * Compute the cell position in a row of the count array for a given hash value
     *
     * @param hash is the integer hash value generated from some hash function
     * @return an integer value in the range [0,width)
     */
    private int getArrayIndex(int hash) {
        return Math.abs(hash % width);
    }

    /**
     * Compute a set of different integer hash values for a given item
     *
     * @param item is the object for which the hash values are calculated
     * @return an int array(of size {@code depth}) of hash values
     */
    private int[] getHashValues(E item) {
        int[] hashValues = new int[depth];
        int hash = MurmurHash.hash(item);
        for (int i = 0; i < depth; i++) {
            hashValues[i] = hashCoefficientsA.get(i) * hash + hashCoefficientsB.get(i);
        }
        return hashValues;
    }

    /**
     * Adds the count of an item to the count min sketch
     * calculate hash values relevant for each row in the count array
     * compute indices in the range of [0, width) from those hash values
     * increment each value in the cell of relevant row and index (e.g. countArray[row][index]++)
     *
     * @param item is the item to be inserted
     * @return the approximate count of the item
     */
    public synchronized long insert(E item) {
        totalNoOfItems++;

        int[] hashValues = getHashValues(item);
        int index;
        long currentMin = Long.MAX_VALUE;

        for (int i = 0; i < depth; i++) {
            index = getArrayIndex(hashValues[i]);
            countArray[i][index]++;
            if (currentMin > countArray[i][index]) {
                currentMin = countArray[i][index];
            }
        }
        return currentMin;
    }

    /**
     * Removes the count of an item from the count min sketch
     * calculate hash values relevant for each row in the count array
     * compute indices in the range of [0, width) from those hash values
     * decrement each value in the cell of relevant row and index (e.g. countArray[row][index]--)
     *
     * @param item is the item to be inserted
     * @return the approximate count of the item
     */
    public synchronized long remove(E item) {
        totalNoOfItems--;

        int[] hashValues = getHashValues(item);
        int index;
        long currentMin = Long.MAX_VALUE;

        for (int i = 0; i < depth; i++) {
            index = getArrayIndex(hashValues[i]);
            countArray[i][index]--;

            if (currentMin > countArray[i][index]) {
                currentMin = countArray[i][index];
            }
        }
        return currentMin;
    }

    /**
     * Calculate the confidence interval of the approximate count
     * [approximateCount - (totalNoOfItems * relativeError), approximateCount + (totalNoOfItems * relativeError)]
     * @param count is the approximate count
     * @return a long array which contains the lower bound and
     * the upper bound of the confidence interval consecutively
     */
    public synchronized long[] getConfidenceInterval(long count) {
        long error = (long) (totalNoOfItems * relativeError);
        if (count - error > 0) {
            return new long[]{count - error, count + error};
        } else {
            return new long[]{0, count + error};
        }
    }

    /**
     * Clears the counts within the sketch.
     */
    public synchronized void clear() {
        this.totalNoOfItems = 0;
        this.countArray = new long[depth][width];
    }
}
