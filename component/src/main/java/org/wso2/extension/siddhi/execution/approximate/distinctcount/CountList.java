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
import java.util.ArrayList;

/**
 * A data structure to keep track of maximum zero counts for a bucket
 * to restore the previous maximums when the old counts are removed.
 */
public class CountList implements Serializable {
    private static final long serialVersionUID = 4596984540980789758L;

    ArrayList<Integer> counts;

    public CountList() {
        counts = new ArrayList<>();
    }

    /**
     * Add new value to the counts if the previous values are greater than or equal to the new value.
     *
     * @param newValue
     * @return {@code true} if the newValue is added, {@code false} if the newValue is not added
     */
    public boolean add(int newValue) {
        if (counts.size() > 0) {
            for (int i = counts.size() - 1; i >= 0; i--) {
                if (newValue > counts.get(i)) {
                    counts.remove(i);
                } else {
                    break;
                }
            }
            if (counts.size() == 0 || newValue <= counts.get(counts.size() - 1)) {
                counts.add(newValue);
                return true;
            } else {
                return false;
            }
        } else {
            counts.add(newValue);
            return true;
        }
    }

    /**
     * Remove the given value from the counts
     *
     * @return the next count value if the removed value was the first value in the list,
     * -1 if the removed value does not affect the counts, 0 if the list is empty
     */
    public int remove(int value) {
        if (counts.size() > 0) {
            if (counts.get(0) == value) {
                counts.remove(0);
                if (counts.isEmpty()) {
                    return 0;
                } else {
                    return counts.get(0);
                }
            } else {
                return -1;
            }
        }
        return 0;
    }
}
