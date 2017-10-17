package org.wso2.extension.siddhi.execution.approximate.distinctcount;

import java.nio.charset.Charset;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

/**
 * This is a very fast, non-cryptographic hash suitable for general hash-based
 * lookup. See http://murmurhash.googlepages.com/ for more details.
 * <p/>
 * <p>
 * The C version of MurmurHash 2.0 found at that site was ported to Java by
 * Andrzej Bialecki (ab at getopt org).
 * </p>
 */
public final class MurmurHash {
    private MurmurHash() {
    }

    public static int hash(Object o) {
        if (o == null) {
            return 0;
        } else if (o instanceof Long) {
            return hashLong(((Long) o).longValue());
        } else if (o instanceof Integer) {
            return hashLong((long) ((Integer) o).intValue());
        } else if (o instanceof Double) {
            return hashLong(Double.doubleToRawLongBits(((Double) o).doubleValue()));
        } else if (o instanceof Float) {
            return hashLong((long) Float.floatToRawIntBits(((Float) o).floatValue()));
        } else if (o instanceof String) {
            return hash(((String) o).getBytes(Charset.forName("UTF-8")));
        } else {
            return hash(o.toString());
        }
    }

    public static int hashLong(long data) {
        int m = 1540483477;
        int r = 24;
        int h = 0;
        int k = (int) data * m;
        k ^= k >>> r;
        h = h ^ k * m;
        k = (int) (data >> 32) * m;
        k ^= k >>> r;
        h *= m;
        h ^= k * m;
        h ^= h >>> 13;
        h *= m;
        h ^= h >>> 15;
        return h;
    }

}
