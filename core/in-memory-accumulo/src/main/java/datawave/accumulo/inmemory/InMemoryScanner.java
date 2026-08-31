/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package datawave.accumulo.inmemory;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.clientImpl.ScannerOptions;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;

public class InMemoryScanner extends InMemoryScannerBase implements Scanner, ScannerRebuilder, Cloneable {

    int batchSize = 0;
    Range range = new Range();

    @Override
    public InMemoryScanner clone() {
        InMemoryScanner clone = new InMemoryScanner(table, getAuthorizations());
        clone.batchSize = getBatchSize();
        clone.range = getRange();
        ScannerOptions.setOptions(clone, this);
        clone.retryTimeout = retryTimeout;
        return clone;
    }

    InMemoryScanner(InMemoryTable table, Authorizations auths) {
        super(table, auths);
    }

    @Override
    public void setRange(Range range) {
        this.range = range;
    }

    @Override
    public Range getRange() {
        return this.range;
    }

    @Override
    public void setBatchSize(int size) {
        this.batchSize = size;
    }

    @Override
    public int getBatchSize() {
        return this.batchSize;
    }

    @Override
    public void enableIsolation() {}

    @Override
    public void disableIsolation() {}

    /**
     * Returns every key the stacks emit, including any a user iterator produces outside the seek range
     * <p>
     * A tablet server does not bound its stack's output, so neither does this
     */
    @Override
    public Iterator<Entry<Key,Value>> iterator() {
        // One independent iterator stack per tablet the range touches, each seeked with only that tablet's slice and built only once the tablet before it is
        // exhausted, as a real client does. With no splits configured this is a single stack over the whole range, exactly as before.
        return new TabletChain(splitRangeByTablets(range), this::stackFor);
    }

    @Override
    public Iterator<Entry<Key,Value>> rebuild(Key lastKey) {
        if (lastKey != null) {
            // simply rebuild the range starting at lastKey, non-inclusive.
            Range newRange = new Range(lastKey, false, range.getEndKey(), range.isEndKeyInclusive());
            this.range = newRange;
        }

        // now rebuild the iterator stack using the new range.
        return iterator();
    }

    @Override
    public long getReadaheadThreshold() {
        return 0;
    }

    @Override
    public void setReadaheadThreshold(long batches) {

    }

}
