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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.clientImpl.ScannerOptions;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.system.SortedMapIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.collections.iterators.IteratorChain;

public class InMemoryBatchScanner extends InMemoryScannerBase implements BatchScanner, ScannerRebuilder, Cloneable {
    
    List<Range> ranges = null;
    
    @Override
    public InMemoryBatchScanner clone() {
        InMemoryBatchScanner clone = new InMemoryBatchScanner(table, getAuthorizations());
        clone.ranges = (ranges == null ? null : new ArrayList<>(ranges));
        ScannerOptions.setOptions(clone, this);
        clone.retryTimeout = retryTimeout;
        
        return clone;
    }
    
    public InMemoryBatchScanner(InMemoryTable mockTable, Authorizations authorizations) {
        super(mockTable, authorizations);
    }
    
    @Override
    public void setRanges(Collection<Range> ranges) {
        if (ranges == null || ranges.size() == 0) {
            throw new IllegalArgumentException("ranges must be non null and contain at least 1 range");
        }
        
        this.ranges = Range.mergeOverlapping(ranges);
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public Iterator<Entry<Key,Value>> iterator() {
        if (ranges == null) {
            throw new IllegalStateException("ranges not set");
        }
        
        IteratorChain chain = new IteratorChain();
        for (Range range : ranges) {
            SortedKeyValueIterator<Key,Value> i = new SortedMapIterator(table.table);
            try {
                i = createFilter(i);
                i.seek(range, createColumnBSS(fetchedColumns), !fetchedColumns.isEmpty());
                if (i.hasTop()) {
                    chain.addIterator(new IteratorAdapter(i));
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return chain;
    }
    
    @Override
    public Iterator<Entry<Key,Value>> rebuild(Key lastKey) {
        // Rebuild the set of ranges. We should drop all ranges up until the range that includes
        // the specified lastKey. The one that includes it will be modified to start at lastKey,
        // non-inclusive. All subsequent ranges will remain in the list.
        // Note the key assumption here is that the ranges are processed in order (see IteratorChain
        // used above) and that the ranges are non-overlapping (see Range.mergeOverlapping() used
        // above).
        if (lastKey != null) {
            List<Range> newRanges = new ArrayList<>();
            boolean found = false;
            for (Range range : ranges) {
                if (found) {
                    newRanges.add(range);
                } else if (range.contains(lastKey)) {
                    found = true;
                    Range newRange = new Range(lastKey, false, range.getEndKey(), range.isEndKeyInclusive());
                    newRanges.add(newRange);
                }
            }
            if (!ranges.isEmpty() && newRanges.isEmpty()) {
                StringBuilder rangesForPrint = new StringBuilder();
                for (Range r : ranges) {
                    rangesForPrint.append(r.toString());
                }
                throw new IllegalStateException("Did not find specified key in previous set of ranges: " + rangesForPrint.toString() + " key: " + lastKey);
            }
            this.ranges = newRanges;
        }
        
        // now return a rebuild iterator stack using the new set of ranges.
        return iterator();
    }
    
    @Override
    public void close() {}
}
