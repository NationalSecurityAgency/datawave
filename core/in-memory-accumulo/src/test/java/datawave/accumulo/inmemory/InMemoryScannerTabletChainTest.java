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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;

import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Covers how a scan of a split table drives its per-tablet iterator stacks.
 * <p>
 * The scanner is built directly rather than through {@link InMemoryAccumuloClient} because constructing a client pulls in ZooKeeper, which this module
 * deliberately excludes from accumulo-server-base.
 */
class InMemoryScannerTabletChainTest {

    private static final int ROWS = 12;
    private static final List<Text> SPLITS = Arrays.asList(new Text("r03"), new Text("r07"));

    private InMemoryTable table;

    /** Records the range it was seeked with, and reassigns its source on init exactly as RebuildingScannerTestHelper's InterruptIterator does. */
    private static class SeekSpy implements SortedKeyValueIterator<Key,Value> {

        private final List<Range> seeks = new ArrayList<>();
        private SortedKeyValueIterator<Key,Value> source;

        @Override
        public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) {
            this.source = source;
        }

        @Override
        public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
            seeks.add(range);
            source.seek(range, columnFamilies, inclusive);
        }

        @Override
        public boolean hasTop() {
            return source.hasTop();
        }

        @Override
        public void next() throws IOException {
            source.next();
        }

        @Override
        public Key getTopKey() {
            return source.getTopKey();
        }

        @Override
        public Value getTopValue() {
            return source.getTopValue();
        }

        @Override
        public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
            throw new UnsupportedOperationException();
        }
    }

    @BeforeEach
    void setUp() {
        // LOGICAL time makes the stored timestamps the mutation count, so the fixture's keys are identical from run to run.
        table = new InMemoryTable(true, TimeType.LOGICAL, "1");
        for (int i = 0; i < ROWS; i++) {
            Mutation m = new Mutation(String.format("r%02d", i));
            m.put("cf", "cq", new Value("v" + i));
            table.addMutation(m);
        }
        table.addSplits(new TreeSet<>(SPLITS));
    }

    private InMemoryScanner scanner() {
        InMemoryScanner scanner = new InMemoryScanner(table, Authorizations.EMPTY);
        scanner.setRange(new Range());
        return scanner;
    }

    private static List<String> drain(Iterator<Entry<Key,Value>> iterator) {
        List<String> rows = new ArrayList<>();
        while (iterator.hasNext()) {
            rows.add(iterator.next().getKey().getRow().toString());
        }
        return rows;
    }

    /**
     * An injected iterator is a single instance shared by every stack the scan builds, and createFilter reassigns its source on each call. Building the stacks
     * one at a time is what keeps that safe - with all of them live at once, the earlier stacks read whichever source was wired in last and most of the table
     * goes missing with no error.
     */
    @Test
    void injectedIteratorSeesEveryTabletsKeys() {
        List<String> expected = drain(scanner().iterator());
        assertEquals(ROWS, expected.size(), "fixture should hold one key per row");

        InMemoryScanner scanner = scanner();
        scanner.addInjectedIterator(new SeekSpy());

        assertEquals(expected, drain(scanner.iterator()), "an injected iterator must not cost the scan any tablet's keys");
    }

    /** A tablet's stack is built only once the scan reaches it, as ThriftScanner opens the next tablet's session only after draining the current one. */
    @Test
    void tabletStacksAreBuiltOnlyAsTheScanReachesThem() {
        InMemoryScanner scanner = scanner();
        SeekSpy spy = new SeekSpy();
        scanner.addInjectedIterator(spy);

        Iterator<Entry<Key,Value>> results = scanner.iterator();
        assertTrue(spy.seeks.isEmpty(), "no tablet should be seeked before the first result is asked for");

        results.next();
        assertEquals(1, spy.seeks.size(), "only the tablet holding the first key should have been seeked");

        drain(results);
        assertEquals(SPLITS.size() + 1, spy.seeks.size(), "draining the scan should seek every tablet exactly once");
    }
}
