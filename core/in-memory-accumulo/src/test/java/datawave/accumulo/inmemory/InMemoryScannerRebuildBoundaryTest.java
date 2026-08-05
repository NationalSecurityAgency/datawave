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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Shows the continuation-range boundary is reachable through this project's own code, not only as an abstract property of {@link Range}.
 * <p>
 * {@link InMemoryScanner#rebuild(Key)} resumes a scan the same way a tablet server and {@code ThriftScanner} do - {@code new Range(lastKey, false, endKey,
 * endKeyInclusive)}. When a caller scans a range whose inclusive end bound is a real key in the table and the scan is torn down on that final key, rebuilding
 * throws instead of reporting a finished scan. {@code RebuildingScannerTestHelper} in query-core drives exactly this call, so its teardown modes reach it.
 * <p>
 * The scanner is built directly rather than through {@link InMemoryAccumuloClient} because constructing a client pulls in ZooKeeper, which this module
 * deliberately excludes from accumulo-server-base.
 *
 * @see ContinuationRangeBoundaryTest
 */
class InMemoryScannerRebuildBoundaryTest {

    private InMemoryTable table;
    private List<Key> keys;

    @BeforeEach
    void setUp() {
        // LOGICAL time makes the stored timestamps the mutation count, so the fixture's keys are identical from run to run.
        table = new InMemoryTable(true, TimeType.LOGICAL, "1");
        for (int i = 0; i < 5; i++) {
            Mutation m = new Mutation(String.format("row%d", i));
            m.put("cf", "cq", new Value("value" + i));
            table.addMutation(m);
        }
        keys = drain(scanner(new Range()).iterator());
        assertEquals(5, keys.size(), "fixture should hold one key per row");
    }

    private InMemoryScanner scanner(Range range) {
        InMemoryScanner scanner = new InMemoryScanner(table, Authorizations.EMPTY);
        scanner.setRange(range);
        return scanner;
    }

    private static List<Key> drain(Iterator<Entry<Key,Value>> iterator) {
        List<Key> found = new ArrayList<>();
        while (iterator.hasNext()) {
            found.add(iterator.next().getKey());
        }
        return found;
    }

    /**
     * The defect, reached through a scanner. The scan has returned every key it was asked for, but resuming after the last one cannot be expressed against an
     * inclusive end bound sitting on that same key.
     */
    @Test
    void rebuildingOnTheInclusiveEndKeyThrows() {
        Key firstKey = keys.get(0);
        Key lastKey = keys.get(keys.size() - 1);
        InMemoryScanner scanner = scanner(new Range(firstKey, true, lastKey, true));

        assertEquals(keys, drain(scanner.iterator()), "the scan itself completes and returns everything in range");

        // new Key(lastKey) mirrors ThriftScanner, which copies the last result key before making it the continuation start.
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> scanner.rebuild(new Key(lastKey)));
        assertTrue(e.getMessage().startsWith("Start key must be less than end key in range"), "unexpected message: " + e.getMessage());
    }

    /** Control: tearing down anywhere other than the end bound rebuilds cleanly and resumes strictly after the key already returned. */
    @Test
    void rebuildingInsideTheRangeResumesAfterTheLastKey() {
        Key firstKey = keys.get(0);
        Key lastKey = keys.get(keys.size() - 1);
        Key middleKey = keys.get(2);
        InMemoryScanner scanner = scanner(new Range(firstKey, true, lastKey, true));

        Iterator<Entry<Key,Value>> resumed = assertDoesNotThrow(() -> scanner.rebuild(new Key(middleKey)));

        List<Key> after = drain(resumed);
        assertEquals(keys.subList(3, keys.size()), after, "resuming must skip the key already returned");
        assertFalse(after.contains(middleKey));
    }

    /** An unbounded scan can never present the condition, because there is no end key for the continuation to collide with. */
    @Test
    void rebuildingOnTheLastKeyOfAnUnboundedScanIsSafe() {
        Key lastKey = keys.get(keys.size() - 1);
        InMemoryScanner scanner = scanner(new Range());

        Iterator<Entry<Key,Value>> resumed = assertDoesNotThrow(() -> scanner.rebuild(new Key(lastKey)));
        assertTrue(drain(resumed).isEmpty(), "nothing follows the last key, so the rebuilt scan is simply empty");
    }

    /**
     * The end bound shape that keeps real scans safe. A range ending one key past the last real key resumes cleanly on that key, which is why callers using
     * {@link Range#exact} or a tablet-clipped range never see the failure above.
     */
    @Test
    void rebuildingIsSafeWhenTheEndBoundSitsPastRealData() {
        Key firstKey = keys.get(0);
        Key lastKey = keys.get(keys.size() - 1);
        Key pastTheEnd = lastKey.followingKey(PartialKey.ROW);
        InMemoryScanner scanner = scanner(new Range(firstKey, true, pastTheEnd, false));

        assertEquals(keys, drain(scanner.iterator()), "the same keys are in range");
        Iterator<Entry<Key,Value>> resumed = assertDoesNotThrow(() -> scanner.rebuild(new Key(lastKey)));
        assertTrue(drain(resumed).isEmpty());
    }
}
