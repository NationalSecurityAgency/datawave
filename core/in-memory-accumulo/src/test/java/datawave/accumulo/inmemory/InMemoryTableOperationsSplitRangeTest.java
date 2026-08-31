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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Covers {@link InMemoryTableOperations#splitRangeByTablets(String, Range, int)}, which reports the units of work a range breaks into.
 * <p>
 * The grouping follows {@code TableOperationsImpl}, so the cases here are the ones where a naive implementation diverges from it: merging must not let the
 * leading range absorb every tablet, and the caller's cap is a maximum rather than a target.
 */
class InMemoryTableOperationsSplitRangeTest {

    private static final String TABLE = "t";

    private InMemoryTableOperations ops;

    @BeforeEach
    void setUp() throws Exception {
        InMemoryAccumulo acu = new InMemoryAccumulo(null);
        ops = new InMemoryTableOperations(acu, "root");
        ops.create(TABLE);
        // four tablets: (-inf,r03] (r03,r06] (r06,r09] (r09,+inf)
        ops.addSplits(TABLE, new TreeSet<>(Arrays.asList(new Text("r03"), new Text("r06"), new Text("r09"))));
    }

    @Test
    void nullRangeIsRejected() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> ops.splitRangeByTablets(TABLE, null, 4));
        assertEquals("range is null", e.getMessage());
    }

    @Test
    void maxSplitsBelowOneIsRejected() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> ops.splitRangeByTablets(TABLE, new Range(), 0));
        assertEquals("maximum splits must be >= 1", e.getMessage());
    }

    @Test
    void aCapOfOneIsTheWholeRange() throws Exception {
        assertEquals(Set.of(new Range()), ops.splitRangeByTablets(TABLE, new Range(), 1));
    }

    @Test
    void anUncappedRangeIsOneUnitOfWorkPerTablet() throws Exception {
        assertEquals(4, ops.splitRangeByTablets(TABLE, new Range(), 4).size());
        assertEquals(4, ops.splitRangeByTablets(TABLE, new Range(), 99).size(), "a cap above the tablet count cannot invent work");
    }

    /**
     * The case that catches merging in place. Four tablets under a cap of three must come back as three ranges - a leading range covering two tablets and the
     * other two untouched - not as one range that swallowed everything.
     */
    @Test
    void adjacentTabletsAreGroupedDownToTheCap() throws Exception {
        assertEquals(3, ops.splitRangeByTablets(TABLE, new Range(), 3).size());
        assertEquals(2, ops.splitRangeByTablets(TABLE, new Range(), 2).size());
    }

    @Test
    void aRangeInsideOneTabletIsNotSplit() throws Exception {
        Set<Range> ranges = ops.splitRangeByTablets(TABLE, new Range("r04", "r05"), 4);
        assertEquals(1, ranges.size());
        assertTrue(ranges.iterator().next().isStartKeyInclusive());
    }
}
