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

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Characterizes the boundary conditions of the range Accumulo builds to resume a scan after a batch.
 * <p>
 * Both the tablet server and the client advance a scan the same way: take the last key returned, make it an <em>exclusive</em> start, and carry the original
 * end bound over unchanged.
 *
 * <pre>
 * // tserver/tablet/Scanner.java - read()
 * range = new Range(results.getContinueKey(), !results.isSkipContinueKey(), range.getEndKey(), range.isEndKeyInclusive());
 *
 * // core/clientImpl/ThriftScanner.java
 * scanState.range = new Range(new Key(lastResultKey), false, scanState.range.getEndKey(), scanState.range.isEndKeyInclusive());
 * </pre>
 *
 * That construction is self-contradictory when the batch boundary lands on the range's own end key - "start strictly after K, stop at or before K" describes an
 * empty interval, and {@link Range} rejects it rather than treating it as empty. These tests pin down exactly which combinations are rejected, and which
 * end-bound shapes are immune.
 *
 * @see <a href="https://github.com/apache/accumulo/blob/rel/2.1.4/core/src/main/java/org/apache/accumulo/core/data/Range.java">Range</a>
 */
class ContinuationRangeBoundaryTest {

    /** A key that could plausibly be both real data and someone's explicit end bound. */
    private static final Key KEY = new Key("row5", "cf", "cq", 100L);
    private static final Key LATER_KEY = new Key("row9", "cf", "cq", 100L);

    /** Builds the continuation exactly as Scanner.read and ThriftScanner do. */
    private static Range continuationAfter(Key lastKeyReturned, Key endKey, boolean endKeyInclusive) {
        return new Range(new Key(lastKeyReturned), false, endKey, endKeyInclusive);
    }

    @Nested
    @DisplayName("the continuation range is rejected when the batch ends on the end bound")
    class Rejected {

        /**
         * The defect. A scan whose last returned key is its own inclusive end key has in fact completed, but the continuation Accumulo builds for it cannot be
         * constructed, so the scan aborts instead.
         */
        @Test
        void lastKeyIsTheInclusiveEndKey() {
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> continuationAfter(KEY, KEY, true));
            assertTrue(e.getMessage().startsWith("Start key must be less than end key in range"), "unexpected message: " + e.getMessage());
        }

        /** An exclusive end bound is no safer: beforeStartKeyImpl uses compareTo(start) <= 0, so an equal key still fails. */
        @Test
        void lastKeyIsTheExclusiveEndKey() {
            assertThrows(IllegalArgumentException.class, () -> continuationAfter(KEY, KEY, false));
        }

        /**
         * A user iterator is free to emit keys outside the range it was seeked with, and a tablet server does not filter them out. If one becomes the
         * continuation point, the same construction fails.
         */
        @Test
        void lastKeyIsPastTheEndKey() {
            assertThrows(IllegalArgumentException.class, () -> continuationAfter(LATER_KEY, KEY, true));
        }
    }

    @Nested
    @DisplayName("the continuation range is accepted")
    class Accepted {

        @Test
        void lastKeyIsStrictlyInsideTheRange() {
            Range continuation = assertDoesNotThrow(() -> continuationAfter(KEY, LATER_KEY, true));
            assertFalse(continuation.contains(KEY), "the key already returned must not be scanned again");
            assertTrue(continuation.contains(LATER_KEY), "an inclusive end bound stays reachable");
        }

        /** An unbounded scan can never present the condition, because there is no end key to collide with. */
        @Test
        void endKeyIsInfinite() {
            Range continuation = assertDoesNotThrow(() -> continuationAfter(LATER_KEY, null, true));
            assertFalse(continuation.contains(LATER_KEY));
        }

        /**
         * Proves the exclusivity is what makes the interval invalid, not the equality. The same two keys as an inclusive start describe a legal single-key
         * range, so Range is not rejecting "start equals end" in general.
         */
        @Test
        void sameKeyWithAnInclusiveStartIsALegalRange() {
            Range single = assertDoesNotThrow(() -> new Range(KEY, true, KEY, true));
            assertTrue(single.contains(KEY));
        }
    }

    @Nested
    @DisplayName("end bounds that sit past real data are immune")
    class ImmuneEndBounds {

        /**
         * Why this is rarely seen in practice. Ranges reaching a tablet server are clipped to the tablet, and a tablet's data range ends at a synthetic key
         * derived from the split row, one byte past every real key in that row. No real data key can equal it, so the continuation is always constructible.
         */
        @Test
        void tabletDataRangeEndsPastEveryRealKey() {
            Range tablet = new KeyExtent(TableId.of("1"), new Text("row9"), new Text("row0")).toDataRange();
            Key lastRealKeyInTablet = new Key("row9", "cf", "cq", 100L);

            assertTrue(lastRealKeyInTablet.compareTo(tablet.getEndKey()) < 0, "a real key must sort strictly below the synthetic tablet end");
            assertDoesNotThrow(() -> continuationAfter(lastRealKeyInTablet, tablet.getEndKey(), tablet.isEndKeyInclusive()));
        }

        /** Range.exact ends on the following row, so callers using it never hit the condition either. */
        @Test
        void rangeExactEndsPastEveryRealKey() {
            Range exact = Range.exact(new Text("row5"));
            Key realKeyInRow = new Key("row5", "cf", "cq", 100L);

            assertTrue(realKeyInRow.compareTo(exact.getEndKey()) < 0);
            assertDoesNotThrow(() -> continuationAfter(realKeyInRow, exact.getEndKey(), exact.isEndKeyInclusive()));
        }

        /** followingKey is the general escape hatch: it is by construction the smallest key strictly greater than its input. */
        @Test
        void followingKeyIsStrictlyGreater() {
            Key following = KEY.followingKey(PartialKey.ROW);

            assertEquals(-1, KEY.compareTo(following));
            assertDoesNotThrow(() -> continuationAfter(KEY, following, false));
        }
    }
}
