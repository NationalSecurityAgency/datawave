package datawave.query.iterator.logic;

import static datawave.table.constants.QueryConstants.NULL_BYTE_CHAR;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iteratorsImpl.system.SortedMapIterator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import datawave.query.attributes.Attribute;
import datawave.query.attributes.Document;
import datawave.query.attributes.PreNormalizedAttributeFactory;
import datawave.query.util.TypeMetadata;

/**
 * Tests for {@link IndexOnlyFieldPresenceFilter}.
 */
public class IndexOnlyFieldPresenceFilterTest {

    private static final Value EMPTY = new Value();

    private SortedMap<Key,Value> data;

    @BeforeEach
    public void beforeEach() {
        data = new TreeMap<>();
    }

    @DisplayName("Basic functionality tests")
    @Nested
    class BasicFunctionalityTests {

        @DisplayName("Null field throws exception")
        @Test
        void testNullField() {
            assertThatThrownBy(() -> new IndexOnlyFieldPresenceFilter(null, new SortedMapIterator(data))).isInstanceOf(IllegalArgumentException.class)
                            .hasMessage("field must not be null or blank");
        }

        @DisplayName("Blank field throws exception")
        @Test
        void testBlankField() {
            assertThatThrownBy(() -> new IndexOnlyFieldPresenceFilter("   ", new SortedMapIterator(data))).isInstanceOf(IllegalArgumentException.class)
                            .hasMessage("field must not be null or blank");
        }

        @DisplayName("Null source throws exception")
        @Test
        void testNullSource() {
            assertThatThrownBy(() -> new IndexOnlyFieldPresenceFilter("FIELD", null)).isInstanceOf(NullPointerException.class)
                            .hasMessage("source must not be null");
        }

        @DisplayName("Context required contract is respected")
        @Test
        void testContextRequiredContract() {
            IndexOnlyFieldPresenceFilter iter = build("FIELD_B");
            assertTrue(iter.isContextRequired());
            assertFalse(iter.isNonEventField());
            assertTrue(iter.hasNext());
            assertNull(iter.next());
            assertTrue(iter.leaves().isEmpty());
            assertTrue(iter.children().isEmpty());
        }
    }

    @DisplayName("Basic query tests")
    @Nested
    class BasicQueryTests {

        @DisplayName("Field present for one document and absent for another")
        @Test
        void testPresentAndAbsent() {
            put("row", "FIELD_A", "value-a", "datatype", "uid-1");
            put("row", "FIELD_A", "value-a", "datatype", "uid-2");
            put("row", "FIELD_B", "zzzzzzz", "datatype", "uid-2");

            IndexOnlyFieldPresenceFilter iter = build("FIELD_B");
            assertMiss(iter, "row", "datatype\0uid-1");
            assertHit(iter, "row", "datatype\0uid-2");
        }

        @DisplayName("Field absent from shard table entirely")
        @Test
        void testFieldAbsentFromShard() {
            put("row", "FIELD_A", "value-a", "datatype", "uid-1");
            put("row", "FIELD_A", "value-a", "datatype", "uid-2");

            IndexOnlyFieldPresenceFilter iter = build("FIELD_B");
            assertMiss(iter, "row", "datatype\0uid-1");
            assertMiss(iter, "row", "datatype\0uid-2");
        }

        @DisplayName("Empty source")
        @Test
        void testEmptySource() {
            IndexOnlyFieldPresenceFilter iter = build("FIELD_B");
            assertMiss(iter, "row", "datatype\0uid-1");
        }

        @DisplayName("One result even with multiple values")
        @Test
        void testMultipleValuesForOneDocument() {
            for (String value : List.of("aaa", "bbb", "ccc", "ddd")) {
                put("row", "FIELD_B", value, "datatype", "uid-2");
            }

            IndexOnlyFieldPresenceFilter iter = build("FIELD_B");
            assertMiss(iter, "row", "datatype\0uid-1");
            assertHit(iter, "row", "datatype\0uid-2");
            assertMiss(iter, "row", "datatype\0uid-3");
        }

        @DisplayName("Entry sorted under last value is still found")
        @Test
        void testSparseDocumentUnderLastValue() {
            put("row", "FIELD_B", "aaa", "datatype", "uid-1");
            put("row", "FIELD_B", "aaa", "datatype", "uid-3");
            put("row", "FIELD_B", "mmm", "datatype", "uid-1");
            put("row", "FIELD_B", "zzz", "datatype", "uid-2");

            IndexOnlyFieldPresenceFilter iter = build("FIELD_B");
            assertHit(iter, "row", "datatype\0uid-1");
            assertHit(iter, "row", "datatype\0uid-2");
            assertHit(iter, "row", "datatype\0uid-3");
            assertMiss(iter, "row", "datatype\0uid-4");
        }

        @DisplayName("Test multiple documents")
        @Test
        void testManyDocuments() {
            // Add entries, with a hit for every evenly-numbered document, and a miss for each oddly-numbered document.
            for (int i = 0; i < 20; i++) {
                if (i % 2 == 0) {
                    put("row", "FIELD_B", "value-" + (i % 3), "datatype", uid(i));
                }
                put("row", "FIELD_A", "value-a", "datatype", uid(i));
            }

            // Verify the hits and misses.
            IndexOnlyFieldPresenceFilter iter = build("FIELD_B");
            for (int i = 0; i < 20; i++) {
                if (i % 2 == 0) {
                    assertHit(iter, "row", "datatype\0" + uid(i));
                } else {
                    assertMiss(iter, "row", "datatype\0" + uid(i));
                }
            }
        }

        @DisplayName("Test hits across multiple rows")
        @Test
        void testMultipleRows() {
            put("row-1", "FIELD_B", "zzz", "datatype", "uid-2");
            put("row-2", "FIELD_B", "aaa", "datatype", "uid-1");

            IndexOnlyFieldPresenceFilter iter = build("FIELD_B");
            assertMiss(iter, "row-1", "datatype\0uid-1");
            assertHit(iter, "row-1", "datatype\0uid-2");
            assertHit(iter, "row-2", "datatype\0uid-1");
            assertMiss(iter, "row-2", "datatype\0uid-2");
        }

        @DisplayName("Entry for same uid in different row does not count.")
        @Test
        void testSameUidDifferentRow() {
            put("row-2", "FIELD_B", "aaa", "datatype", "uid-1");

            IndexOnlyFieldPresenceFilter iter = build("FIELD_B");
            assertMiss(iter, "row-1", "datatype\0uid-1");
        }

        @DisplayName("Hits only on exact matches for field")
        @Test
        void testFieldNamePrefix() {
            // FIELD_B should not match against FIELD_BB.
            put("row", "FIELD_BB", "aaa", "datatype", "uid-1");
            put("row", "FIELD_C", "aaa", "datatype", "uid-1");

            IndexOnlyFieldPresenceFilter iter = build("FIELD_B");
            assertMiss(iter, "row", "datatype\0uid-1");
        }

        @DisplayName("Hits only exact matches for UIDs")
        @Test
        void testUidPrefix() {
            // uid-1 should not match aginst uid-10.
            put("row", "FIELD_B", "aaa", "datatype", "uid-10");
            put("row", "FIELD_B", "bbb", "datatype", "uid-10");

            IndexOnlyFieldPresenceFilter iter = build("FIELD_B");
            assertMiss(iter, "row", "datatype\0uid-1");
            assertHit(iter, "row", "datatype\0uid-10");
        }

        @DisplayName("Hits on exact matches for datatypes")
        @Test
        void testDatatypeMismatch() {
            put("row", "FIELD_B", "aaa", "datatype-b", "uid-1");

            IndexOnlyFieldPresenceFilter iter = build("FIELD_B");
            assertMiss(iter, "row", "datatype-a\0uid-1");
            assertHit(iter, "row", "datatype-b\0uid-1");
        }

        @DisplayName("Time filter applies correctly")
        @Test
        void testTimeFilter() {
            data.put(new Key("row", "fi\0FIELD_B", "aaa\0datatype\0uid-1", "PUBLIC", 5L), EMPTY);
            data.put(new Key("row", "fi\0FIELD_B", "aaa\0datatype\0uid-2", "PUBLIC", 5L), EMPTY);
            data.put(new Key("row", "fi\0FIELD_B", "bbb\0datatype\0uid-2", "PUBLIC", 50L), EMPTY);

            IndexOnlyFieldPresenceFilter iter = new IndexOnlyFieldPresenceFilter("FIELD_B", new SortedMapIterator(data))
                            .withTimeFilter(key -> key.getTimestamp() >= 10L);
            assertMiss(iter, "row", "datatype\0uid-1");
            assertHit(iter, "row", "datatype\0uid-2");
        }

        @DisplayName("Aggregates entries given attribute factory")
        @Test
        void testAggregation() {
            put("row", "FIELD_B", "aaa", "datatype", "uid-2");
            put("row", "FIELD_B", "bbb", "datatype", "uid-2");

            // With an attribute factory, a hit populates the document with a single attribute for the field that is used for evaluation only. A miss leaves the
            // document empty.
            IndexOnlyFieldPresenceFilter iter = build("FIELD_B").withAttributeFactory(new PreNormalizedAttributeFactory(new TypeMetadata()));

            assertHit(iter, "row", "datatype\0uid-2");
            Document doc = iter.document();
            Attribute<?> attr = doc.get("FIELD_B");
            assertNotNull(attr);
            assertEquals(1, attr.size());
            assertFalse(attr.isToKeep(), "presence attribute should be used for evaluation only");

            assertMiss(iter, "row", "datatype\0uid-3");
            assertNull(iter.document().get("FIELD_B"));
        }

        @DisplayName("No aggregation without attribute factory")
        @Test
        void testNoAggregationWithoutAttributeFactory() {
            put("row", "FIELD_B", "aaa", "datatype", "uid-2");

            IndexOnlyFieldPresenceFilter iter = build("FIELD_B");
            assertHit(iter, "row", "datatype\0uid-2");
            assertNull(iter.document().get("FIELD_B"));
        }
    }

    @DisplayName("TLD query tests")
    @Nested
    class TLDQueryTests {

        @DisplayName("Entries on child documents ignored without TLD matching.")
        @Test
        void testChildIgnoredWithoutTldMatching() {
            put("row", "FIELD_B", "aaa", "datatype", "root.1");

            IndexOnlyFieldPresenceFilter iter = build("FIELD_B");
            assertMiss(iter, "row", "datatype\0root");
        }

        @DisplayName("Match on root entry is a hit")
        @Test
        void testRootEntry() {
            put("row", "FIELD_B", "aaa", "datatype", "root");

            IndexOnlyFieldPresenceFilter iter = buildTld("FIELD_B");
            assertHit(iter, "row", "datatype\0root");
        }

        @DisplayName("Match on child entry is a hit")
        @Test
        void testChildEntry() {
            put("row", "FIELD_B", "aaa", "datatype", "root.1");

            IndexOnlyFieldPresenceFilter iter = buildTld("FIELD_B");
            assertHit(iter, "row", "datatype\0root");
        }

        @DisplayName("Match on grandchild entry is a hit")
        @Test
        void testGrandchildEntry() {
            put("row", "FIELD_B", "aaa", "datatype", "root.1.4");

            IndexOnlyFieldPresenceFilter iter = buildTld("FIELD_B");
            assertHit(iter, "row", "datatype\0root");
        }

        @DisplayName("Neighboring roots are not considered descendents")
        @Test
        void testSimilarRootsAreNotDescendants() {
            put("row", "FIELD_B", "aaa", "datatype", "root-x");
            put("row", "FIELD_B", "bbb", "datatype", "root2");
            put("row", "FIELD_B", "ccc", "datatype", "root-x.1");

            // Other roots that share the target's prefix are not descendants. 'root-x' sorts between 'root' and 'root.1', and 'root2' after 'root.1'
            IndexOnlyFieldPresenceFilter iter = buildTld("FIELD_B");
            assertMiss(iter, "row", "datatype\0root");
        }

        @DisplayName("Match on descendent entry sorted after similar root for same value")
        @Test
        void testChildAfterSimilarRoot() {
            put("row", "FIELD_B", "aaa", "datatype", "root-x");
            put("row", "FIELD_B", "aaa", "datatype", "root.2");

            IndexOnlyFieldPresenceFilter iter = buildTld("FIELD_B");
            assertHit(iter, "row", "datatype\0root");
            assertHit(iter, "row", "datatype\0root-x");
            assertMiss(iter, "row", "datatype\0root0");
        }

        @DisplayName("Match on children spread across values for similar roots")
        @Test
        void testChildrenSpreadAcrossValues() {
            put("row", "FIELD_B", "aaa", "datatype", "a-root.1");
            put("row", "FIELD_B", "mmm", "datatype", "b-root");
            put("row", "FIELD_B", "zzz", "datatype", "c-root.3");

            IndexOnlyFieldPresenceFilter iter = buildTld("FIELD_B");
            assertHit(iter, "row", "datatype\0a-root");
            assertHit(iter, "row", "datatype\0b-root");
            assertHit(iter, "row", "datatype\0c-root");
            assertMiss(iter, "row", "datatype\0d-root");
        }

        private IndexOnlyFieldPresenceFilter buildTld(String field) {
            return build(field).withTldMatching(true);
        }
    }

    private void assertHit(IndexOnlyFieldPresenceFilter iter, String row, String datatypeUid) {
        Key candidate = new Key(row, datatypeUid);
        assertEquals(candidate, iter.move(candidate), "expected field to be present for " + datatypeUid.replace(NULL_BYTE_CHAR, '/'));
    }

    private void assertMiss(IndexOnlyFieldPresenceFilter iter, String row, String datatypeUid) {
        Key candidate = new Key(row, datatypeUid);
        Key result = iter.move(candidate);
        assertNotEquals(candidate, result, "expected field to be absent for " + datatypeUid.replace(NULL_BYTE_CHAR, '/'));
        // the miss key must sort after the candidate but before any following document in the same row
        assertTrue(result.compareTo(candidate) > 0);
        assertEquals(new Key(row, datatypeUid + NULL_BYTE_CHAR), result);
    }

    private IndexOnlyFieldPresenceFilter build(String field) {
        return new IndexOnlyFieldPresenceFilter(field, new SortedMapIterator(data));
    }

    private void put(String row, String field, String value, String datatype, String uid) {
        data.put(new Key(row, "fi\0" + field, value + NULL_BYTE_CHAR + datatype + NULL_BYTE_CHAR + uid, 10L), EMPTY);
    }

    private static String uid(int i) {
        return String.format("uid-%03d", i);
    }
}
