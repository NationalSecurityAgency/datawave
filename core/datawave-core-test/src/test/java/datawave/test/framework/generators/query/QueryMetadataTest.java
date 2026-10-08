package datawave.test.framework.generators.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * Tests {@link QueryMetadata} accessors and equality, including unordered comparison of expected event ids.
 */
class QueryMetadataTest {

    @Test
    void testAccessorsReturnWhatWasSupplied() {
        QueryMetadata metadata = QueryMetadata.of("FIELD == 'a'", "FIELD == 'a'", List.of(1, 2));

        assertEquals("FIELD == 'a'", metadata.getQuery());
        assertEquals("FIELD == 'a'", metadata.getPlan());
        assertEquals(List.of(1, 2), metadata.getIds());
    }

    /**
     * The query text is what a failure report is read by, so it is what {@code toString} shows.
     */
    @Test
    void testToStringIsTheQuery() {
        assertEquals("FIELD == 'a'", QueryMetadata.of("FIELD == 'a'", "FIELD == 'plan'", List.of(1)).toString());
    }

    @Test
    void testEqualWhenQueryPlanAndIdsMatch() {
        QueryMetadata first = QueryMetadata.of("FIELD == 'a'", "FIELD == 'a'", List.of(1, 2));
        QueryMetadata second = QueryMetadata.of("FIELD == 'a'", "FIELD == 'a'", List.of(1, 2));

        assertEquals(first, second);
        assertEquals(first.hashCode(), second.hashCode());
    }

    @Test
    void testDifferingQueryIsNotEqual() {
        QueryMetadata first = QueryMetadata.of("FIELD == 'a'", "FIELD == 'a'", List.of(1));
        QueryMetadata second = QueryMetadata.of("FIELD == 'b'", "FIELD == 'a'", List.of(1));

        assertNotEquals(first, second);
    }

    /**
     * The plan is asserted separately from the query because a term can normalize into a different plan than the query text it came from.
     */
    @Test
    void testDifferingPlanIsNotEqual() {
        QueryMetadata first = QueryMetadata.of("FIELD == 'Alpha'", "FIELD == 'alpha'", List.of(1));
        QueryMetadata second = QueryMetadata.of("FIELD == 'Alpha'", "FIELD == 'Alpha'", List.of(1));

        assertNotEquals(first, second);
    }

    /**
     * Different expected event ids make query metadata unequal even when query and plan text match.
     */
    @Test
    void testDifferingIdsAreNotEqual() {
        QueryMetadata first = QueryMetadata.of("FIELD == 'a'", "FIELD == 'a'", List.of(1, 2));
        QueryMetadata second = QueryMetadata.of("FIELD == 'a'", "FIELD == 'a'", List.of(1, 2, 3));

        assertNotEquals(first, second);
        assertNotEquals(first.hashCode(), second.hashCode());
    }

    /**
     * Event id order does not affect equality or the hash code.
     */
    @Test
    void testIdOrderDoesNotAffectEquality() {
        QueryMetadata ascending = QueryMetadata.of("FIELD == 'a'", "FIELD == 'a'", List.of(1, 2, 3));
        QueryMetadata shuffled = QueryMetadata.of("FIELD == 'a'", "FIELD == 'a'", List.of(3, 1, 2));

        assertEquals(ascending, shuffled);
        assertEquals(ascending.hashCode(), shuffled.hashCode());
    }

    @Test
    void testNotEqualToNullOrAnotherType() {
        QueryMetadata metadata = QueryMetadata.of("FIELD == 'a'", "FIELD == 'a'", List.of(1));

        assertNotEquals(null, metadata);
        assertNotEquals("FIELD == 'a'", metadata);
    }

    @Test
    void testEmptyIdsAreDistinctFromPopulatedIds() {
        QueryMetadata empty = QueryMetadata.of("FIELD == 'a'", "FIELD == 'a'", List.of());
        QueryMetadata populated = QueryMetadata.of("FIELD == 'a'", "FIELD == 'a'", List.of(1));

        assertNotEquals(empty, populated);
    }
}
