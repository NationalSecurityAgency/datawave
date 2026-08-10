package datawave.query.iterator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.query.data.parsers.EventKey;
import datawave.query.data.parsers.KeyParser;
import datawave.query.iterator.logic.IndexIteratorBridgeTest;

public class NestedQueryIteratorTest {

    private static final Logger log = LoggerFactory.getLogger(NestedQueryIteratorTest.class);

    private final KeyParser parser = new EventKey();
    private final SortedSet<String> results = new TreeSet<>();

    @Test
    public void testSingleNest() {
        SortedSet<String> uids = new TreeSet<>(List.of("uid-a", "uid-b", "uid-c"));
        NestedQuery<Key> nestedQuery = createNestedQuery("FIELD_A", uids);

        NestedQueryIterator<Key> nestedQueryIterator = new NestedQueryIterator<>(nestedQuery);
        drive(nestedQueryIterator);
        assertEquals(uids, results);
    }

    @Test
    public void testMultipleNests() {
        SortedSet<String> uidsA = new TreeSet<>(List.of("uid-a", "uid-b", "uid-c"));
        NestedQuery<Key> nestedQueryA = createNestedQuery("FIELD_A", uidsA);

        SortedSet<String> uidsB = new TreeSet<>(List.of("uid-x", "uid-y", "uid-z"));
        NestedQuery<Key> nestedQueryB = createNestedQuery("FIELD_A", uidsB);

        Collection<NestedQuery<Key>> nestedQueries = List.of(nestedQueryA, nestedQueryB);

        NestedQueryIterator<Key> nestedQueryIterator = new NestedQueryIterator<>(nestedQueries);
        drive(nestedQueryIterator);

        SortedSet<String> expected = new TreeSet<>();
        expected.addAll(uidsA);
        expected.addAll(uidsB);
        assertEquals(expected, results);
    }

    private void drive(NestedQueryIterator<Key> nestedQueryIterator) {
        results.clear();
        while (nestedQueryIterator.hasNext()) {
            Key tk = nestedQueryIterator.next();
            parser.parse(tk);
            results.add(parser.getUid());
        }
    }

    private NestedQuery<Key> createNestedQuery(String field, SortedSet<String> uids) {
        NestedIterator<Key> iter = IndexIteratorBridgeTest.createIndexIteratorBridge(field, uids);
        try {
            iter.seek(new Range(), Collections.emptySet(), true);
        } catch (IOException e) {
            fail("failed to initialize nested query");
            throw new RuntimeException(e);
        }

        NestedQuery<Key> nestedQuery = new NestedQuery<>();
        nestedQuery.setIterator(iter);
        return nestedQuery;
    }

}
