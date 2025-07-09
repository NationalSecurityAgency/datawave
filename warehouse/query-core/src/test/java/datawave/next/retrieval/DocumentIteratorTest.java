package datawave.next.retrieval;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.system.SortedMapIterator;
import org.junit.jupiter.api.Test;

import datawave.query.iterator.QueryOptions;
import datawave.query.iterator.SourceManagerTest.MockIteratorEnvironment;
import datawave.query.util.TypeMetadata;

public class DocumentIteratorTest {

    private final Value value = new Value();

    @Test
    public void testTeardownRebuild() throws IOException {
        SortedKeyValueIterator<Key,Value> source = createSource();
        Map<String,String> options = createOptions();
        IteratorEnvironment env = new MockIteratorEnvironment();

        DocumentIterator documentIterator = new DocumentIterator();
        documentIterator.init(source, options, env);

        // create exclusive start key to simulate a teardown/rebuild
        Key start = new Key("row", "dt\0uid-4");
        Key end = new Key("row\0");
        Range range = new Range(start, false, end, true);

        documentIterator.seek(range, Collections.emptySet(), true);
        assertTrue(documentIterator.hasTop());
        Key tk = documentIterator.getTopKey();
        assertEquals(0, tk.compareTo(new Key("row", "dt\0uid-5"), PartialKey.ROW_COLFAM));
    }

    protected SortedKeyValueIterator<Key,Value> createSource() {
        SortedMap<Key,Value> data = new TreeMap<>();
        data.put(new Key("row", "dt\0uid-1", "FIELD\0value"), value);
        data.put(new Key("row", "dt\0uid-2", "FIELD\0value"), value);
        data.put(new Key("row", "dt\0uid-3", "FIELD\0value"), value);
        data.put(new Key("row", "dt\0uid-4", "FIELD\0value"), value);
        data.put(new Key("row", "dt\0uid-5", "FIELD\0value"), value);
        data.put(new Key("row", "dt\0uid-6", "FIELD\0value"), value);
        return new SortedMapIterator(data);
    }

    protected Map<String,String> createOptions() {
        Map<String,String> options = new HashMap<>();
        options.put(QueryOptions.QUERY, "FIELD == 'value'");
        options.put(QueryOptions.TYPE_METADATA, new TypeMetadata().toString());
        options.put(DocumentIteratorOptions.CANDIDATES, "dt\0uid-1,dt\0uid-2,dt\0uid-3,dt\0uid-4,dt\0uid-5,dt\0uid-6");
        options.put(QueryOptions.START_TIME, String.valueOf(0L));
        options.put(QueryOptions.END_TIME, String.valueOf(Long.MAX_VALUE));
        return options;
    }
}
