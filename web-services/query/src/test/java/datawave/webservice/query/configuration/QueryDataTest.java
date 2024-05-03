package datawave.webservice.query.configuration;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.junit.Test;

public class QueryDataTest {

    @Test
    public void testCopyConstructor() {
        String query = "FOO == 'bar'";
        Collection<Range> ranges = Collections.singleton(new Range(new Key("row"), true, new Key("row\0"), false));
        Collection<String> columnFamilies = Collections.singleton("FOO");
        List<IteratorSetting> settings = new ArrayList<>();
        settings.add(new IteratorSetting(20, "iterator", "QueryIterator.class"));

        QueryData original = new QueryData(query, ranges, columnFamilies, settings);
        QueryData copy = new QueryData(original);

        assertEquals(original.getQuery(), copy.getQuery());
        assertEquals(original.getRanges(), copy.getRanges());
        assertEquals(original.getColumnFamilies(), copy.getColumnFamilies());
        assertEquals(original.getSettings(), copy.getSettings());
    }

    @Test
    public void testCorrectReuse() {
        String query = "FOO == 'bar'";
        List<IteratorSetting> settings = new ArrayList<>();
        settings.add(new IteratorSetting(20, "iter1", "iter1.class"));

        QueryData original = new QueryData();
        original.setQuery(query);
        original.setSettings(settings);
        original.setColumnFamilies(Collections.emptySet());
        original.setRanges(Collections.singleton(Range.prefix("1")));

        List<QueryData> queries = new ArrayList<>();
        queries.add(original);
        queries.add(new QueryData(original).withRanges(Collections.singleton(Range.prefix("2"))));
        queries.add(new QueryData(original).withRanges(Collections.singleton(Range.prefix("3"))));

        int count = 1;
        for (QueryData qd : queries) {
            assertEquals(query, qd.getQuery());
            assertRange(Integer.toString(count), qd.getRanges());
            assertEquals(0, qd.getColumnFamilies().size());
            assertEquals(settings, qd.getSettings());
            count++;
        }
    }

    @Test
    public void testCorrectDownstreamReuse() {
        String query = "FOO == 'bar'";
        List<IteratorSetting> settings = new ArrayList<>();
        settings.add(new IteratorSetting(20, "iter1", "iter1.class"));

        QueryData original = new QueryData();
        original.setQuery(query);
        original.setSettings(settings);
        original.setRanges(Collections.singleton(Range.prefix("1")));

        List<QueryData> queries = new ArrayList<>();
        queries.add(original);
        queries.add(new QueryData(original).withRanges(Collections.singleton(Range.prefix("2"))));
        queries.add(new QueryData(original).withRanges(Collections.singleton(Range.prefix("3"))));

        for (QueryData qd : queries) {
            qd.getSettings().add(new IteratorSetting(21, "iter2", "iter2.class"));
        }

        List<IteratorSetting> expectedSettings = new ArrayList<>();
        expectedSettings.add(new IteratorSetting(20, "iter1", "iter1.class"));
        expectedSettings.add(new IteratorSetting(21, "iter2", "iter2.class"));

        int count = 1;
        for (QueryData qd : queries) {
            assertEquals(query, qd.getQuery());
            assertRange(Integer.toString(count), qd.getRanges());
            assertEquals(0, qd.getColumnFamilies().size());
            assertEquals(expectedSettings, qd.getSettings());
            count++;
        }
    }

    private void assertRange(String expectedRow, Collection<Range> ranges) {
        assertEquals(1, ranges.size());
        Range range = ranges.iterator().next();
        assertEquals(expectedRow, range.getStartKey().getRow().toString());
    }
}
