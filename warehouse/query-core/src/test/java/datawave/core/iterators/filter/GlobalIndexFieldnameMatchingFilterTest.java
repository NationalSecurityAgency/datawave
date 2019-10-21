package datawave.core.iterators.filter;

import datawave.data.normalizer.NumberNormalizer;
import datawave.query.util.regex.RegexTrie;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class GlobalIndexFieldnameMatchingFilterTest {

    @Test
    public void testFilterList() throws Exception {
        GlobalIndexFieldnameMatchingFilter filter = new GlobalIndexFieldnameMatchingFilter();

        // add all filenames except for #10
        Map<String, String> options = new HashMap<>();
        int index = 1;
        for (int i = 0; i < 10; i++) {
            options.put(GlobalIndexFieldnameMatchingFilter.LITERAL + Integer.toString(index++), getFieldname(i));
        }
        for (int i = 11; i < 40; i++) {
            options.put(GlobalIndexFieldnameMatchingFilter.LITERAL + Integer.toString(index++), getFieldname(i));
        }

        // create our generator source
        GlobalIndexKeyValueGenerator source = new GlobalIndexKeyValueGenerator();
        filter.init(source, options, null);

        // range from 0 to 39
        filter.seek(new Range(getKey(0), true, getKey(40), false), Collections.emptyList(), false);

        // test that we skipped #10
        int expected = 0;
        for (; expected < 10; expected++) {
            index = getIndex(filter.getTopKey().getColumnFamily().toString());
            assertEquals(expected, index);
            filter.next();
        }

        expected = 11;

        while (filter.getTopKey() != null) {
            index = getIndex(filter.getTopKey().getColumnFamily().toString());
            assertEquals(expected++, index);
            filter.next();
        }
    }

    @Ignore
    @Test
    public void testPerformance() throws Exception {

        List<String> fieldnames = new ArrayList<>();
        for (int i = 0; i <= 5000; i++) {
            fieldnames.add(getFieldname(i));
        }

        for (int numFields = 1; numFields < 5000; numFields++) {

            GlobalIndexFieldnameMatchingFilter filter = new GlobalIndexFieldnameMatchingFilter();
            Map<String, String> options = new HashMap<>();
            for (int index = 0; index < numFields; index++) {
                options.put(GlobalIndexFieldnameMatchingFilter.LITERAL + Integer.toString(index+1), fieldnames.get(index));
            }

            // create our generator source
            GlobalIndexKeyValueGenerator source = new GlobalIndexKeyValueGenerator();
            filter.init(source, options, null);

            long start = System.currentTimeMillis();

            // range from 0 to 9999
            filter.seek(new Range(getKey(0), true, getKey(10000), false), Collections.emptyList(), false);
            while (filter.getTopKey() != null) {
                filter.next();
            }

            long literalTime = System.currentTimeMillis() - start;


            filter = new GlobalIndexFieldnameMatchingFilter();
            options = new HashMap<>();
            RegexTrie trie = new RegexTrie();
            trie.addAll(fieldnames.subList(0, numFields));
            options.put(GlobalIndexFieldnameMatchingFilter.PATTERN + "1", trie.toRegex());

            // create our generator source
            source = new GlobalIndexKeyValueGenerator();
            filter.init(source, options, null);

            start = System.currentTimeMillis();

            // range from 0 to 9999
            filter.seek(new Range(getKey(0), true, getKey(10000), false), Collections.emptyList(), false);
            while (filter.getTopKey() != null) {
                filter.next();
            }

            long regexTime = System.currentTimeMillis() - start;

            System.out.println("#" + numFields + ": " + literalTime + " / " + regexTime + " " + options);
        }
    }



    /**
     * This is an iterator that simply generates a series of keys with the same row and a sequentially increasing fieldname
     */
    private static class GlobalIndexKeyValueGenerator implements SortedKeyValueIterator {
        private Key next = null;
        private static final Value EMPTY_VALUE = new Value();

        @Override
        public boolean hasTop() {
            return next != null;
        }

        private int index = 0;
        private int lastIndex = 0;
        @Override
        public void next() throws IOException {
            if (index > lastIndex) {
                next = null;
            } else {
                next = getKey(index++);
            }
        }

        @Override
        public WritableComparable<?> getTopKey() {
            return next;
        }

        @Override
        public Writable getTopValue() {
            return EMPTY_VALUE;
        }

        @Override
        public SortedKeyValueIterator deepCopy(IteratorEnvironment env) {
            return new GlobalIndexKeyValueGenerator();
        }

        @Override
        public void seek(Range range, Collection columnFamilies, boolean inclusive) throws IOException {
            index = getIndex(range.getStartKey());
            if (!range.isStartKeyInclusive()) {
                index++;
            }
            lastIndex = getIndex(range.getEndKey());
            if (!range.isEndKeyInclusive()) {
                lastIndex--;
            }
            next();
        }

        @Override
        public void init(SortedKeyValueIterator source, Map options, IteratorEnvironment env) throws IOException {
        }
    }

    private static final NumberNormalizer n = new NumberNormalizer();
    protected static String getFieldname(int index) {
        return "FIELD." + n.normalize(Integer.toString(index++));
    }

    protected static Key getKey(int index) {
        return new Key("row", getFieldname(index));
    }

    protected static int getIndex(String fieldname) {
        return n.denormalize(fieldname.substring(6)).intValue();
    }

    protected static int getIndex(Key key) {
        return getIndex(key.getColumnFamily().toString());
    }

}
