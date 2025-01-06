package datawave.query.scanner;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.accumulo.core.iteratorsImpl.IteratorBuilder;
import org.apache.accumulo.core.iteratorsImpl.IteratorConfigUtil;
import org.apache.accumulo.core.security.Authorizations;

import datawave.query.iterator.SortedListKeyValueIterator;
import datawave.query.tables.SessionOptions;

public class LocalBatchScanner extends SessionOptions implements BatchScanner {
    private SortedListKeyValueIterator itr;
    private Collection<Range> ranges;
    private boolean statsEnabled = false;
    private StatsIterator statsIterator;

    public LocalBatchScanner(SortedListKeyValueIterator itr) {
        this(itr, false);
    }

    public LocalBatchScanner(SortedListKeyValueIterator itr, boolean statsEnabled) {
        this.itr = itr;
        this.statsEnabled = statsEnabled;
    }

    public long getNextCount() {
        return statsIterator == null ? -1 : statsIterator.getNextCount();
    }

    public long getSeekCount() {
        return statsIterator == null ? -1 : statsIterator.getSeekCount();
    }

    @Override
    public Iterator<Map.Entry<Key,Value>> iterator() {
        Collections.sort(serverSideIteratorList, (o1, o2) -> {
            if (o1.priority < o2.priority) {
                return -1;
            } else if (o1.priority > o2.priority) {
                return 1;
            } else {
                return 0;
            }
        });

        SortedKeyValueIterator<Key,Value> base = this.itr;
        IteratorEnvironment env = new LocalIteratorEnvironment();

        if (statsEnabled) {
            statsIterator = new StatsIterator();
            try {
                statsIterator.init(base, Collections.emptyMap(), env);
                base = statsIterator;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        IteratorBuilder iteratorBuilder = IteratorBuilder.builder(serverSideIteratorList).opts(serverSideIteratorOptions).env(env).build();

        List<Map.Entry<Key,Value>> list = new ArrayList<>();
        try {
            SortedKeyValueIterator<Key,Value> created = IteratorConfigUtil.loadIterators(base, iteratorBuilder);
            List<ByteSequence> columns = new ArrayList<>();
            for (Column c : fetchedColumns) {
                columns.add(new ArrayByteSequence(c.columnFamily));
            }

            for (Range range : ranges) {
                created.seek(range, columns, true);
                while (created.hasTop()) {
                    list.add(new AbstractMap.SimpleImmutableEntry<>(created.getTopKey(), created.getTopValue()));
                    created.next();
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return list.iterator();
    }

    @Override
    public void setRanges(Collection<Range> ranges) {
        this.ranges = ranges;
    }

    public static class LocalIteratorEnvironment implements IteratorEnvironment {
        @Override
        public IteratorUtil.IteratorScope getIteratorScope() {
            return IteratorUtil.IteratorScope.scan;
        }

        @Override
        public boolean isUserCompaction() {
            return false;
        }

        @Override
        public boolean isFullMajorCompaction() {
            return false;
        }

        @Override
        public Authorizations getAuthorizations() {
            return new Authorizations();
        }
    }

    public static class StatsIterator extends WrappingIterator {
        private long nextCount = 0;
        private long seekCount = 0;

        @Override
        public void next() throws IOException {
            super.next();
            nextCount++;
        }

        @Override
        public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
            super.seek(range, columnFamilies, inclusive);
            seekCount++;
        }

        public long getNextCount() {
            return nextCount;
        }

        public long getSeekCount() {
            return seekCount;
        }
    }
}
