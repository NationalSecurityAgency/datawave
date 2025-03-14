package datawave.next;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ParseException;
import org.apache.commons.lang3.LongRange;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;

import datawave.next.stats.DocIdQueryIteratorStats;
import datawave.next.stats.DocumentIteratorStats;
import datawave.query.iterator.QueryOptions;
import datawave.query.jexl.JexlASTHelper;

/**
 * An iterator that runs against the field index and returns all document keys that match the query
 */
public class DocIdQueryIterator implements SortedKeyValueIterator<Key,Value> {

    private final Logger log = LoggerFactory.getLogger(DocIdQueryIterator.class);

    public static final String BATCH_SIZE = "batch.size";
    public static final String SCAN_TIMEOUT = "scan.timeout";

    private Range range;
    private ASTJexlScript script;
    private Set<String> datatypeFilter;
    private LongRange timeFilter;

    private Set<String> indexedFields;

    private SortedKeyValueIterator<Key,Value> source;
    private Map<String,String> options;
    private IteratorEnvironment env;
    private int batchSize = 1;
    private long scanTimeout = -1;

    private Key tk;
    private Value tv = new Value();

    private Iterator<Key> data;

    private boolean statsReturned = false;
    private final DocIdQueryIteratorStats timingStats = new DocIdQueryIteratorStats();
    private final DocumentIteratorStats iteratorStats = new DocumentIteratorStats();

    public DocIdQueryIterator() {}

    public DocIdQueryIterator(DocIdQueryIterator other, IteratorEnvironment env) {
        this.source = other.source.deepCopy(env);
        this.options = other.options;
        this.env = other.env;

        this.range = other.range;
        this.script = other.script;
        this.datatypeFilter = other.datatypeFilter;
        this.timeFilter = other.timeFilter;
    }

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        timingStats.markScanInit();
        this.source = source;
        this.options = options;
        this.env = env;

        // need the query, datatype filter, start and end dates (for time filter)

        if (options.containsKey(QueryOptions.QUERY)) {
            String opt = options.get(QueryOptions.QUERY);

            try {
                this.script = JexlASTHelper.parseAndFlattenJexlQuery(opt);
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
        }

        if (options.containsKey(QueryOptions.DATATYPE_FILTER)) {
            String opt = options.get(QueryOptions.DATATYPE_FILTER);
            this.datatypeFilter = new HashSet<>(Splitter.on(',').splitToList(opt));
        }

        if (options.containsKey(QueryOptions.START_TIME) && options.containsKey(QueryOptions.END_TIME)) {
            String opt = options.get(QueryOptions.START_TIME);
            long start = Long.parseLong(opt);

            opt = options.get(QueryOptions.END_TIME);
            long stop = Long.parseLong(opt);

            this.timeFilter = LongRange.of(start, stop);
        } else {
            throw new RuntimeException("date not set");
        }

        if (options.containsKey(QueryOptions.INDEXED_FIELDS)) {
            String opt = options.get(QueryOptions.INDEXED_FIELDS);
            this.indexedFields = new HashSet<>(Splitter.on(',').splitToList(opt));
        } else {
            throw new RuntimeException("indexed fields not set");
        }

        if (options.containsKey(BATCH_SIZE)) {
            batchSize = Integer.parseInt(options.get(BATCH_SIZE));
        } else {
            batchSize = 1;
        }

        if (options.containsKey(SCAN_TIMEOUT)) {
            scanTimeout = Long.parseLong(options.get(SCAN_TIMEOUT));
        }
    }

    @Override
    public boolean hasTop() {
        return tk != null;
    }

    @Override
    public void next() throws IOException {
        tk = null;

        if (data.hasNext()) {
            tk = data.next();
        }

        // if this is the last key,
        if (batchSize == 1 && !data.hasNext() && !statsReturned) {
            statsReturned = true;

            // close enough
            timingStats.markRetrievalStop();

            tv = new Value(iteratorStats + ":" + timingStats);
            if (tk == null) {
                // if no document keys were found, i.e. a scan that returned no results, then create a fake key
                tk = new Key(range.getStartKey().getRow(), new Text("STATS"));
            }
        }

        if (batchSize > 1 && !statsReturned) {
            int count = 0;
            Set<Key> batch = new HashSet<>();
            if (tk != null) {
                count++;
                batch.add(tk);
            }

            while (data.hasNext() && count < batchSize) {
                count++;
                batch.add(data.next());
            }

            String serialized = batchToString(batch);

            if (!data.hasNext()) {
                // add stats if this is the last key
                statsReturned = true;
                serialized += ";" + iteratorStats + ":" + timingStats;

                if (tk == null) {
                    // if no document keys were found, i.e. a scan that returned no results, then create a fake key
                    tk = new Key(range.getStartKey().getRow(), new Text("STATS"));
                }
            }

            tv = new Value(serialized);
        }
    }

    /**
     * Data structure <code>row;column-families;iter-stats:timing-stats</code>
     *
     * @param batch
     *            the batch of record ids
     * @return serialized data
     */
    private String batchToString(Set<Key> batch) {
        StringBuilder sb = new StringBuilder();
        sb.append(range.getStartKey().getRow()).append(';');

        Set<String> columnFamilies = columnFamiliesFromKeys(batch);
        sb.append(Joiner.on(',').join(columnFamilies));
        return sb.toString();
    }

    private Set<String> columnFamiliesFromKeys(Set<Key> batch) {
        Set<String> columnFamilies = new HashSet<>();
        for (Key key : batch) {
            columnFamilies.add(key.getColumnFamily().toString());
        }
        return columnFamilies;
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        timingStats.markScanStart();
        this.range = range;

        DocIdIteratorVisitor visitor = new DocIdIteratorVisitor(source, range, datatypeFilter, timeFilter, indexedFields);
        if (scanTimeout > 0) {
            visitor.setMaxScanTimeMillis(scanTimeout);
        }

        Set<Key> docIds = visitor.getDocIds(script);
        data = new TreeSet<>(docIds).iterator();

        timingStats.markScanStop();

        long elapsedNS = timingStats.getScanTime();
        long elapsedMS = TimeUnit.NANOSECONDS.toMillis(elapsedNS);

        log.info("scanned {} ids in {} ns or {} ms", docIds.size(), elapsedNS, elapsedMS);

        iteratorStats.merge(visitor.getStats());
        timingStats.incrementTotalDocumentIds(docIds.size());

        timingStats.markRetrievalStart();
        next();
    }

    @Override
    public Key getTopKey() {
        return tk;
    }

    @Override
    public Value getTopValue() {
        return tv;
    }

    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        return new DocIdQueryIterator(this, env);
    }
}
