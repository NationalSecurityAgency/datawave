package datawave.next;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.base.Splitter;

import datawave.core.iterators.ResultCountingIterator;
import datawave.query.iterator.QueryIterator;
import datawave.query.iterator.QueryOptions;
import datawave.query.jexl.JexlASTHelper;

/**
 * An alternate {@link QueryIterator} that provides counts for simple queries.
 * <p>
 * This was an attempt an at optimized count query for the field index.
 */
public class CountQueryIterator implements SortedKeyValueIterator<Key,Value> {

    private final Logger log = LoggerFactory.getLogger(CountQueryIterator.class);

    private Range range;
    private ASTJexlScript script;
    private Set<String> datatypeFilter;
    private LongRange timeFilter;

    private Set<String> indexedFields;

    private SortedKeyValueIterator<Key,Value> source;
    private Map<String,String> options;
    private IteratorEnvironment env;

    private Key tk;
    private Value tv = new Value();

    private final Kryo kryo = new Kryo();

    public CountQueryIterator() {}

    public CountQueryIterator(CountQueryIterator other, IteratorEnvironment env) {
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
        } else {
            throw new RuntimeException("query not set");
        }

        if (options.containsKey(QueryOptions.DATATYPE_FILTER)) {
            String opt = options.get(QueryOptions.DATATYPE_FILTER);
            this.datatypeFilter = new HashSet<>(Splitter.on(',').splitToList(opt));
        }

        long start = -1;
        long stop = -1;

        if (options.containsKey(QueryOptions.START_TIME)) {
            String opt = options.get(QueryOptions.START_TIME);
            start = Long.parseLong(opt);
        }

        if (options.containsKey(QueryOptions.END_TIME)) {
            String opt = options.get(QueryOptions.END_TIME);
            stop = Long.parseLong(opt);
        }

        if (start > 0 && stop > 0) {
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
    }

    @Override
    public boolean hasTop() {
        return tk != null;
    }

    @Override
    public void next() throws IOException {
        tk = null;
        tv = null;
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        this.range = range;

        long start = System.currentTimeMillis();
        Set<Key> docIds = DocIdIteratorVisitor.getDocIds(script, range, source, datatypeFilter, timeFilter, indexedFields);
        long elapsed = System.currentTimeMillis() - start;
        log.info("scanned {} ids in {} ms", docIds.size(), elapsed);

        this.tk = range.getStartKey();
        this.tv = serializeCount(docIds.size(), range.getStartKey().getColumnVisibilityParsed());
    }

    private boolean isDocRange(Range range) {
        return range.isStartKeyInclusive() && range.getStartKey().getColumnFamily().getLength() > 0;
    }

    private Value serializeCount(long count, ColumnVisibility cv) {
        ResultCountingIterator.ResultCountTuple result = new ResultCountingIterator.ResultCountTuple(count, cv);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Output kryoOutput = new Output(baos);
        kryo.writeObject(kryoOutput, result);
        kryoOutput.close();

        return new Value(baos.toByteArray());
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
        return new CountQueryIterator(this, env);
    }
}
