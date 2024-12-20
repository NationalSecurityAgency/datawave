package datawave.query.pointer;

import static datawave.query.Constants.EMPTY_VALUE;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.attribute.pointer.DataPointer;
import datawave.attribute.pointer.ViewDataPointer;
import datawave.query.data.parsers.EventKey;
import datawave.query.table.parser.ContentKeyValueFactory;

public class ViewDataPointerHandler implements DataPointerHandler {
    public static final String LENGTH_LIMIT = "ViewDataPointer.limit.length";
    public static final String TRUNCATE_FIELD = "ViewDataPointer.truncate.field";

    private SortedKeyValueIterator<Key,Value> source;
    private IteratorEnvironment env;
    private int lengthLimit = -1;
    private String truncateField;
    private final ObjectMapper dataPointerObjectMapper;

    public ViewDataPointerHandler() {
        dataPointerObjectMapper = new ObjectMapper();
    }

    @Override
    public boolean canFetch(DataPointer pointer) {
        return pointer instanceof ViewDataPointer;
    }

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) {
        this.source = source;
        this.env = env;
        if (options != null && options.containsKey(LENGTH_LIMIT)) {
            lengthLimit = Integer.parseInt(options.get(LENGTH_LIMIT));
        }
        if (options != null && options.containsKey(TRUNCATE_FIELD)) {
            truncateField = options.get(TRUNCATE_FIELD);
        }
    }

    @Override
    public Multimap<Key,Value> fetch(DataPointer pointer, Key reference) throws IOException {
        if (source == null) {
            throw new IllegalStateException("cannot fetch without initializing a source iterator");
        }

        Key startKey = pointer.get();
        Key endKey = startKey.followingKey(PartialKey.ROW_COLFAM_COLQUAL);

        // construct the view range
        Range dColumn = new Range(startKey, true, endKey, false);

        source.seek(dColumn, Collections.emptyList(), true);

        Multimap<Key,Value> data = HashMultimap.create();

        EventKey referenceParser = null;

        // pull back all the data
        while (source.hasTop()) {
            if (referenceParser == null) {
                referenceParser = new EventKey();
                referenceParser.parse(reference);
            }
            byte[] rawBytes = ContentKeyValueFactory.decodeAndDecompressContent(source.getTopValue().get());
            if (lengthLimit != -1 && rawBytes.length > lengthLimit) {
                // truncate
                rawBytes = Arrays.copyOf(rawBytes, lengthLimit);
                if (truncateField != null) {
                    data.put(getTruncatedKey(reference, referenceParser), EMPTY_VALUE);
                }
            }
            data.put(getEventKey(reference, referenceParser, rawBytes), EMPTY_VALUE);

            source.next();
        }

        return data;
    }

    @Override
    public boolean isPointer(Key key, Value value) {
        EventKey eventKey = new EventKey();
        eventKey.parse(key);

        return eventKey.isDataPointer() && value != null && value.get().length > 0;
    }

    @Override
    public DataPointer getPointer(Key key, Value value) throws IOException {
        return dataPointerObjectMapper.readerFor(DataPointer.class).readValue(value.get());
    }

    private Key getEventKey(Key reference, EventKey referenceParser, byte[] value) {
        return new Key(reference.getRow(), reference.getColumnFamily(), new Text(referenceParser.getField() + '\u0000' + new String(value)),
                        reference.getColumnVisibility(), reference.getTimestamp());
    }

    private Key getTruncatedKey(Key reference, EventKey referenceParser) {
        return new Key(reference.getRow(), reference.getColumnFamily(), new Text(truncateField + '\u0000' + referenceParser.getField()),
                        reference.getColumnVisibility(), reference.getTimestamp());
    }
}
