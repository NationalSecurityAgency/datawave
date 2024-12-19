package datawave.query.pointer;

import static datawave.query.Constants.EMPTY_VALUE;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.query.data.parsers.EventKey;
import datawave.query.table.parser.ContentKeyValueFactory;

public class ViewDataPointer implements DataPointer {
    @JsonIgnore
    public static final String LENGTH_LIMIT = "ViewDataPointer.limit.length";
    @JsonIgnore
    public static final String TRUNCATE_FIELD = "ViewDataPointer.truncate.field";

    private final String type = "dView";

    @JsonProperty
    private String shard;

    @JsonProperty
    private String docId;

    @JsonProperty
    private String view;

    @JsonIgnore
    private SortedKeyValueIterator<Key,Value> source;

    @JsonIgnore
    private IteratorEnvironment env;

    @JsonIgnore
    private int lengthLimit = -1;

    @JsonIgnore
    private String truncateField;

    public ViewDataPointer() {
        // no-op
    }

    public ViewDataPointer(String docId, String view) {
        this.docId = docId;
        this.view = view;
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
    public Multimap<Key,Value> fetch(Key reference) throws IOException {
        if (source == null) {
            throw new IllegalStateException("cannot fetch without initializing a source iterator");
        }

        Key startKey = new Key(shard, "d", docId + '\u0000' + view);
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

    private Key getEventKey(Key reference, EventKey referenceParser, byte[] value) {
        return new Key(reference.getRow(), reference.getColumnFamily(), new Text(referenceParser.getField() + '\u0000' + new String(value)),
                        reference.getColumnVisibility(), reference.getTimestamp());
    }

    private Key getTruncatedKey(Key reference, EventKey referenceParser) {
        return new Key(reference.getRow(), reference.getColumnFamily(), new Text(truncateField + '\u0000' + referenceParser.getField()),
                        reference.getColumnVisibility(), reference.getTimestamp());
    }

    private Value decodeAndTruncate(Value raw) {
        byte[] rawBytes = ContentKeyValueFactory.decodeAndDecompressContent(raw.get());
        if (lengthLimit != -1 && rawBytes.length > lengthLimit) {
            byte[] truncatedBytes = Arrays.copyOf(rawBytes, lengthLimit);
            return new Value(truncatedBytes);
        }

        return new Value(rawBytes);
    }

    @Override
    public List<Key> getTransformKeys(Key reference) {
        EventKey parsedKey = new EventKey();
        parsedKey.parse(reference);

        // create the keys to be transformed by the EventQueryDataDecorator to produce a lookup for this content directly
        // SOME_FIELD=/DataWave/Query/LookupContentUUID/{view}/{id}
        // SOME_FIELD_CONTENT_UID=event:shardID/datatype/uid
        // SOME_FIELD_CONTENT_VIEW={view}

        Key idKey = new Key(reference.getRow().toString(), reference.getColumnFamily().toString(), parsedKey.getField() + "_CONTENT_ID" + '\u0000'
                        + reference.getRow().toString() + '/' + parsedKey.getDatatype() + '/' + parsedKey.getUid());
        Key viewKey = new Key(reference.getRow().toString(), reference.getColumnFamily().toString(), parsedKey.getField() + "_CONTENT_VIEW" + '\u0000' + view);

        return List.of(idKey, viewKey);
    }
}
