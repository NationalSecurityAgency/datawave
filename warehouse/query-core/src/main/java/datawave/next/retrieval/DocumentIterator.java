package datawave.next.retrieval;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.TreeMultimap;
import com.google.protobuf.InvalidProtocolBufferException;

import datawave.ingest.protobuf.TermWeight;
import datawave.ingest.protobuf.TermWeightPosition;
import datawave.query.Constants;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.AttributeFactory;
import datawave.query.attributes.Content;
import datawave.query.attributes.Document;
import datawave.query.attributes.DocumentKey;
import datawave.query.data.parsers.EventKey;
import datawave.query.data.parsers.FieldIndexKey;
import datawave.query.data.parsers.TermFrequencyKey;
import datawave.query.function.serializer.KryoDocumentSerializer;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.functions.TermFrequencyList;
import datawave.query.postprocessing.tf.TermOffsetMap;
import datawave.query.util.Tuple3;

/**
 * An iterator that retrieves documents from the shard table.
 */
public class DocumentIterator extends DocumentIteratorOptions implements SortedKeyValueIterator<Key,Value> {

    private Key tk = null;
    private Value tv = null;

    private Range range = null;
    private Collection<ByteSequence> columnFamilies = null;
    private boolean inclusive = false;

    private final KryoDocumentSerializer serializer = new KryoDocumentSerializer();
    private final List<Entry<Key,Value>> results = new LinkedList<>();

    public DocumentIterator() {}

    public DocumentIterator(DocumentIterator other, IteratorEnvironment env) {
        this.source = other.source.deepCopy(env);
        super.deepCopy(other);
    }

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        this.source = source;
        this.options = options;
        this.env = env;
        validateOptions(this.options);
    }

    @Override
    public boolean hasTop() {
        if (tk == null) {
            if (!results.isEmpty()) {
                Entry<Key,Value> entry = results.remove(0);
                tk = entry.getKey();
                tv = entry.getValue();
            }
        }
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
        this.columnFamilies = columnFamilies;
        this.inclusive = inclusive;

        checkForScanRebuild();

        // aggregate document
        AttributeFactory attributeFactory = new AttributeFactory(typeMetadata);
        EventKey parser = new EventKey();

        for (String candidate : candidates) {
            // must clear state between candidates
            context.clear();

            Range candidateRange = rangeForCandidate(candidate);
            source.seek(candidateRange, excludeCFs, false);

            Document d = new Document();
            Key key = null;
            while (source.hasTop()) {
                key = source.getTopKey();
                source.next();

                if (timeFilter != null && !timeFilter.contains(key.getTimestamp())) {
                    // check for time stamp just in case
                    continue;
                }

                parser.parse(key);

                String field = JexlASTHelper.deconstructIdentifier(parser.getField());
                if (includeFields != null && !includeFields.contains(field)) {
                    // field was not present in inclusive filter
                    continue;
                } else if (excludeFields != null && excludeFields.contains(field)) {
                    // field matched the exclusive filter
                    continue;
                }

                Attribute<?> attr = attributeFactory.create(field, parser.getValue(), key, parser.getDatatype(), true);
                d.put(field, attr);
            }

            // collect index only fragments
            collectIndexOnlyFragments(d, attributeFactory, candidate);

            // collect term frequency fragments
            collectTermFrequencyFragments(d, attributeFactory, candidate);

            // populate context, only pulling in the attributes required by the query
            d.visit(identifiers, context);

            boolean matched = evaluation.apply(new Tuple3<>(tk, d, context));
            if (!matched) {
                continue;
            }

            if (d.size() > 0 && key != null) {
                Text cf = new Text(parser.getDatatype() + "\0" + parser.getUid());
                Key recordId = new Key(key.getRow(), cf, new Text(), key.getColumnVisibility(), key.getTimestamp());
                Attribute<?> attr = new DocumentKey(recordId, false);
                d.put(Document.DOCKEY_FIELD_NAME, attr);
            }

            if (d.size() > 0) {
                Map.Entry<Key,Document> entry = new AbstractMap.SimpleEntry<>(key, d);
                Map.Entry<Key,Value> result = serializer.apply(entry);
                results.add(result);
            }
        }
    }

    /**
     * Check if the start key is exclusive. If
     */
    private void checkForScanRebuild() {
        if (!range.isStartKeyInclusive()) {
            String cf = range.getStartKey().getColumnFamily().toString();
            if (cf != null && !cf.isEmpty() && cf.indexOf('\u0000') != -1) {
                // reasonably sure that if the column family is not empty and a null byte exists then
                // we can use this to pair down the candidate list
                while (!candidates.isEmpty() && candidates.get(0).compareTo(cf) <= 0) {
                    candidates.remove(0);
                }
            }
        }
    }

    private Range rangeForCandidate(String candidate) {
        Key start = new Key(range.getStartKey().getRow(), new Text(candidate));
        Key stop = start.followingKey(PartialKey.ROW_COLFAM);
        return new Range(start, true, stop, false);
    }

    private void collectIndexOnlyFragments(Document d, AttributeFactory attributeFactory, String candidate) {
        if (indexOnlyFieldValues == null) {
            return;
        }

        List<Range> ranges = new ArrayList<>();
        for (String field : indexOnlyFieldValues.keySet()) {
            for (String value : indexOnlyFieldValues.get(field)) {
                Text cf = new Text("fi\0" + field);
                Text cq = new Text(value + "\0" + candidate);
                Key start = new Key(range.getStartKey().getRow(), cf, cq);
                Range range = new Range(start, true, start.followingKey(PartialKey.ROW_COLFAM), false);
                ranges.add(range);
            }
        }

        FieldIndexKey fiParser = new FieldIndexKey();

        for (Range range : ranges) {
            try {
                source.seek(range, Collections.emptySet(), false);
                while (source.hasTop()) {
                    Key key = source.getTopKey();
                    fiParser.parse(key);
                    Attribute<?> attribute = attributeFactory.create(fiParser.getField(), fiParser.getValue(), key, fiParser.getDatatype(), true, false);
                    // I don't remember why this is important, but set the flag
                    attribute.setFromIndex(true);
                    d.put(fiParser.getField(), attribute);

                    source.next();
                }
            } catch (IOException e) {
                throw new RuntimeException("Error collecting index only fragment", e);
            }
        }
    }

    private void collectTermFrequencyFragments(Document d, AttributeFactory attributeFactory, String candidate) {
        if (termFrequencyFieldValues == null) {
            return;
        }

        Text cf = new Text("tf");
        List<Range> ranges = new ArrayList<>();
        for (String field : termFrequencyFieldValues.keySet()) {
            for (String value : termFrequencyFieldValues.get(field)) {
                Text cq = new Text(candidate + "\0" + value + "\0" + field);
                Key start = new Key(range.getStartKey().getRow(), cf, cq);
                Range range = new Range(start, true, start.followingKey(PartialKey.ROW_COLFAM), false);
                ranges.add(range);
            }
        }

        TermFrequencyKey tfParser = new TermFrequencyKey();
        Map<String,TermFrequencyList> termOffsetMap = Maps.newHashMap();

        for (Range range : ranges) {
            try {
                source.seek(range, Collections.emptySet(), false);
                while (source.hasTop()) {
                    Key key = source.getTopKey();
                    tfParser.parse(key);

                    Content content = new Content(tfParser.getValue(), key, true);
                    d.put(tfParser.getField(), content);

                    TreeMultimap<TermFrequencyList.Zone,TermWeightPosition> offsets = parseTermFrequencyValue(source.getTopValue(), tfParser.getField(),
                                    candidate);

                    // First time looking up this term in a field
                    TermFrequencyList tfl = termOffsetMap.get(tfParser.getValue());
                    if (null == tfl) {
                        termOffsetMap.put(tfParser.getValue(), new TermFrequencyList(offsets));
                    } else {
                        // Merge in the offsets for the current field+term with all previous
                        // offsets from other fields in the same term
                        tfl.addOffsets(offsets);
                    }

                    source.next();
                }
            } catch (IOException e) {
                throw new RuntimeException("Error collecting index only fragment", e);
            }
        }

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, new TermOffsetMap(termOffsetMap));
    }

    private final TermWeightPosition.Builder position = new TermWeightPosition.Builder();

    private TreeMultimap<TermFrequencyList.Zone,TermWeightPosition> parseTermFrequencyValue(Value value, String field, String recordId) {
        TreeMultimap<TermFrequencyList.Zone,TermWeightPosition> offsets = TreeMultimap.create();
        try {
            TermWeight.Info twInfo = TermWeight.Info.parseFrom(value.get());
            TermFrequencyList.Zone twZone = new TermFrequencyList.Zone(field, true, recordId);

            for (int i = 0; i < twInfo.getTermOffsetCount(); i++) {
                position.setTermWeightOffsetInfo(twInfo, i);
                offsets.put(twZone, position.build());
                position.reset();
            }
        } catch (InvalidProtocolBufferException e) {
            return offsets;
        }
        return offsets;
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
        Preconditions.checkNotNull(source, "deepCopy() called with null source");
        return source.deepCopy(env);
    }
}
