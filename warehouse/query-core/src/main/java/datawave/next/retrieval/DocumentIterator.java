package datawave.next.retrieval;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
import org.apache.accumulo.core.iteratorsImpl.system.IterationInterruptedException;
import org.apache.accumulo.tserver.tablet.TabletClosedException;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.TreeMultimap;
import com.google.protobuf.InvalidProtocolBufferException;

import datawave.ingest.protobuf.TermWeight;
import datawave.ingest.protobuf.TermWeightPosition;
import datawave.query.Constants;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Content;
import datawave.query.attributes.Document;
import datawave.query.attributes.DocumentKey;
import datawave.query.function.serializer.KryoDocumentSerializer;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.functions.TermFrequencyList;
import datawave.query.postprocessing.tf.TermOffsetMap;
import datawave.query.util.Tuple3;

/**
 * An iterator that retrieves documents from the shard table.
 */
public class DocumentIterator extends DocumentIteratorOptions implements SortedKeyValueIterator<Key,Value> {

    private static final Logger log = LoggerFactory.getLogger(DocumentIterator.class);

    private Key tk = null;
    private Value tv = null;

    private Range range = null;
    private Collection<ByteSequence> columnFamilies = null;
    private boolean inclusive = false;

    private final KryoDocumentSerializer serializer = new KryoDocumentSerializer();

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
        return tk != null;
    }

    @Override
    public void next() throws IOException {
        tk = null;
        tv = null;
        try {
            aggregateNextCandidate();
        } catch (Exception e) {
            handleException(e);
        }
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        this.range = range;
        this.columnFamilies = columnFamilies;
        this.inclusive = inclusive;

        checkForScanRebuild();
        aggregateNextCandidate();
    }

    /**
     * Aggregates the next candidate if one exists
     *
     * @throws IOException
     *             if something goes wrong
     */
    private void aggregateNextCandidate() throws IOException {
        tk = null;
        tv = null;
        // aggregate document
        if (!candidates.isEmpty()) {
            String nextCandidate = candidates.remove(0);
            aggregateCandidate(nextCandidate);
        }
    }

    /**
     * Aggregate the candidate document
     *
     * @param candidate
     *            the candidate
     * @throws IOException
     *             if something goes wrong
     */
    private void aggregateCandidate(String candidate) throws IOException {
        // must clear state between candidates
        context.clear();
        valueToAttributes.resetState();
        Preconditions.checkNotNull(candidate, "candidate was null");

        Range candidateRange = rangeForCandidate(candidate);
        source.seek(candidateRange, excludeCFs, inclusive);

        Key key = null;
        final Document d = new Document();

        while (source.hasTop()) {
            key = source.getTopKey();
            source.next();

            if (timeFilter != null && !timeFilter.contains(key.getTimestamp())) {
                // check for time stamp just in case
                continue;
            }

            eventKeyParser.parse(key);

            String fieldWithoutGrouping = JexlASTHelper.deconstructIdentifier(eventKeyParser.getField());
            if (includeFields != null && !includeFields.contains(fieldWithoutGrouping)) {
                // field was not present in inclusive filter
                continue;
            } else if (excludeFields != null && excludeFields.contains(fieldWithoutGrouping)) {
                // field matched the exclusive filter
                continue;
            }

            String field = JexlASTHelper.deconstructIdentifier(eventKeyParser.getField(), includeGroupingContext);
            Entry<Key,String> from = new AbstractMap.SimpleEntry<>(key, field);
            Iterable<Map.Entry<String,Attribute<?>>> elements = valueToAttributes.apply(from);
            elements.forEach(entry -> d.put(entry.getKey(), entry.getValue()));
        }

        // collect index only fragments
        collectIndexOnlyFragments(d, candidate);

        // collect term frequency fragments
        collectTermFrequencyFragments(d, candidate);

        // populate context, only pulling in the attributes required by the query
        d.visit(identifiers, context);

        boolean matched = evaluation.apply(new Tuple3<>(tk, d, context));
        if (!matched) {
            return;
        }

        if (d.size() > 0 && key != null) {
            Text cf = new Text(eventKeyParser.getDatatype() + "\0" + eventKeyParser.getUid());
            Key recordId = new Key(key.getRow(), cf, new Text(), key.getColumnVisibility(), key.getTimestamp());
            Attribute<?> attr = new DocumentKey(recordId, true);
            d.put(Document.DOCKEY_FIELD_NAME, attr);
        }

        if (d.size() > 0) {
            // reduce to keepers
            d.reduceToKeep();

            Map.Entry<Key,Document> entry = new AbstractMap.SimpleEntry<>(key, d);
            Map.Entry<Key,Value> result = serializer.apply(entry);
            tk = result.getKey();
            tv = result.getValue();
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

    private void collectIndexOnlyFragments(Document d, String candidate) {
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

    private void collectTermFrequencyFragments(Document d, String candidate) {
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

    /**
     * Handle an exception returned from seek or next. This will silently ignore IterationInterruptedException as that happens when the underlying iterator was
     * interrupted because the client is no longer listening.
     *
     * @param e
     *            the exception to handle
     * @throws IOException
     *             for read/write issues
     */
    private void handleException(Exception e) throws IOException {
        Throwable reason = e;

        // We need to pass IOException, IteratorInterruptedException, and TabletClosedExceptions up to the Tablet as they are
        // handled specially to ensure that the client will retry the scan elsewhere
        IOException ioe = null;
        IterationInterruptedException iie = null;
        TabletClosedException tce = null;
        if (reason instanceof IOException) {
            ioe = (IOException) reason;
        }
        if (reason instanceof IterationInterruptedException) {
            iie = (IterationInterruptedException) reason;
        }
        if (reason instanceof TabletClosedException) {
            tce = (TabletClosedException) reason;
        }

        int depth = 1;
        while (iie == null && reason.getCause() != null && reason.getCause() != reason && depth < 100) {
            reason = reason.getCause();
            if (reason instanceof IOException) {
                ioe = (IOException) reason;
            }
            if (reason instanceof IterationInterruptedException) {
                iie = (IterationInterruptedException) reason;
            }
            if (reason instanceof TabletClosedException) {
                tce = (TabletClosedException) reason;
            }
            depth++;
        }

        // NOTE: Only logging debug (for the most part) here because the Tablet/LookupTask will log the exception
        // as a WARN if we actually have a problem here
        if (iie != null) {
            log.debug("Query interrupted ", e);
            throw iie;
        } else if (tce != null) {
            log.debug("Query tablet closed ", e);
            throw tce;
        } else if (ioe != null) {
            log.debug("Query io exception ", e);
            throw ioe;
        } else {
            log.error("Failure for query ", e);
            throw new RuntimeException("Failure for query ", e);
        }
    }
}
