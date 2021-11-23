package datawave.experimental.iterators;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.TreeSet;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.esotericsoftware.kryo.Kryo;

import datawave.query.attributes.Attribute;
import datawave.query.attributes.AttributeFactory;
import datawave.query.attributes.Document;
import datawave.query.attributes.DocumentKey;
import datawave.query.data.parsers.EventKey;
import datawave.query.function.serializer.KryoDocumentSerializer;
import datawave.query.util.TypeMetadata;
import datawave.webservice.common.logging.ThreadConfigurableLogger;

/**
 * Iterator that accepts one or more document keys as options and provides a rolling scan through the event column
 *
 */
public class DocumentScanIterator implements SortedKeyValueIterator<Key,Value> {

    private static final Logger log = ThreadConfigurableLogger.getLogger(DocumentScanIterator.class);
    public static final String UID_OPT = "uid.opt";
    public static final String TYPE_METADATA = "type.metadata";
    private TreeSet<String> uids;
    private String lastUid = null;

    private SortedKeyValueIterator<Key,Value> source;
    private IteratorEnvironment env;

    private Key tk;
    private Value tv;

    private Range range;
    private Collection<ByteSequence> columnFamilies;

    private final EventKey parser = new EventKey();
    private AttributeFactory attributeFactory;
    private String shard;
    private Document document;
    private final KryoDocumentSerializer serializer = new KryoDocumentSerializer();

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        this.source = source;
        this.env = env;

        if (options.containsKey(UID_OPT)) {
            this.uids = new TreeSet<>();
            this.uids.addAll(Arrays.asList(StringUtils.split(options.get(UID_OPT), ',')));
        }

        if (options.containsKey(TYPE_METADATA)) {
            String typeMetadataOpt = options.get(TYPE_METADATA);
            this.attributeFactory = new AttributeFactory(new TypeMetadata(typeMetadataOpt));
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
        boolean seeked = seekToNextDocument();
        if (seeked && source.hasTop()) {
            document = aggregateDocument();
            if (document != null) {
                tk = document.get("RECORD_ID").getMetadata();
                tv = new Value(serializer.serialize(document));
            }
        }
    }

    private Document aggregateDocument() throws IOException {
        Document d = null;
        Attribute<?> attr;
        Key key;

        if (source.hasTop()) {
            d = new Document();
        }

        while (source.hasTop()) {
            key = source.getTopKey();
            parser.parse(key);
            if (lastUid.equals(parser.getDatatype() + '\0' + parser.getUid())) {
                attr = attributeFactory.create(parser.getField(), parser.getValue(), key, true);
                d.put(parser.getField(), attr);
                source.next();
            } else {
                // found a key for a new document
                break;
            }
        }

        if (d != null) {
            d.put(Document.DOCKEY_FIELD_NAME, new DocumentKey(new Key(shard, lastUid), true));
        }

        return d;
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        this.range = range;
        this.columnFamilies = columnFamilies;
        this.shard = range.getStartKey().getRow().toString();

        // call to next will seek to the next document hit
        next();
    }

    private boolean seekToNextDocument() throws IOException {
        Range seekRange = buildNextSeekRange(range);
        if (seekRange != null) {
            source.seek(seekRange, columnFamilies, false);
            return true;
        }
        return false;
    }

    /**
     * Build the next seek range based on the current range.
     *
     * @param range
     *            the current range
     * @return a range built for the next document id, or null if no such id exists
     */
    private Range buildNextSeekRange(Range range) {
        if (lastUid == null || !uids.isEmpty()) {
            lastUid = uids.pollFirst();
            Key nextStart = new Key(range.getStartKey().getRow(), new Text(lastUid));
            return new Range(nextStart, true, range.getEndKey(), range.isEndKeyInclusive());
        }

        return null;
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
        DocumentScanIterator iter = new DocumentScanIterator();
        iter.env = env;
        iter.columnFamilies = this.columnFamilies;
        iter.lastUid = this.lastUid;
        iter.uids = this.uids;
        return iter;
    }
}
