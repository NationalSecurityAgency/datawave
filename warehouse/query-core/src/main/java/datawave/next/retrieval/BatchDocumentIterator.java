package datawave.next.retrieval;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ParseException;
import org.apache.hadoop.io.Text;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

import datawave.query.attributes.Attribute;
import datawave.query.attributes.AttributeFactory;
import datawave.query.attributes.Document;
import datawave.query.attributes.DocumentKey;
import datawave.query.data.parsers.EventKey;
import datawave.query.function.JexlEvaluation;
import datawave.query.function.serializer.KryoDocumentSerializer;
import datawave.query.iterator.QueryOptions;
import datawave.query.jexl.DatawaveJexlContext;
import datawave.query.jexl.HitListArithmetic;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.util.Tuple3;
import datawave.query.util.TypeMetadata;

public class BatchDocumentIterator implements SortedKeyValueIterator<Key,Value> {

    public static final String CANDIDATES = "candidates";

    private final Set<String> candidates = new HashSet<>();
    private final List<Map.Entry<Key,Value>> results = new LinkedList<>();

    private final KryoDocumentSerializer serializer = new KryoDocumentSerializer();

    private String query;

    // below copied from DocumentIterator

    private Key tk = null;
    private Value tv = null;

    private SortedKeyValueIterator<Key,Value> source = null;
    private Map<String,String> options = null;
    private IteratorEnvironment env;

    private Range range = null;
    private Collection<ByteSequence> columnFamilies = null;
    private boolean inclusive = false;

    // exclusive scan with these column families exclude them
    protected final Collection<ByteSequence> excludeCFs = Lists.newArrayList(new ArrayByteSequence("tf"), new ArrayByteSequence("d"));

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        this.source = source;
        this.options = options;
        this.env = env;

        if (options.containsKey(CANDIDATES)) {
            String opt = options.get(CANDIDATES);
            candidates.addAll(Splitter.on(',').splitToList(opt));
        } else {
            throw new RuntimeException("BatchDocumentIterator requires CANDIDATES option");
        }

        if (options.containsKey(QueryOptions.QUERY)) {
            query = options.get(QueryOptions.QUERY);
        } else {
            throw new RuntimeException("DocumentIterator requires a query option");
        }
    }

    @Override
    public boolean hasTop() {
        if (tk == null) {
            if (!results.isEmpty()) {
                Map.Entry<Key,Value> entry = results.remove(0);
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

        // aggregate document
        TypeMetadata typeMetadata = new TypeMetadata();
        AttributeFactory attributeFactory = new AttributeFactory(typeMetadata);
        EventKey parser = new EventKey();

        DatawaveJexlContext context = new DatawaveJexlContext();

        JexlEvaluation evaluation = new JexlEvaluation(query, new HitListArithmetic());

        ASTJexlScript queryTree = parse(query);
        Set<String> identifiers = JexlASTHelper.getIdentifierNames(queryTree);

        for (String candidate : candidates) {
            Range candidateRange = rangeForCandidate(candidate);
            source.seek(candidateRange, excludeCFs, false);

            Key key = null;
            Document d = new Document();

            while (source.hasTop()) {
                key = source.getTopKey();
                parser.parse(key);
                Attribute<?> attr = attributeFactory.create(parser.getField(), parser.getValue(), key, true);
                d.put(parser.getField(), attr);
                source.next();
            }

            // do an evaluation just to see the effect
            context.clear();
            d.visit(identifiers, context);

            boolean matched = evaluation.apply(new Tuple3<>(tk, d, context));
            if (!matched) {
                return;
            }

            if (d.size() > 0 && key != null) {
                Text cf = new Text(parser.getDatatype() + "\0" + parser.getUid());
                Key recordId = new Key(key.getRow(), cf, new Text(), key.getColumnVisibility(), key.getTimestamp());
                Attribute<?> attr = new DocumentKey(recordId, false);
                d.put(Document.DOCKEY_FIELD_NAME, attr);

                // Content hitTerm = new Content("COLOR:red", recordId, true);
                // d.put("HIT_TERM", hitTerm);
                // TODO: hit terms or proper evaluation
            }

            if (d.size() > 0) {
                Map.Entry<Key,Document> entry = new AbstractMap.SimpleEntry<>(key, d);
                Map.Entry<Key,Value> result = serializer.apply(entry);
                results.add(result);
            }
        }
    }

    private ASTJexlScript parse(String query) {
        try {
            return JexlASTHelper.parseAndFlattenJexlQuery(query);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    private Range rangeForCandidate(String candidate) {
        Key start = new Key(range.getStartKey().getRow(), new Text(candidate));
        Key stop = start.followingKey(PartialKey.ROW_COLFAM);
        return new Range(start, true, stop, false);
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
