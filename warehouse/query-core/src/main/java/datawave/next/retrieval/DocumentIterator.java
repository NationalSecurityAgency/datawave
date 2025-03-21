package datawave.next.retrieval;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.jexl3.JexlScript;
import org.apache.hadoop.io.Text;
import org.springframework.context.support.GenericGroovyApplicationContext;

import com.google.common.base.Preconditions;

import datawave.query.attributes.Attribute;
import datawave.query.attributes.AttributeFactory;
import datawave.query.attributes.Document;
import datawave.query.attributes.DocumentKey;
import datawave.query.data.parsers.EventKey;
import datawave.query.function.serializer.KryoDocumentSerializer;
import datawave.query.iterator.QueryOptions;
import datawave.query.jexl.ArithmeticJexlEngines;
import datawave.query.jexl.DatawaveArithmetic;
import datawave.query.jexl.DatawaveJexlContext;
import datawave.query.jexl.DatawaveJexlEngine;
import datawave.query.jexl.HitListArithmetic;
import datawave.query.util.TypeMetadata;

/**
 * An extremely simple iterator that retrieves a document.
 */
public class DocumentIterator implements SortedKeyValueIterator<Key,Value> {

    private Key tk = null;
    private Value tv = null;

    private SortedKeyValueIterator<Key,Value> source = null;
    private Map<String,String> options = null;
    private IteratorEnvironment env;

    private Range range = null;
    private Collection<ByteSequence> columnFamilies = null;
    private boolean inclusive = false;

    // new vars
    private String query;

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        this.source = source;
        this.options = options;
        this.env = env;

        if (options.containsKey(QueryOptions.QUERY)) {
            query = options.get(QueryOptions.QUERY);
        } else {
            throw new RuntimeException("DocumentIterator requires a query option");
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
        this.columnFamilies = columnFamilies;
        this.inclusive = inclusive;

        source.seek(range, columnFamilies, inclusive);

        // aggregate document
        Document d = new Document();
        TypeMetadata typeMetadata = new TypeMetadata();
        AttributeFactory attributeFactory = new AttributeFactory(typeMetadata);
        EventKey parser = new EventKey();
        Key key = null;
        while (source.hasTop()) {
            key = source.getTopKey();
            tk = key;
            parser.parse(key);
            Attribute<?> attr = attributeFactory.create(parser.getField(), parser.getValue(), key, true);
            d.put(parser.getField(), attr);
            source.next();
        }

        // do an evaluation just to see the effect
        HitListArithmetic arithmetic = new HitListArithmetic();
        DatawaveJexlEngine engine = ArithmeticJexlEngines.getEngine(arithmetic);
        JexlScript script = engine.createScript(query);

        DatawaveJexlContext context = new DatawaveJexlContext();
        d.visit(Set.of("COLOR"), context);

        boolean result = (boolean) script.execute(context);

        if (d.size() > 0 && key != null) {
            Text cf = new Text(parser.getDatatype() + "\0" + parser.getUid());
            Key recordId = new Key(key.getRow(), cf, new Text(), key.getColumnVisibility(), key.getTimestamp());
            Attribute<?> attr = new DocumentKey(recordId, false);
            d.put(Document.DOCKEY_FIELD_NAME, attr);
        }

        if (d.size() > 0) {
            Map.Entry<Key,Document> entry = new AbstractMap.SimpleEntry<>(key, d);
            Map.Entry<Key,Value> e2 = new KryoDocumentSerializer().apply(entry);
            tk = e2.getKey();
            tv = e2.getValue();
        }
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
