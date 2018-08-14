package datawave.query.transformer;

import com.google.common.base.Predicate;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;
import datawave.marking.MarkingFunctions;
import datawave.query.model.QueryModel;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Document;
import datawave.query.tables.ShardQueryLogic;
import datawave.webservice.query.Query;
import datawave.webservice.query.logic.BaseQueryLogic;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

/**
 * This is a iterator that will filter documents base on a uniqueness across a set of configured fields. Only the first instance of an event with a unique set
 * of those fields will be returned.
 */
public class UniqueTransform extends DocumentTransform.DefaultDocumentTransform {
    
    private static final Logger log = Logger.getLogger(GroupingTransform.class);
    
    private BloomFilter<byte[]> bloom = null;
    private HashSet<ByteSequence> seen;
    private Set<String> fields;
    private Multimap<String,String> modelMapping;
    private final boolean DEBUG = false;
    
    public UniqueTransform(Set<String> fields) {
        this.fields = fields;
        this.bloom = BloomFilter.create(new ByteFunnel(), 500000, 1e-15);
        if (DEBUG) {
            this.seen = new HashSet<ByteSequence>();
        }
        if (log.isTraceEnabled())
            log.trace("unique fields: " + this.fields);
    }
    
    /**
     * If passing the logic in, then the model being used by the logic then capture the reverse field mapping
     *
     * @param logic
     * @param fields
     */
    public UniqueTransform(BaseQueryLogic<Entry<Key,Value>> logic, Set<String> fields) {
        this(fields);
        QueryModel model = ((ShardQueryLogic) logic).getQueryModel();
        if (model != null) {
            modelMapping = HashMultimap.create();
            // reverse the reverse query mapping which will give us a mapping from the final field name to the original field name(s)
            for (Map.Entry<String,String> entry : model.getReverseQueryMapping().entrySet()) {
                modelMapping.put(entry.getValue(), entry.getKey());
            }
        }
    }
    
    public Predicate<Entry<Key,Document>> getUniquePredicate() {
        return new Predicate<Entry<Key,Document>>() {
            @Override
            public boolean apply(@Nullable Entry<Key,Document> input) {
                return UniqueTransform.this.apply(input) != null;
            }
        };
    }
    
    @Nullable
    @Override
    public Entry<Key,Document> apply(@Nullable Entry<Key,Document> keyDocumentEntry) {
        if (keyDocumentEntry != null) {
            try {
                if (isDuplicate(keyDocumentEntry.getValue())) {
                    keyDocumentEntry = null;
                }
            } catch (IOException ioe) {
                log.error("Failed to convert document to bytes.  Returning document as unique.", ioe);
            }
        }
        return keyDocumentEntry;
    }
    
    private byte[] getBytes(Document document) throws IOException {
        // we need to pull the fields out of the document.
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream output = new DataOutputStream(bytes);
        for (String field : fields) {
            output.writeChars(field);
            List<Attribute<?>> attrs = getFields(document, field);
            if (!attrs.isEmpty()) {
                for (Attribute<?> attr : attrs) {
                    attr.write(output, true);
                }
            }
        }
        output.flush();
        return bytes.toByteArray();
    }
    
    private List<Attribute<?>> getFields(Document document, String field) {
        List<Attribute<?>> attrs = new ArrayList<>();
        List<String> fields = new ArrayList<>();
        if (modelMapping != null) {
            fields.addAll(modelMapping.get(field));
        }
        // always include the original field name in case it exists in that form within the DB
        fields.add(field);
        Collections.sort(fields);
        for (String finalField : fields) {
            Attribute<?> attr = document.get(finalField);
            if (attr != null) {
                attrs.add(attr);
            }
        }
        return attrs;
    }
    
    private boolean isDuplicate(Document document) throws IOException {
        byte[] bytes = getBytes(document);
        ByteSequence byteSeq = new ArrayByteSequence(bytes);
        if (bloom.mightContain(bytes)) {
            if (DEBUG && !seen.contains(byteSeq)) {
                throw new IllegalStateException("This event is 1 in 1Q!");
            } else {
                return true;
            }
        }
        bloom.put(bytes);
        if (DEBUG) {
            seen.add(byteSeq);
        }
        return false;
    }
    
    public static class ByteFunnel implements Funnel<byte[]>, Serializable {
        
        private static final long serialVersionUID = -2126172579955897986L;
        
        public ByteFunnel() {}
        
        @Override
        public void funnel(byte[] from, PrimitiveSink into) {
            into.putBytes(from);
        }
        
    }
    
}
