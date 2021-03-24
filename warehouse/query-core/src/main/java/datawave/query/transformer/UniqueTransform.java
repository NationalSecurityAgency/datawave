package datawave.query.transformer;

import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Document;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.model.QueryModel;
import datawave.query.tables.ShardQueryLogic;
import datawave.util.StringUtils;
import datawave.webservice.query.logic.BaseQueryLogic;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * This iterator will filter documents based on uniqueness across a set of configured fields. Only the first instance of an event with a unique set of those
 * fields will be returned. This transform is thread safe.
 */
public class UniqueTransform extends DocumentTransform.DefaultDocumentTransform {
    
    private static final Logger log = Logger.getLogger(GroupingTransform.class);
    
    private BloomFilter<byte[]> bloom = null;
    private Set<String> fields;
    private Multimap<String,String> modelMapping;
    
    public UniqueTransform(Set<String> fields) {
        this.fields = deconstruct(fields);
        this.bloom = BloomFilter.create(new ByteFunnel(), 500000, 1e-15);
        if (log.isTraceEnabled())
            log.trace("unique fields: " + this.fields);
    }
    
    private Set<String> deconstruct(Collection<String> fields) {
        return fields.stream().map(field -> JexlASTHelper.deconstructIdentifier(field)).collect(Collectors.toSet());
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
    
    /**
     * Get a predicate that will apply this transform.
     * 
     * @return A unique transform predicate
     */
    public Predicate<Entry<Key,Document>> getUniquePredicate() {
        return input -> UniqueTransform.this.apply(input) != null;
    }
    
    /**
     * Apply uniqueness to a document.
     * 
     * @param keyDocumentEntry
     * @return The document if unique per the configured fields, null otherwise.
     */
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
    
    /**
     * Determine if a document is unique per the fields specified. If we have seen this set of fields and values before, then it is not unique.
     * 
     * @param document
     * @return
     * @throws IOException
     */
    private boolean isDuplicate(Document document) throws IOException {
        byte[] bytes = getBytes(document);
        synchronized (bloom) {
            if (bloom.mightContain(bytes)) {
                return true;
            }
            bloom.put(bytes);
        }
        return false;
    }
    
    /**
     * Get a sequence of bytes that uniquely identifies this document using the configured unique fields.
     * 
     * @param document
     * @return A document signature
     * @throws IOException
     *             if we failed to generate the byte array
     */
    private byte[] getBytes(Document document) throws IOException {
        // we need to pull the fields out of the document.
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream output = new DataOutputStream(bytes);
        List<FieldSet> fieldSets = getOrderedFieldSets(document);
        int count = 0;
        for (FieldSet fieldSet : fieldSets) {
            String separator = "f" + (count++) + ":";
            for (Map.Entry<String,String> entry : fieldSet.entrySet()) {
                output.writeChars(separator);
                output.writeChars(entry.getKey());
                output.writeChar('=');
                output.writeChars(entry.getValue());
                separator = ",";
            }
        }
        output.flush();
        return bytes.toByteArray();
    }
    
    /**
     * A field set if a sorted map that can be compared to other field sets. A field set represents a unique set of field/value pairs pulled from a document.
     * (package private for testing)
     */
    static class FieldSet extends TreeMap<String,String> implements Comparable<FieldSet> {
        
        @Override
        public int compareTo(FieldSet o) {
            Iterator<Map.Entry<String,String>> theseKeys = entrySet().iterator();
            Iterator<Map.Entry<String,String>> thoseKeys = o.entrySet().iterator();
            int comparison = 0;
            while (comparison == 0 && theseKeys.hasNext() && thoseKeys.hasNext()) {
                Map.Entry<String,String> thisKey = theseKeys.next();
                Map.Entry<String,String> thatKey = thoseKeys.next();
                comparison = thisKey.getKey().compareTo(thatKey.getKey());
                if (comparison == 0) {
                    comparison = thisKey.getValue().compareTo(thatKey.getValue());
                }
            }
            if (comparison == 0) {
                if (theseKeys.hasNext()) {
                    return 1;
                } else if (thoseKeys.hasNext()) {
                    return -1;
                }
            }
            return comparison;
        }
    }
    
    /**
     * Get a list of field sets that are sorted. (package private for testing)
     * 
     * @param document
     * @return the fields sets that uniquely identify this document
     */
    List<FieldSet> getOrderedFieldSets(Document document) {
        Set<Multimap<String,String>> fieldSets = getFieldSets(document);
        List<FieldSet> orderedFieldSets = new ArrayList<>(fieldSets.size());
        for (Multimap<String,String> fieldSet : fieldSets) {
            FieldSet orderedFieldSet = new FieldSet();
            for (String field : fieldSet.keySet()) {
                List<String> values = new ArrayList<>(fieldSet.get(field));
                Collections.sort(values);
                String value = Joiner.on(',').join(values);
                orderedFieldSet.put(field, value);
            }
            orderedFieldSets.add(orderedFieldSet);
        }
        Collections.sort(orderedFieldSets);
        return orderedFieldSets;
    }
    
    /**
     * This will return attributes from a document that uniquely identify this document for a set of fields. The attributes will be organized as set of
     * attribute sets.
     *
     * Definitions using example of "field.a.b.x = y" fieldname: "field" grouping context: "a.x" (the first part of the grouping is the group, the last part is
     * the instance) value: "y" The unique fields to be grouped are specified as a set of fieldnames.
     *
     * The attributes that uniquely identify this document will actually be composed of multiple sets of attributes where the grouping context is consistent
     * within each set.
     *
     * Example: Document: field1.a.1.0 = 1 field2.a.2.0 = 2 field1.a.1.1 = 3 field3.c.3.0 = 10 field3 = 11 field3 = 12 field4 = 100 ... unique fields = field1,
     * field2, field3, field4 Resulting groups: field1 = 1, field2 = 2, field3 = 10, field4 = 100 field1 = 1, field2 = 2, field3 = 11/12, field4 = 100 field1 =
     * 3, field2 = N/A, field3 = 10, field4 = 100 field1 = 3, field2 = N/A, field3 = 11/12, field4 = 100
     *
     */
    private Set<Multimap<String,String>> getFieldSets(Document document) {
        Map<String,Multimap<String,String>> mapGroupingContextToField = new HashMap<>();
        for (String documentField : document.getDictionary().keySet()) {
            String field = getUniqueField(documentField);
            if (field != null) {
                String groupingContext = getGroupingContext(documentField);
                Set<String> values = getValues(document.get(documentField));
                Multimap<String,String> groupedValues = mapGroupingContextToField.get(groupingContext);
                if (groupedValues == null) {
                    groupedValues = HashMultimap.create();
                    mapGroupingContextToField.put(groupingContext, groupedValues);
                }
                groupedValues.putAll(field, values);
            }
        }
        
        // combine grouped sets that are mutually exclusive
        Set<Multimap<String,String>> set1 = new HashSet<>(mapGroupingContextToField.values());
        Set<Multimap<String,String>> set2 = new HashSet<>(set1);
        Set<Multimap<String,String>> combined = multiply(set1, set2);
        
        // anything left in set1 can be considered part of the final results
        Set<Multimap<String,String>> results = set1;
        
        // now continue multiplying until nothing changes
        while (!combined.isEmpty()) {
            set1 = combined;
            set2 = new HashSet<>(mapGroupingContextToField.values());
            combined = multiply(set1, set2);
            results.addAll(set1);
        }
        return results;
    }
    
    /**
     * Multiply set1 and set2 by combining those in set1 are mutually exclusive with those in set2. Those left remaining in set1 are those entries that could
     * not be combined with anything in set2
     * 
     * @param set1
     * @param set1
     * @return the multiplication
     */
    private Set<Multimap<String,String>> multiply(Set<Multimap<String,String>> set1, Set<Multimap<String,String>> set2) {
        Set<Multimap<String,String>> combined = new HashSet<>();
        for (Iterator<Multimap<String,String>> it = set1.iterator(); it.hasNext();) {
            Multimap<String,String> entry = it.next();
            boolean remove = false;
            for (Multimap<String,String> other : set2) {
                if (entry == other) {
                    
                } else if (!intersects(entry.keySet(), other.keySet())) {
                    Multimap<String,String> combinedFields = HashMultimap.create(entry);
                    combinedFields.putAll(other);
                    combined.add(combinedFields);
                    remove = true;
                }
            }
            if (remove) {
                it.remove();
            }
        }
        return combined;
    }
    
    /**
     * Detemine if two sets of values intersect
     * 
     * @param set1
     * @param set2
     * @return true if they have a value in common, false otherwise
     */
    private boolean intersects(Set<String> set1, Set<String> set2) {
        for (String a : set1) {
            if (set2.contains(a)) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * Get the set of values for an attribute
     * 
     * @param attr
     * @return the set of values
     */
    private Set<String> getValues(Attribute<?> attr) {
        Set<String> values = new HashSet<>();
        if (attr instanceof Attributes) {
            for (Attribute<?> child : ((Attributes) attr).getAttributes()) {
                values.addAll(getValues(child));
            }
        } else {
            values.add(String.valueOf(attr.getData()));
        }
        return values;
    }
    
    /**
     * Get the base field name for a field (removed grouping context)
     *
     * @param documentField
     * @return the base field name
     */
    private String getBaseFieldname(String documentField) {
        int index = documentField.indexOf('.');
        if (index < 0) {
            return documentField;
        } else {
            return documentField.substring(0, index);
        }
    }
    
    /**
     * Get the grouping context for a field
     * 
     * @param documentField
     * @return the grouping context (first and last elements of the group). If no grouping context, then a unique group is given for this fieldname.
     */
    private String getGroupingContext(String documentField) {
        String[] parts = StringUtils.split(documentField, '.');
        if (parts.length == 1) {
            // if the field does not have a grouping context, then it is its own group
            return documentField + ".ungrouped";
        } else if (parts.length == 2) {
            return parts[1];
        } else {
            return parts[1] + '.' + parts[parts.length - 1];
        }
    }
    
    /**
     * Get the field that this documentField matches
     * 
     * @param documentField
     * @return the matching field
     */
    private String getUniqueField(String documentField) {
        String baseDocumentField = getBaseFieldname(documentField);
        for (String field : fields) {
            // Match should be case insensitive
            if (isMatchingField(baseDocumentField.toUpperCase(), field.toUpperCase())) {
                return field;
            }
        }
        return null;
    }
    
    /**
     * Determine if this document field is the same as this field, applying reverse model mappings if configured.
     * 
     * @param baseDocumentField
     * @param field
     * @return true of matching
     */
    private boolean isMatchingField(String baseDocumentField, String field) {
        if (field.equals(baseDocumentField)) {
            return true;
        }
        if (modelMapping != null && modelMapping.get(field).contains(baseDocumentField)) {
            return true;
        }
        return false;
    }
    
    public static class ByteFunnel implements Funnel<byte[]>, Serializable {
        
        private static final long serialVersionUID = -2126172579955897986L;
        
        @Override
        public void funnel(byte[] from, PrimitiveSink into) {
            into.putBytes(from);
        }
        
    }
    
}
