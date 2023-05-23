package datawave.query.postprocessing.tf;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeMultimap;
import com.google.protobuf.InvalidProtocolBufferException;
import datawave.core.iterators.TermFrequencyIterator;
import datawave.data.type.NoOpType;
import datawave.data.type.Type;
import datawave.ingest.protobuf.TermWeight;
import datawave.ingest.protobuf.TermWeightPosition;
import datawave.query.Constants;
import datawave.query.attributes.Content;
import datawave.query.attributes.Document;
import datawave.query.jexl.functions.TermFrequencyList;
import datawave.query.jexl.visitors.LiteralNodeSubsetVisitor;
import datawave.query.predicate.EventDataQueryFilter;
import datawave.util.StringUtils;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;

import static datawave.query.Constants.TERM_FREQUENCY_COLUMN_FAMILY;
import static datawave.query.jexl.functions.ContentFunctions.CONTENT_ADJACENT_FUNCTION_NAME;
import static datawave.query.jexl.functions.ContentFunctions.CONTENT_FUNCTION_NAMESPACE;
import static datawave.query.jexl.functions.ContentFunctions.CONTENT_PHRASE_FUNCTION_NAME;
import static datawave.query.jexl.functions.ContentFunctions.CONTENT_SCORED_PHRASE_FUNCTION_NAME;
import static datawave.query.jexl.functions.ContentFunctions.CONTENT_WITHIN_FUNCTION_NAME;

public class TermOffsetPopulator {
    private static final Logger log = Logger.getLogger(TermOffsetPopulator.class);
    
    private static final Set<String> phraseFunctions;
    
    static {
        Set<String> _phraseFunctions = Sets.newHashSet();
        _phraseFunctions.add(CONTENT_WITHIN_FUNCTION_NAME);
        _phraseFunctions.add(CONTENT_ADJACENT_FUNCTION_NAME);
        _phraseFunctions.add(CONTENT_PHRASE_FUNCTION_NAME);
        _phraseFunctions.add(CONTENT_SCORED_PHRASE_FUNCTION_NAME);
        phraseFunctions = Collections.unmodifiableSet(_phraseFunctions);
    }
    
    private Multimap<String,String> termFrequencyFieldValues;
    private EventDataQueryFilter evaluationFilter;
    private SortedKeyValueIterator<Key,Value> source;
    private Document document;
    private Set<String> contentExpansionFields;
    
    private final TermFrequencyConfig config;
    
    public TermOffsetPopulator(Multimap<String,String> termFrequencyFieldValues, TermFrequencyConfig config) {
        this.termFrequencyFieldValues = termFrequencyFieldValues;
        this.config = config;
        this.contentExpansionFields = this.config.getContentExpansionFields();
        this.source = this.config.getSource();
        this.evaluationFilter = this.config.getEvaluationFilter();
    }
    
    public Document document() {
        return document;
    }
    
    public Multimap<String,String> getTermFrequencyFieldValues() {
        return termFrequencyFieldValues;
    }
    
    protected Range getRange(Set<Key> keys) {
        // building a range from the beginning of the term frequencies for the first datatype\0uid
        // to the end of the term frequencies for the last datatype\0uid
        List<String> dataTypeUids = new ArrayList<>();
        Text row = null;
        for (Key key : keys) {
            row = key.getRow();
            dataTypeUids.add(key.getColumnFamily().toString());
        }
        Collections.sort(dataTypeUids);
        
        Key startKey = new Key(row, TERM_FREQUENCY_COLUMN_FAMILY, new Text(dataTypeUids.get(0)));
        Key endKey = new Key(row, TERM_FREQUENCY_COLUMN_FAMILY, new Text(dataTypeUids.get(dataTypeUids.size() - 1) + '\1'));
        return new Range(startKey, true, endKey, true);
    }
    
    public Map<String,Object> getContextMap(Key key) {
        return getContextMap(key, Collections.singleton(key), null);
    }
    
    /**
     * Build TermOffset map for use in JexlEvaluation
     *
     * @param docKey
     *            key that maps to a document
     * @param keys
     *            set of keys that map to hits on tf fields
     * @param fields
     *            set of fields to remove from the search space
     * @return TermOffset map
     */
    public Map<String,Object> getContextMap(Key docKey, Set<Key> keys, Set<String> fields) {

        if (keys.isEmpty()) {
            return Collections.emptyMap();
        }

        document = new Document();
        
        TermFrequencyIterator tfSource;
        // Do not prune if no fields exist or if the tf fields would prune to nothing. TODO skip tf entirely if this would prune to zero
        if (fields == null || fields.isEmpty() || fields.size() == termFrequencyFieldValues.keySet().size()) {
            tfSource = new TermFrequencyIterator(termFrequencyFieldValues, keys);
        } else {
            // There are fields to remove, reduce the search space and continue
            Multimap<String,String> tfFVs = HashMultimap.create(termFrequencyFieldValues);
            fields.forEach(tfFVs::removeAll);
            tfSource = new TermFrequencyIterator(tfFVs, keys);
            
            if (tfFVs.size() == 0) {
                log.error("Created a TFIter with no field values. Orig fields: " + termFrequencyFieldValues.keySet() + " fields to remove: " + fields);
            }
        }

        
        Range range = getRange(keys);
        try {
            tfSource.init(source, null, null);
            tfSource.seek(range, null, false);
        } catch (IOException e) {
            log.error("Seek to the range failed: " + range, e);
        }
        
        // set the document context on the filter
        if (evaluationFilter != null) {
            evaluationFilter.startNewDocument(docKey);
        }
        
        Map<String,TermFrequencyList> termOffsetMap = Maps.newHashMap();
        
        while (tfSource.hasTop()) {
            Key key = tfSource.getTopKey();
            FieldValue fv = FieldValue.getFieldValue(key);
            
            // add the zone and term to our internal document
            Content attr = new Content(fv.getValue(), source.getTopKey(), evaluationFilter == null || evaluationFilter.keep(key));
            
            // no need to apply the evaluation filter here as the TermFrequencyIterator above is already doing more filtering than we can do here.
            // So this filter is simply extraneous. However if the an EventDataQueryFilter implementation gets smarter somehow, then it can be added back in
            // here.
            // For example the AncestorQueryLogic may require this....
            // if (evaluationFilter == null || evaluationFilter.apply(Maps.immutableEntry(key, StringUtils.EMPTY_STRING))) {
            
            this.document.put(fv.getField(), attr);
            
            TreeMultimap<TermFrequencyList.Zone,TermWeightPosition> offsets = TreeMultimap.create();
            try {
                TermWeight.Info twInfo = TermWeight.Info.parseFrom(tfSource.getTopValue().get());
                
                // if no content expansion fields then assume every field is permitted for unfielded content functions
                TermFrequencyList.Zone twZone = new TermFrequencyList.Zone(fv.getField(),
                                (contentExpansionFields == null || contentExpansionFields.isEmpty() || contentExpansionFields.contains(fv.getField())),
                                TermFrequencyList.getEventId(key));
                
                TermWeightPosition.Builder position = new TermWeightPosition.Builder();
                for (int i = 0; i < twInfo.getTermOffsetCount(); i++) {
                    position.setTermWeightOffsetInfo(twInfo, i);
                    offsets.put(twZone, position.build());
                    position.reset();
                }
                
            } catch (InvalidProtocolBufferException e) {
                log.error("Could not deserialize TermWeight protocol buffer for: " + source.getTopKey());
                
                return null;
            }
            
            // First time looking up this term in a field
            TermFrequencyList tfl = termOffsetMap.get(fv.getValue());
            if (null == tfl) {
                termOffsetMap.put(fv.getValue(), new TermFrequencyList(offsets));
            } else {
                // Merge in the offsets for the current field+term with all previous
                // offsets from other fields in the same term
                tfl.addOffsets(offsets);
            }
            
            try {
                tfSource.next();
            } catch (IOException ioe) {
                log.error("Next failed: " + range, ioe);
                break;
            }
        }
        
        // Load the actual map into map that will be put into the JexlContext
        Map<String,Object> map = new HashMap<>();
        map.put(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, new TermOffsetMap(termOffsetMap));
        
        return map;
    }
    
    /**
     * Build a range from the search space.
     *
     * @param row
     *            the shard
     * @param searchSpace
     *            a sorted set of TF column qualifiers
     * @return a range encompassing the full search space
     */
    public static Range getRangeFromSearchSpace(Text row, SortedSet<Text> searchSpace) {
        Key start = new Key(row, TERM_FREQUENCY_COLUMN_FAMILY, searchSpace.first());
        Key end = new Key(row, TERM_FREQUENCY_COLUMN_FAMILY, searchSpace.last());
        return new Range(start, true, end.followingKey(PartialKey.ROW_COLFAM_COLQUAL), false);
    }
    
    public static boolean isContentFunctionTerm(String functionName) {
        return phraseFunctions.contains(functionName);
    }
    
    /**
     * Finds all the content functions and returns a map indexed by function name to the function.
     * 
     * @param node
     *            a JexlNode
     * @return a map indexed by function name to the function
     */
    public static Multimap<String,Function> getContentFunctions(JexlNode node) {
        return FunctionReferenceVisitor.functions(node, Collections.singleton(CONTENT_FUNCTION_NAMESPACE));
    }
    
    /**
     * Get the list of content function fields to normalized values
     * 
     * @param contentExpansionFields
     *            set of content expansion fields
     * @param dataTypes
     *            map of datatypes
     * @param functions
     *            map of functions
     * @return list of content function fields to normalized values
     */
    public static Multimap<String,String> getContentFieldValues(Set<String> contentExpansionFields, Multimap<String,Class<? extends Type<?>>> dataTypes,
                    Multimap<String,Function> functions) {
        
        Multimap<String,String> contentFieldValues = HashMultimap.create();
        Map<Class<? extends Type<?>>,Type<?>> dataTypeCacheMap = Maps.newHashMap();
        
        for (Entry<String,Function> namespacedF : functions.entries()) {
            // Get the function arguments
            ContentFunctionArguments args;
            try {
                args = new ContentFunctionArguments(namespacedF.getValue());
            } catch (ParseException e) {
                log.warn("Could not parse the content function", e);
                return null;
            }
            
            // We can't construct a phrase with less than 2 terms
            if (args.terms() == null || args.terms().size() < 2) {
                if (log.isTraceEnabled()) {
                    log.trace("Received less than two terms. terms: " + args.terms());
                }
                return null;
            }
            
            Set<String> zones = new HashSet<>();
            if (args.zone() != null && !args.zone().isEmpty()) {
                zones.addAll(args.zone());
            } else {
                zones.addAll(contentExpansionFields);
            }
            
            // Add the terms/zones to the map of fields to look up
            for (String term : args.terms()) {
                for (String zone : zones) {
                    contentFieldValues.putAll(zone, getNormalizedTerms(term, zone, dataTypes, dataTypeCacheMap));
                }
            }
        }
        
        return contentFieldValues;
    }
    
    private static Set<String> getNormalizedTerms(String originalTerm, String zone, Multimap<String,Class<? extends Type<?>>> dataTypes,
                    Map<Class<? extends Type<?>>,Type<?>> dataTypeCacheMap) {
        
        Set<String> normalizedTerms = new HashSet<>();
        
        Collection<Class<? extends Type<?>>> dataTypesForZone = dataTypes.get(zone);
        if (dataTypesForZone.isEmpty()) {
            dataTypesForZone = Collections.singleton(NoOpType.class);
        }
        
        // Get the dataType version of the term for each dataType set up on
        // the zone (field)
        for (Class<? extends Type<?>> dataTypeClass : dataTypesForZone) {
            Type<?> dataTypeInstance = null;
            
            // Get an instance of the dataType
            if (dataTypeCacheMap.containsKey(dataTypeClass) && dataTypeCacheMap.get(dataTypeClass) != null) {
                dataTypeInstance = dataTypeCacheMap.get(dataTypeClass);
            } else {
                // If we don't have an instance of the dataType, make one and put it
                // in the cache
                try {
                    dataTypeInstance = dataTypeClass.getDeclaredConstructor().newInstance();
                } catch (InstantiationException | NoSuchMethodException | InvocationTargetException e) {
                    log.error("Could not instantiate dataType class: " + dataTypeClass);
                    continue;
                } catch (IllegalAccessException e) {
                    log.error("IllegalAccessException when trying to create dataType: " + dataTypeClass);
                    continue;
                }
                
                dataTypeCacheMap.put(dataTypeClass, dataTypeInstance);
            }
            
            // Add the normalized term to the list
            try {
                normalizedTerms.add(dataTypeInstance.normalize(originalTerm));
            } catch (Exception e) {
                log.error("Unable to normalize " + zone + " = " + originalTerm + " using " + dataTypeClass + ", using original value");
                normalizedTerms.add(originalTerm);
            }
        }
        
        // Return the list of terms
        return normalizedTerms;
    }
    
    /**
     * A method to get the list of fields and values for which term frequencies need to be gathered. ASSUMPTION: The query planner (@see DefaultQueryPlanner)
     * has: 1) expanded the content functions into the query 2) the values in the query have already been normalized appropriately 3) the query has been reduced
     * to those values actually in the index The query is scraped for content functions from which a list of zones to normalized values is determined (the
     * contentExpansionFields are used for unfielded content functions). The list of fields to values as a subset of the term frequency fields is gathered. The
     * intersection of those two sets are returned.
     * 
     * @param dataTypes
     *            map of datatypes
     * @param contentExpansionFields
     *            set of content expansion fields
     * @param query
     *            the query script
     * @param termFrequencyFields
     *            set of term frequency fields
     * @return list of fields and values for which term frequencies need to be gathered
     */
    public static Multimap<String,String> getTermFrequencyFieldValues(ASTJexlScript query, Set<String> contentExpansionFields, Set<String> termFrequencyFields,
                    Multimap<String,Class<? extends Type<?>>> dataTypes) {
        
        Multimap<String,Function> functions = TermOffsetPopulator.getContentFunctions(query);
        
        if (!functions.isEmpty()) {
            Multimap<String,String> queryFieldValues = LiteralNodeSubsetVisitor.getLiterals(termFrequencyFields, query);
            if (!queryFieldValues.isEmpty()) {
                // if the content expansion fields is empty, then the term frequency field set will be used instead
                if (contentExpansionFields == null || contentExpansionFields.isEmpty()) {
                    contentExpansionFields = termFrequencyFields;
                }
                return getTermFrequencyFieldValues(functions, contentExpansionFields, queryFieldValues, dataTypes);
            }
        }
        return HashMultimap.create();
    }
    
    public static Multimap<String,String> getTermFrequencyFieldValues(Multimap<String,Function> functions, Set<String> contentExpansionFields,
                    Multimap<String,String> queryFieldValues, Multimap<String,Class<? extends Type<?>>> dataTypes) {
        // get the intersection of the content expansion fields (or term frequency fields) and those that are in the content functions
        Multimap<String,String> contentFieldValues = getContentFieldValues(contentExpansionFields, dataTypes, functions);
        
        // now get the intersection of the content function values and those literals in the query:
        Multimap<String,String> termFrequencyFieldValues = HashMultimap.create();
        for (String key : contentFieldValues.keySet()) {
            Collection<String> contentValues = contentFieldValues.get(key);
            Collection<String> queryValues = queryFieldValues.get(key);
            Set<String> intersection = new HashSet<>(contentValues);
            intersection.retainAll(queryValues);
            if (!intersection.isEmpty()) {
                termFrequencyFieldValues.putAll(key, intersection);
            }
        }
        return termFrequencyFieldValues;
    }
    
    /**
     * A field name and value which is sorted on {@code <value>\0<name>}
     */
    public static class FieldValue implements Comparable<FieldValue> {
        private int nullOffset;
        private String valueField;
        
        public FieldValue(String field, String value) {
            this.nullOffset = value.length();
            this.valueField = value + '\0' + field;
        }
        
        /**
         * A distance between this field value and another. Here we want a distance that correlates with the number of keys between here and there for the same.
         * Essentially we want the inverse of the number of bytes that match. document.
         *
         * @param fv
         *            a field value
         * @return a distance between here and there (negative means there is before here)
         */
        public double distance(FieldValue fv) {
            byte[] s1 = getValueField().getBytes();
            byte[] s2 = fv.getValueField().getBytes();
            int len = Math.min(s1.length, s2.length);
            
            int matches = 0;
            int lastCharDiff = 0;
            
            for (int i = 0; i <= len; i++) {
                lastCharDiff = getValue(s2, i) - getValue(s1, i);
                if (lastCharDiff == 0) {
                    matches++;
                } else {
                    break;
                }
            }
            
            return Math.copySign(1.0d / (matches + 1), lastCharDiff);
        }
        
        private int getValue(byte[] bytes, int index) {
            if (index >= bytes.length) {
                return 0;
            } else {
                return bytes[index];
            }
        }
        
        public String getValueField() {
            return valueField;
        }
        
        public String getField() {
            return valueField.substring(nullOffset + 1);
        }
        
        public String getValue() {
            return valueField.substring(0, nullOffset);
        }
        
        @Override
        public int compareTo(FieldValue o) {
            return valueField.compareTo(o.valueField);
        }
        
        @Override
        public int hashCode() {
            return valueField.hashCode();
        }
        
        @Override
        public boolean equals(Object obj) {
            if (obj instanceof FieldValue) {
                return valueField.equals(((FieldValue) obj).valueField);
            }
            return false;
        }
        
        @Override
        public String toString() {
            return getField() + " -> " + getValue();
        }
        
        public static FieldValue getFieldValue(Key key) {
            return getFieldValue(key.getColumnQualifier());
        }
        
        public static FieldValue getFieldValue(Text cqText) {
            if (cqText == null) {
                return null;
            }
            return getFieldValue(cqText.toString());
        }
        
        public static FieldValue getFieldValue(String cq) {
            if (cq == null) {
                return null;
            }
            
            // pull apart the cq
            String[] cqParts = StringUtils.split(cq, '\0');
            
            // if we do not even have the first datatype\0uid, then lets find it
            if (cqParts.length <= 2) {
                return null;
            }
            
            // get the value and field
            String value = "";
            String field = "";
            if (cqParts.length >= 4) {
                field = cqParts[cqParts.length - 1];
                value = cqParts[2];
                // in case the value had null characters therein
                for (int i = 3; i < (cqParts.length - 1); i++) {
                    value = value + '\0' + cqParts[i];
                }
            } else if (cqParts.length == 3) {
                value = cqParts[2];
            }
            
            return new FieldValue(field, value);
        }
        
    }
}
