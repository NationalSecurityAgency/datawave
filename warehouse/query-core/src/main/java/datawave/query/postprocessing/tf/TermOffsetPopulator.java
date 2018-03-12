package datawave.query.postprocessing.tf;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import datawave.core.iterators.TermFrequencyIterator;
import datawave.core.iterators.TermFrequencyIterator.FieldValue;
import datawave.data.type.NoOpType;
import datawave.data.type.Type;
import datawave.ingest.protobuf.TermWeight;
import datawave.ingest.protobuf.TermWeightPosition;
import datawave.query.jexl.functions.TermFrequencyList;
import datawave.query.predicate.EventDataQueryFilter;
import datawave.query.Constants;
import datawave.query.attributes.Content;
import datawave.query.attributes.Document;
import datawave.query.jexl.functions.ContentFunctions;
import datawave.query.jexl.visitors.LiteralNodeSubsetVisitor;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.base.Predicate;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeMultimap;
import com.google.protobuf.InvalidProtocolBufferException;

public class TermOffsetPopulator {
    private static final Logger log = Logger.getLogger(TermOffsetPopulator.class);
    
    private static final Set<String> phraseFunctions;
    
    static {
        Set<String> _phraseFunctions = Sets.newHashSet();
        _phraseFunctions.add(ContentFunctions.CONTENT_WITHIN_FUNCTION_NAME);
        _phraseFunctions.add(ContentFunctions.CONTENT_ADJACENT_FUNCTION_NAME);
        _phraseFunctions.add(ContentFunctions.CONTENT_PHRASE_FUNCTION_NAME);
        phraseFunctions = Collections.unmodifiableSet(_phraseFunctions);
    }
    
    private Multimap<String,String> termFrequencyFieldValues;
    private EventDataQueryFilter evaluationFilter;
    private SortedKeyValueIterator<Key,Value> source;
    private Document document;
    private Set<String> contentExpansionFields;
    
    public TermOffsetPopulator(Multimap<String,String> termFrequencyFieldValues, Set<String> contentExpansionFields, EventDataQueryFilter evaluationFilter,
                    SortedKeyValueIterator<Key,Value> source) {
        this.termFrequencyFieldValues = termFrequencyFieldValues;
        this.contentExpansionFields = contentExpansionFields;
        this.source = source;
        this.evaluationFilter = evaluationFilter;
    }
    
    public Document document() {
        return document;
    }
    
    // merge two maps presumming both came from getContextMap()
    @SuppressWarnings("unchecked")
    public static Map<String,Object> mergeContextMap(Map<String,Object> map1, Map<String,Object> map2) {
        Map<String,Object> map = new HashMap<>();
        Map<String,TermFrequencyList> termOffsetMap = Maps.newHashMap();
        
        Map<String,TermFrequencyList> termOffsetMap1 = (Map<String,TermFrequencyList>) (map1.get(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME));
        Map<String,TermFrequencyList> termOffsetMap2 = (Map<String,TermFrequencyList>) (map2.get(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME));
        
        if (termOffsetMap1 == null) {
            if (termOffsetMap2 != null) {
                termOffsetMap.putAll(termOffsetMap2);
            }
        } else {
            termOffsetMap.putAll(termOffsetMap1);
            if (termOffsetMap2 != null) {
                for (Map.Entry<String,TermFrequencyList> entry : termOffsetMap2.entrySet()) {
                    String key = entry.getKey();
                    TermFrequencyList list1 = termOffsetMap.get(key);
                    TermFrequencyList list2 = entry.getValue();
                    if (list1 == null) {
                        termOffsetMap.put(key, list2);
                    } else if (list2 != null) {
                        termOffsetMap.put(key, TermFrequencyList.merge(list1, list2));
                    }
                }
            }
        }
        
        // Load the actual map into map that will be put into the JexlContext
        map.put(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffsetMap);
        
        return map;
    }
    
    protected Range getRange(Set<Key> keys) {
        // building a range from the begining of the term frequencies for the first datatype\0uid
        // to the end of the term frequencies for the last datatype\0uid
        List<String> dataTypeUids = new ArrayList<String>();
        Text row = null;
        for (Key key : keys) {
            row = key.getRow();
            dataTypeUids.add(key.getColumnFamily().toString());
        }
        Collections.sort(dataTypeUids);
        
        Key startKey = new Key(row, Constants.TERM_FREQUENCY_COLUMN_FAMILY, new Text(dataTypeUids.get(0)));
        Key endKey = new Key(row, Constants.TERM_FREQUENCY_COLUMN_FAMILY, new Text(dataTypeUids.get(dataTypeUids.size() - 1) + '\1'));
        return new Range(startKey, true, endKey, true);
    }
    
    public Map<String,Object> getContextMap(Key key) {
        return getContextMap(key, Collections.singleton(key));
    }
    
    public Map<String,Object> getContextMap(Key docKey, Set<Key> keys) {
        document = new Document();
        
        TermFrequencyIterator tfSource = new TermFrequencyIterator(termFrequencyFieldValues);
        
        Range range = getRange(keys);
        try {
            tfSource.init(source, null, null);
            tfSource.seek(getRange(keys), null, false);
        } catch (IOException e) {
            log.error("Seek to the range failed: " + range, e);
        }
        
        // set the document context on the filter
        if (evaluationFilter != null) {
            evaluationFilter.setDocumentKey(docKey);
        }
        
        Map<String,TermFrequencyList> termOffsetMap = Maps.newHashMap();
        TreeSet<Integer> timestampKeys = new TreeSet<>();
        
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
                
                for (int i = 0; i < twInfo.getTermOffsetCount(); i++) {
                    TermWeightPosition.Builder position = new TermWeightPosition.Builder();
                    timestampKeys.add(twInfo.getTermOffset(i));
                    position.setTermWeightOffsetInfo(twInfo, i);
                    offsets.put(twZone, position.build());
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
        map.put(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffsetMap);
        map.put(Constants.CONTENT_TERM_POSITION_KEY, timestampKeys);
        
        return map;
    }
    
    public static boolean isContentFunctionTerm(String functionName) {
        return phraseFunctions.contains(functionName);
    }
    
    /**
     * Finds all the content functions and returns a map indexed by function name to the function.
     */
    public static Multimap<String,Function> getContentFunctions(JexlNode query) {
        FunctionReferenceVisitor visitor = new FunctionReferenceVisitor();
        query.jjtAccept(visitor, null);
        
        Multimap<String,Function> functionsInNamespace = Multimaps.index(visitor.functions().get(ContentFunctions.CONTENT_FUNCTION_NAMESPACE),
                        new com.google.common.base.Function<Function,String>() {
                            public String apply(Function from) {
                                return from.name();
                            }
                        });
        
        return Multimaps.filterKeys(functionsInNamespace, new Predicate<String>() {
            public boolean apply(String input) {
                return isContentFunctionTerm(input);
            }
        });
    }
    
    /**
     * Get the list of content function fields to normalized values
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
        
        Set<String> normalizedTerms = new HashSet<String>();
        
        Collection<Class<? extends Type<?>>> dataTypesForZone = dataTypes.get(zone);
        if (dataTypesForZone.isEmpty()) {
            dataTypesForZone = Collections.<Class<? extends Type<?>>> singleton(NoOpType.class);
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
                    dataTypeInstance = dataTypeClass.newInstance();
                } catch (InstantiationException e) {
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
            Set<String> intersection = new HashSet<String>(contentValues);
            intersection.retainAll(queryValues);
            if (!intersection.isEmpty()) {
                termFrequencyFieldValues.putAll(key, intersection);
            }
        }
        return termFrequencyFieldValues;
    }
    
}
