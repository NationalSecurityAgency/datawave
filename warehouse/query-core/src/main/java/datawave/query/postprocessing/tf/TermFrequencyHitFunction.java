package datawave.query.postprocessing.tf;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import datawave.data.type.Type;
import datawave.query.Constants;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Document;
import datawave.query.attributes.PreNormalizedAttribute;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTDelayedPredicate;
import org.apache.commons.jexl2.parser.ASTIdentifier;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ASTNumberLiteral;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTUnaryMinusNode;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * Function that determines the search space for a query's content functions given a document.
 *
 * The search space is a set of fully qualified TermFrequency column qualifiers
 */
public class TermFrequencyHitFunction {
    
    private final static Logger log = Logger.getLogger(TermFrequencyHitFunction.class);
    
    // The fields to get from the document
    private final Set<String> functionFields = new HashSet<>();
    
    // Track positive/negative field value pairs
    private final TreeSet<String> positiveSearchSpace = new TreeSet<>();
    private final TreeSet<String> negativeSearchSpace = new TreeSet<>();
    
    // Track individual sub queries defined by field-values
    private final Set<SubQuery> positiveSubQueries = new HashSet<>();
    private final Set<SubQuery> negativeSubQueries = new HashSet<>();
    
    // If a content function does not have a field in it's argument list, fall back to this map.
    private final Multimap<String,String> tfFVs;
    
    // If a content function is negated, those field values will need to be fetched
    private boolean hasNegatedFunctions;
    
    // Document built from any negated content function terms that hit
    private Document negatedDocument;
    
    private SortedKeyValueIterator<Key,Value> source;
    
    // support for fetching all negated function fields and values in sorted order, reducing number
    // of seeks across index block boundaries.
    private int subQueryId = 0;
    private final Multimap<String,Integer> fvToFunctionIds = HashMultimap.create();
    
    private TermFrequencyConfig tfConfig;
    
    /**
     *
     * @param tfConfig
     *            config object for this function
     *
     * @param tfFVs
     *            a multimap of term frequency fields to values
     */
    public TermFrequencyHitFunction(TermFrequencyConfig tfConfig, Multimap<String,String> tfFVs) {
        this.tfConfig = tfConfig;
        this.tfFVs = tfFVs;
        populateFunctionSearchSpace(tfConfig.getScript());
    }
    
    // used to perform a lazy init
    private void initializeSource() {
        this.source = tfConfig.getSourceDeepCopy();
    }
    
    /**
     * Build the search space for all content phrase functions that contributed to this document hit
     *
     * @param docKey
     *            a key taking the form shard:datatype\0uid
     * @param doc
     *            an aggregated document
     * @return a sorted set of TermFrequency column qualifiers (datatype\0uid\0value\0field)
     */
    public TreeSet<Text> apply(Key docKey, Document doc) {
        TreeSet<Text> hits = new TreeSet<>();
        // Track the document hits as FIELD -> field\0value
        Multimap<String,String> documentHits = HashMultimap.create();
        // Track the mapping of field/value to uids, specifically for the TLD case
        Multimap<String,String> fvUidCache = HashMultimap.create();
        
        StringBuilder cqBuilder = new StringBuilder();
        
        // 0. Handle negated content functions first.
        if (hasNegatedFunctions) {
            if (tfConfig.isTld()) {
                // A negated field value could be in any child document. Must scan the fi and build a document of those hits.
                try {
                    negatedDocument = buildNegatedDocument(docKey);
                } catch (IOException e) {
                    log.error("Problem building negated document: ", e);
                    throw new DatawaveFatalQueryException("Could not build negated document for term frequencies", e);
                }
            } else {
                // In the event query case we only have one uid, thus we know where to find the negated terms -- if they exist.
                buildSearchSpaceFromNegatedFunctions(docKey, documentHits, fvUidCache);
            }
        }
        
        // 1. Filter the document by the content function search space
        for (String field : functionFields) {
            Attribute<?> attribute = doc.get(field);
            buildSearchSpaceFromDocument(field, attribute, documentHits, fvUidCache);
            
            if (negatedDocument != null) {
                attribute = negatedDocument.get(field);
                buildSearchSpaceFromDocument(field, attribute, documentHits, fvUidCache);
            }
        }
        
        // 2. Now filter the function search space via the document hits
        // At this point terms positive terms exist in the document and any negated terms are fetched.
        Collection<SubQuery> subQueries = new HashSet<>();
        subQueries.addAll(positiveSubQueries);
        subQueries.addAll(negativeSubQueries);
        for (SubQuery subQuery : subQueries) {
            
            // Compute the function field-value pairs on the fly...
            Set<String> functionHitsForField = new HashSet<>();
            for (String fieldValue : subQuery.fieldValues) {
                if (documentHits.containsEntry(subQuery.field, fieldValue)) {
                    functionHitsForField.add(fieldValue);
                }
            }
            
            // If any pair was filtered out continue to the next function
            if (functionHitsForField.size() != subQuery.fieldValues.size()) {
                continue;
            }
            
            // Perform iterative intersection
            Set<String> intersectedUids = new HashSet<>();
            if (!functionHitsForField.isEmpty()) {
                
                Iterator<String> vfIter = functionHitsForField.iterator();
                intersectedUids.addAll(fvUidCache.get(vfIter.next()));
                
                while (vfIter.hasNext()) {
                    
                    Set<String> nextUids = (Set<String>) fvUidCache.get(vfIter.next());
                    intersectedUids = Sets.intersection(intersectedUids, nextUids);
                    
                    if (intersectedUids.isEmpty()) {
                        // If we prune to zero at any point, stop for this field.
                        functionHitsForField.clear();
                        break;
                    }
                }
            }
            
            // Build TF CQ like 'datatype\0uid\0value\0field'
            if (!functionHitsForField.isEmpty()) {
                
                String datatype = parseDatatypeFromCF(docKey);
                
                for (String uid : intersectedUids) {
                    for (String fieldValue : functionHitsForField) {
                        int index = fieldValue.indexOf('\u0000');
                        
                        cqBuilder.setLength(0);
                        cqBuilder.append(datatype);
                        cqBuilder.append('\u0000');
                        cqBuilder.append(uid);
                        cqBuilder.append('\u0000');
                        cqBuilder.append(fieldValue.substring(index + 1));
                        cqBuilder.append('\u0000');
                        cqBuilder.append(fieldValue, 0, index);
                        
                        hits.add(new Text(cqBuilder.toString()));
                    }
                }
            }
        }
        
        return hits;
    }
    
    /**
     * For the case of an event query with negated content functions, build the search space without reaching back to the field index.
     *
     * @param docKey
     *            a key like shard:datatype\0uid
     * @param documentHits
     *            tracks the value hits for a field
     * @param fvUidCache
     *            tracks the uids that map to a specific field\0value pair
     */
    private void buildSearchSpaceFromNegatedFunctions(Key docKey, Multimap<String,String> documentHits, Multimap<String,String> fvUidCache) {
        String uid = parseUidFromCF(docKey);
        for (SubQuery subQuery : negativeSubQueries) {
            for (String fieldValue : subQuery.fieldValues) {
                if (positiveSearchSpace.contains(fieldValue) || negativeSearchSpace.contains(fieldValue)) {
                    documentHits.put(subQuery.field, fieldValue);
                    // populate uid cache.
                    fvUidCache.put(fieldValue, uid);
                }
            }
        }
    }
    
    /**
     * Given a document attribute determine which values exist within the function search space.
     *
     * @param field
     *            the field
     * @param attribute
     *            an attribute from a document
     * @param documentHits
     *            tracks the field-value hits for a given field
     * @param fvUidCache
     *            tracks the uid hits for a given field-value pair
     */
    private void buildSearchSpaceFromDocument(String field, Attribute<?> attribute, Multimap<String,String> documentHits, Multimap<String,String> fvUidCache) {
        if (attribute instanceof Attributes) {
            Attributes attrs = (Attributes) attribute;
            Set<Attribute<?>> attrSet = attrs.getAttributes();
            for (Attribute<?> element : attrSet) {
                
                String value;
                Object data = element.getData();
                if (data instanceof String) {
                    value = (String) data;
                } else if (data instanceof Type<?>) {
                    Type<?> type = (Type<?>) data;
                    value = type.getNormalizedValue();
                } else {
                    throw new IllegalStateException("Expected attribute to be either a String or a Type<?> but was " + data.getClass());
                }
                
                String fieldValue = field + '\u0000' + value;
                if (positiveSearchSpace.contains(fieldValue) || negativeSearchSpace.contains(fieldValue)) {
                    documentHits.put(field, fieldValue);
                    
                    // populate uid cache from key like 'shard:datatype\0uid'
                    Key metadata = element.getMetadata();
                    String uid = parseUidFromCF(metadata);
                    fvUidCache.put(fieldValue, uid);
                }
            }
        }
        // If the attribute was a single instance there is no point in evaluating it.
        // ContentFunctions by definition require more than one hit.
    }
    
    /**
     * Build a document from negated content function terms. Uids for a field value pair are fetched from the field index.
     *
     * Tracks which field value pairs have already been run, that is which returned results or returned zero results. This information avoids duplicate work and
     * can exclude entire content functions that share a field value that has no entries in the field index.
     *
     * Note on implementation: seeking to a new index block is expensive. Therefore we roll through the fields and values in sorted order.
     *
     * @param docKey
     *            the document key
     * @return a document of negated field value pairs
     * @throws IOException
     *             if the underlying iterator encounters a problem
     */
    private Document buildNegatedDocument(Key docKey) throws IOException {
        
        long start = System.nanoTime();
        
        Document doc = new Document();
        Range limitRange = buildLimitRange(docKey);
        
        // Track function ids that will never hit, therefore do not continue
        // looking up additional field value pairs from that function
        Set<Integer> missed = new HashSet<>();
        Multimap<String,Attribute<?>> fetchedAttrs = HashMultimap.create();
        
        for (String fieldValue : negativeSearchSpace) {
            Collection<Integer> ids = fvToFunctionIds.get(fieldValue);
            
            boolean allMissed = true;
            for (int id : ids) {
                if (!missed.contains(id)) {
                    allMissed = false;
                    break;
                }
            }
            
            if (allMissed) {
                // If every function mapped to this field value pair is part of the missed cache, don't bother looking up this field value.
                continue;
            }
            
            int index = fieldValue.indexOf('\u0000');
            String field = fieldValue.substring(0, index);
            String value = fieldValue.substring(index + 1);
            List<Attribute<?>> fetched = new ArrayList<>();
            boolean fetchedValues = fetchKeysForFieldValue(fetched, limitRange, field, value);
            if (fetchedValues) {
                fetchedAttrs.putAll(fieldValue, fetched);
            } else {
                missed.addAll(fvToFunctionIds.get(fieldValue));
            }
        }
        
        Set<Integer> functionsWithHits = new HashSet<>(fvToFunctionIds.values());
        functionsWithHits.removeAll(missed);
        
        for (SubQuery subQuery : negativeSubQueries) {
            if (functionsWithHits.contains(subQuery.id)) {
                for (String fieldValue : subQuery.fieldValues) {
                    Collection<Attribute<?>> attrs = fetchedAttrs.get(fieldValue);
                    for (Attribute<?> attr : attrs) {
                        doc.put(subQuery.field, attr);
                    }
                }
            }
        }
        
        if (log.isDebugEnabled()) {
            long total = System.nanoTime() - start;
            log.debug("Time to build negated document: " + (total / 1000000) + " ms, " + total + " ns.");
        }
        
        return doc;
    }
    
    /**
     * Fetch field value pairs from the field index using a delayed iterator
     *
     * @param fetched
     *            add fetched attributes to this list
     * @param limitRange
     *            used to build the seek range
     * @param field
     *            the field
     * @param value
     *            the value
     * @return true if this field value pair contained entries in the field index
     * @throws IOException
     *             if an error occurred while fetching keys off the field index
     */
    private boolean fetchKeysForFieldValue(List<Attribute<?>> fetched, Range limitRange, String field, String value) throws IOException {
        
        if (source == null) {
            // lazy init
            initializeSource();
        }
        
        Range seekRange = buildSeekRangeForFi(limitRange.getStartKey(), field, value);
        Collection<ByteSequence> seekCFs = Collections.singleton(new ArrayByteSequence("fi\0" + field));
        source.seek(seekRange, seekCFs, true);
        
        int keysFetched = 0;
        while (source.hasTop()) {
            keysFetched++;
            Key next = transformKey(source.getTopKey(), field, value);
            Attribute<?> attr = new PreNormalizedAttribute(value, next, true);
            fetched.add(attr);
            source.next();
        }
        
        return keysFetched > 0;
    }
    
    /**
     * Transform a raw field index key into a document key
     *
     * @param key
     *            a field index formatted key
     * @param field
     *            the field
     * @param value
     *            the value
     * @return a document formatted key
     */
    private Key transformKey(Key key, String field, String value) {
        String cqStr = key.getColumnQualifier().toString();
        int index = cqStr.indexOf('\u0000');
        int nextIndex = cqStr.indexOf('\u0000', index + 1);
        
        String datatype = cqStr.substring(index + 1, nextIndex);
        String uid = cqStr.substring(nextIndex + 1);
        
        return new Key(key.getRow(), new Text(datatype + "\0" + uid), new Text(field + "\0" + value));
    }
    
    // return shard:row:datatype\0uid
    private Range buildLimitRange(Key docKey) {
        return new Range(docKey, true, docKey.followingKey(PartialKey.ROW_COLFAM), false);
    }
    
    /**
     * Build a seek range for the field index
     *
     * @param docKey
     *            takes the form 'shard:datatype\0uid
     * @param field
     *            the field
     * @param value
     *            the value
     * @return a seek range built for the field index in hte form 'shard:fi\0field:value\0datatype\0uid
     */
    private Range buildSeekRangeForFi(Key docKey, String field, String value) {
        String cfStr = docKey.getColumnFamily().toString();
        int index = cfStr.indexOf('\u0000');
        String datatype = cfStr.substring(0, index);
        String uid = cfStr.substring(index + 1);
        
        // FI key is shard:fi\0FIELD:value\0datatype\0uid
        Text cf = new Text("fi\u0000" + field);
        Text cq = new Text(value + '\u0000' + datatype + '\u0000' + uid);
        
        Key startKey = new Key(docKey.getRow(), cf, cq);
        Key endKey;
        if (tfConfig.isTld()) {
            // Append max unicode to pick up child uids
            cq = new Text(value + '\u0000' + datatype + '\u0000' + uid + Constants.MAX_UNICODE_STRING);
            endKey = new Key(docKey.getRow(), cf, cq);
        } else {
            // restrict search to just this specific uid
            endKey = startKey.followingKey(PartialKey.ROW_COLFAM_COLQUAL);
        }
        
        return new Range(startKey, true, endKey, false);
    }
    
    // Will recursively ascend the tree looking for an ASTDelayedPredicate
    private boolean isFunctionDelayed(Function function) {
        JexlNode arg = function.args().get(0);
        return isFunctionDelayed(arg, new HashSet<>());
    }
    
    // recursively ascend a query tree looking for ASTDelayedPredicate nodes
    private boolean isFunctionDelayed(JexlNode node, Set<JexlNode> seen) {
        
        seen.add(node);
        
        if (node instanceof ASTJexlScript) {
            return false;
        } else if (node instanceof ASTAndNode) {
            // If a child is an instance of an ASTDelayedPredicate
            for (int i = 0; i < node.jjtGetNumChildren(); i++) {
                JexlNode child = node.jjtGetChild(i);
                if (!seen.contains(child) && QueryPropertyMarker.findInstance(child).isType(ASTDelayedPredicate.class)) {
                    return true;
                }
            }
            
            // If no child was delayed, continue ascending
            return isFunctionDelayed(node.jjtGetParent(), seen);
        } else if (QueryPropertyMarker.findInstance(node).isType(ASTDelayedPredicate.class)) {
            return true;
        } else {
            return isFunctionDelayed(node.jjtGetParent(), seen);
        }
    }
    
    // parse the uid directly from a key with a column family like datatype\0uid
    private String parseUidFromCF(Key k) {
        ByteSequence backing = k.getColumnFamilyData();
        int index = -1;
        for (int i = backing.offset(); i < backing.length(); i++) {
            if (backing.byteAt(i) == '\u0000') {
                index = i + 1;
                break;
            }
        }
        return new String(backing.subSequence(index, backing.length()).toArray());
    }
    
    // parse the datatype directly from a key with a column family like datatype\0uid
    private String parseDatatypeFromCF(Key k) {
        ByteSequence backing = k.getColumnFamilyData();
        int index = -1;
        for (int i = backing.offset(); i < backing.length(); i++) {
            if (backing.byteAt(i) == '\u0000') {
                index = i;
                break;
            }
        }
        return new String(backing.subSequence(0, index).toArray());
    }
    
    /**
     * Preprocessing step, parse functions into their respective sub queries and precompute search space.
     * 
     * @param script
     *            the query tree
     */
    private void populateFunctionSearchSpace(ASTJexlScript script) {
        Multimap<String,Function> allFunctions = TermOffsetPopulator.getContentFunctions(script);
        Set<Function> functions = new HashSet<>(allFunctions.values());
        for (Function function : functions) {
            List<SubQuery> subQueries = parseFunction(function);
            for (SubQuery subQuery : subQueries) {
                
                // populate field search space
                functionFields.add(subQuery.field);
                
                if (subQuery.isNegated) {
                    negativeSubQueries.add(subQuery);
                    for (String fieldValue : subQuery.fieldValues) {
                        fvToFunctionIds.put(fieldValue, subQuery.id);
                        negativeSearchSpace.add(fieldValue);
                    }
                } else {
                    positiveSubQueries.add(subQuery);
                    for (String fieldValue : subQuery.fieldValues) {
                        fvToFunctionIds.put(fieldValue, subQuery.id);
                        positiveSearchSpace.add(fieldValue);
                    }
                }
            }
        }
    }
    
    /**
     * Get this function's search space as defined by fields and values
     *
     * @param function
     *            a {@link Function} that is either adjacent, phrase, or within
     * @return a list of fields and values associated with the provided function
     */
    private List<SubQuery> parseFunction(Function function) {
        // functions may take different forms..
        // within = {field, number, termOffsetMap, terms...}
        // within = {number, termOffsetMap, terms...}
        // adjacent = {field, termOffsetMap, terms...}
        // adjacent = {termOffsetMap, terms...}
        // phrase = {field, termOffsetMap, terms...}
        // phrase = {termOffsetMap, terms...}
        
        if (!TermOffsetPopulator.phraseFunctions.contains(function.name())) {
            log.error(function.name() + " is not a supported content function.");
            return Collections.EMPTY_LIST;
        }
        
        List<JexlNode> args = function.args();
        int index = function.name().equals("within") ? 3 : 2;
        
        // If the first arg is a number or termOffsetMap then no field was provided as part of the function.
        // The fields will be build via value lookups from the tfFVs.
        boolean specialCase = false;
        JexlNode first = args.get(0);
        if (isFirstNodeSpecial(first)) {
            specialCase = true;
            index--;
        }
        
        // Parse the values first. Might have to lookup fields by value.
        TreeSet<String> values = getValuesFromArgs(args, index);
        
        TreeSet<String> fields;
        if (specialCase) {
            // field(s) were not present in the function, lookup fields by value.
            fields = lookupFieldsByValues(values);
        } else {
            // function's first arg is the field, or fields in the form (FIELD_A || FIELD_B)
            fields = parseField(args.get(0));
        }
        
        // Negated terms are not looked up in the field index, and thus do not have values in the document.
        // This function needs to fetch the missing values for any content function wrapped in a delayed marker.
        boolean isDelayed = isFunctionDelayed(function);
        if (isDelayed) {
            this.hasNegatedFunctions = true;
        }
        
        // Build up the list of sub queries
        LinkedList<SubQuery> subQueries = new LinkedList<>();
        for (String field : fields) {
            subQueries.add(new SubQuery(subQueryId++, isDelayed, field, values));
        }
        
        return subQueries;
    }
    
    // If a function does not contain the fields being queried, find the fields via the term frequency field-value map
    private TreeSet<String> lookupFieldsByValues(Set<String> values) {
        TreeSet<String> fields = new TreeSet<>();
        for (String key : tfFVs.keySet()) {
            for (String value : values) {
                if (tfFVs.containsEntry(key, value)) {
                    fields.add(key);
                }
            }
        }
        return fields;
    }
    
    // A node is special if it is not the fields being searched. That is, the first node is a number or a variable 'termOffsetMap'
    private boolean isFirstNodeSpecial(JexlNode node) {
        if (node instanceof ASTNumberLiteral || node instanceof ASTUnaryMinusNode) {
            return true;
        } else if (node instanceof ASTReference) {
            List<ASTIdentifier> ids = JexlASTHelper.getIdentifiers(node);
            return ids.size() == 1 && ids.get(0).image.equals("termOffsetMap");
        }
        return false;
    }
    
    /**
     * Extract fields from a JexlNode. Node may be an OR node like (FIELD_A || FIELD_B).
     *
     * @param node
     *            a jexl node
     * @return a sorted set of fields
     */
    private TreeSet<String> parseField(JexlNode node) {
        TreeSet<String> fields = new TreeSet<>();
        List<ASTIdentifier> identifiers = JexlASTHelper.getIdentifiers(node);
        for (ASTIdentifier identifier : identifiers) {
            fields.add(JexlASTHelper.deconstructIdentifier(identifier));
        }
        return fields;
    }
    
    /**
     * Parse the values out of a content functions arguments
     *
     * @param args
     *            content function arguments as a list of Jexl nodes
     * @param start
     *            the start index for the values
     * @return a list of normalized string values
     */
    private TreeSet<String> getValuesFromArgs(List<JexlNode> args, int start) {
        TreeSet<String> values = new TreeSet<>();
        for (int i = start; i < args.size(); i++) {
            List<String> parsed = parseArg(args.get(i));
            values.addAll(parsed);
        }
        return values;
    }
    
    private List<String> parseArg(JexlNode node) {
        List<String> parsed = new LinkedList<>();
        List<Object> values = JexlASTHelper.getLiteralValues(node);
        for (Object value : values) {
            if (value instanceof String) {
                parsed.add((String) value);
            } else {
                throw new IllegalStateException("Expected literal value to be String cast-able");
            }
        }
        return parsed;
    }
    
    /**
     * A multi-fielded function can be thought of as the conjunction of multiple sub queries.
     */
    private class SubQuery {
        public final int id;
        public final boolean isNegated;
        public final String field;
        public final TreeSet<String> fieldValues;
        
        public SubQuery(int id, boolean isNegated, String field, TreeSet<String> values) {
            this.id = id;
            this.isNegated = isNegated;
            this.field = field;
            this.fieldValues = new TreeSet<>();
            for (String value : values) {
                fieldValues.add(this.field + '\u0000' + value);
            }
        }
    }
}
