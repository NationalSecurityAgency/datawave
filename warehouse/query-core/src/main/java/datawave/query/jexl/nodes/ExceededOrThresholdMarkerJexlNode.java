package datawave.query.jexl.nodes;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.commons.jexl2.parser.JexlNode;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;

/**
 * This is a node that can be put in place of or list to denote that the or list threshold was exceeded
 */
public class ExceededOrThresholdMarkerJexlNode extends QueryPropertyMarker {
    
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    public static final String EXCEEDED_OR_ID = "id";
    public static final String EXCEEDED_OR_FIELD = "field";
    public static final String EXCEEDED_OR_PARAMS = "params";
    
    private static final String LABEL = "_List_";
    
    public static String label() {
        return LABEL;
    }
    
    public ExceededOrThresholdMarkerJexlNode(int id) {
        super(id);
    }
    
    public ExceededOrThresholdMarkerJexlNode() {
        super();
    }
    
    /**
     * This will create a structure as follows around the specified node: Reference (this node) Reference Expression AND Reference Reference Expression
     * Assignment Reference Identifier:_List_ True node (the one specified
     * 
     * Hence the resulting expression will be ((_List_ = True) AND {specified node})
     * 
     * @param node
     */
    public ExceededOrThresholdMarkerJexlNode(JexlNode node) {
        super(node);
    }
    
    @Override
    public String getLabel() {
        return LABEL;
    }
    
    public static ExceededOrThresholdMarkerJexlNode createFromFstURI(String fieldName, URI fstPath) throws JsonProcessingException {
        return new ExceededOrThresholdMarkerJexlNode(fieldName, fstPath, null, null);
    }
    
    public static ExceededOrThresholdMarkerJexlNode createFromValues(String fieldName, Set<String> values) throws JsonProcessingException {
        return new ExceededOrThresholdMarkerJexlNode(fieldName, null, values, null);
    }
    
    public static ExceededOrThresholdMarkerJexlNode createFromRanges(String fieldName, Collection<Range> ranges) throws JsonProcessingException {
        return new ExceededOrThresholdMarkerJexlNode(fieldName, null, null, ranges);
    }
    
    private ExceededOrThresholdMarkerJexlNode(String fieldName, URI fstPath, Set<String> values, Collection<Range> ranges) throws JsonProcessingException {
        ExceededOrParams params = (fstPath != null) ? new ExceededOrParams(fstPath.toString()) : new ExceededOrParams(values, ranges);
        
        // Create an assignment for the params
        JexlNode idNode = JexlNodeFactory.createExpression(JexlNodeFactory.createAssignment(EXCEEDED_OR_ID, UUID.randomUUID().toString()));
        JexlNode fieldNode = JexlNodeFactory.createExpression(JexlNodeFactory.createAssignment(EXCEEDED_OR_FIELD, fieldName));
        JexlNode paramsNode = JexlNodeFactory.createExpression(JexlNodeFactory.createAssignment(EXCEEDED_OR_PARAMS, objectMapper.writeValueAsString(params)));
        
        // now set the source
        setupSource(JexlNodeFactory.createAndNode(Arrays.asList(idNode, fieldNode, paramsNode)));
    }
    
    /**
     * A routine to determine whether an and node is actually an exceeded or threshold marker. The reason for this routine is that if the query is serialized
     * and deserialized, then only the underlying assignment will persist.
     * 
     * @param node
     * @return true if this and node is an exceeded or marker
     */
    public static boolean instanceOf(JexlNode node) {
        return QueryPropertyMarker.instanceOf(node, ExceededOrThresholdMarkerJexlNode.class);
    }
    
    /**
     * A routine to determine get the node which is the source of the exceeded or threshold (i.e. the underlying regex or range)
     * 
     * @param node
     * @return the source node or null if not an an exceededOrThreshold Marker
     */
    public static JexlNode getExceededOrThresholdSource(JexlNode node) {
        return QueryPropertyMarker.getQueryPropertySource(node, ExceededOrThresholdMarkerJexlNode.class);
    }
    
    /**
     * Get the parameters for this marker node (see constructors)
     * 
     * @param source
     * @return The params associated with this ExceededOrThresholdMarker
     * @throws IOException
     */
    public static ExceededOrParams getParameters(JexlNode source) throws IOException {
        Map<String,Object> parameters = JexlASTHelper.getAssignments(source);
        // turn the FST URI into a URI and the values into a set of values
        Object paramsObj = parameters.get(EXCEEDED_OR_PARAMS);
        if (paramsObj != null)
            return objectMapper.readValue(String.valueOf(paramsObj), ExceededOrParams.class);
        else
            return null;
    }
    
    /**
     * Get the id for this marker node
     *
     * @param source
     * @return The id associated with this ExceededOrThresholdMarker
     */
    public static String getId(JexlNode source) {
        Map<String,Object> parameters = JexlASTHelper.getAssignments(source);
        Object paramsObj = parameters.get(EXCEEDED_OR_ID);
        if (paramsObj != null)
            return String.valueOf(paramsObj);
        else
            return null;
    }
    
    /**
     * Get the field for this marker node
     *
     * @param source
     * @return The field associated with this ExceededOrThresholdMarker
     */
    public static String getField(JexlNode source) {
        Map<String,Object> parameters = JexlASTHelper.getAssignments(source);
        Object paramsObj = parameters.get(EXCEEDED_OR_FIELD);
        if (paramsObj != null)
            return String.valueOf(paramsObj);
        else
            return null;
    }
    
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class ExceededOrParams {
        private String fstURI;
        private Set<String> values;
        private Collection<String[]> ranges;
        
        // Required for deserialization.
        @SuppressWarnings("unused")
        private ExceededOrParams() {}
        
        ExceededOrParams(String fstURI) {
            this.fstURI = fstURI;
        }
        
        ExceededOrParams(Set<String> values, Collection<Range> ranges) {
            init(values, ranges);
        }
        
        private void init(Set<String> values, Collection<Range> ranges) {
            if (ranges == null || ranges.isEmpty()) {
                this.values = values;
            } else {
                SortedSet<Range> rangeSet = new TreeSet<>();
                
                if (values != null)
                    values.forEach(value -> rangeSet.add(new Range(new Key(value), new Key(value))));
                
                rangeSet.addAll(ranges);
                
                List<Range> mergedRanges = Range.mergeOverlapping(rangeSet);
                Collections.sort(mergedRanges);
                
                this.ranges = new ArrayList<>();
                for (Range mergedRange : mergedRanges) {
                    if (mergedRange.getStartKey().getRow().equals(mergedRange.getEndKey().getRow())) {
                        this.ranges.add(new String[] {String.valueOf(mergedRange.getStartKey().getRow())});
                    } else {
                        this.ranges.add(new String[] {(mergedRange.isStartKeyInclusive() ? "[" : "(") + mergedRange.getStartKey().getRow(),
                                mergedRange.getEndKey().getRow() + (mergedRange.isEndKeyInclusive() ? "]" : ")")});
                    }
                }
            }
        }
        
        private Range decodeRange(String[] range) {
            if (range != null) {
                if (range.length == 1) {
                    return new Range(new Key(range[0]), new Key(range[0]));
                } else if (range.length == 2 && isLowerBoundValid(range[0]) && isUpperBoundValid(range[1])) {
                    return new Range(new Key(range[0].substring(1)), range[0].charAt(0) == '[', new Key(range[1].substring(0, range[1].length() - 1)),
                                    range[1].charAt(range[1].length() - 1) == ']');
                }
            }
            return null;
        }
        
        @JsonIgnore
        private boolean isLowerBoundValid(String lowerBound) {
            return lowerBound.length() > 1 && (lowerBound.charAt(0) == '[' || lowerBound.charAt(0) == '(');
        }
        
        @JsonIgnore
        private boolean isUpperBoundValid(String upperBound) {
            return upperBound.length() > 1 && (upperBound.charAt(upperBound.length() - 1) == ']' || upperBound.charAt(upperBound.length() - 1) == ')');
        }
        
        public String getFstURI() {
            return fstURI;
        }
        
        public Collection<String> getValues() {
            return values;
        }
        
        @JsonIgnore
        public SortedSet<Range> getSortedAccumuloRanges() {
            SortedSet<Range> accumuloRanges = new TreeSet<>();
            ranges.forEach(range -> accumuloRanges.add(decodeRange(range)));
            return accumuloRanges;
        }
        
        public Collection<String[]> getRanges() {
            return ranges;
        }
    }
}
