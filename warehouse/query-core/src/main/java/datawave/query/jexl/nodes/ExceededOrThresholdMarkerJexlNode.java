package datawave.query.jexl.nodes;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.AbstractCollection;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;

import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.hadoop.util.StringUtils;

/**
 * This is a node that can be put in place of or list to denote that the or list threshold was exceeded
 */
public class ExceededOrThresholdMarkerJexlNode extends QueryPropertyMarker {
    
    public static final String FIELD_PROP = "fieldName";
    public static final String VALUES_PROP = "fieldValues";
    public static final String FST_URI_PROP = "fieldFstURI";
    
    public ExceededOrThresholdMarkerJexlNode(int id) {
        super(id);
    }
    
    public ExceededOrThresholdMarkerJexlNode() {
        super();
    }
    
    /**
     * This will create a structure as follows around the specified node: Reference (this node) Reference Expression AND Reference Reference Expression
     * Assignment Reference Identifier:ExceededOrThresholdMarkerJexlNode True node (the one specified
     * 
     * Hence the resulting expression will be ((ExceededOrThresholdMarkerJexlNode = True) AND {specified node})
     * 
     * @param node
     */
    public ExceededOrThresholdMarkerJexlNode(JexlNode node) {
        super(node);
    }
    
    public ExceededOrThresholdMarkerJexlNode(String fieldname, URI fstPath) {
        // Create an assignment for the field name and fst paths
        JexlNode fieldAssignment = JexlNodeFactory.createExpression(JexlNodeFactory.createAssignment(FIELD_PROP, fieldname));
        JexlNode fstAssignment = JexlNodeFactory.createExpression(JexlNodeFactory.createAssignment(FST_URI_PROP, fstPath.toString()));
        
        // wrap the assignments in an AND node
        JexlNode andNode = JexlNodeFactory.createUnwrappedAndNode(Arrays.asList(fieldAssignment, fstAssignment));
        
        // now set the source
        setupSource(andNode);
    }
    
    public ExceededOrThresholdMarkerJexlNode(String fieldname, Collection<String> values) {
        // Create an assignment for the field name
        JexlNode fieldAssignment = JexlNodeFactory.createExpression(JexlNodeFactory.createAssignment(FIELD_PROP, fieldname));
        
        // Create an assignment for the values
        JexlNode valuesAssignment = JexlNodeFactory.createExpression(JexlNodeFactory.createAssignment(VALUES_PROP,
                        StringUtils.join(StringUtils.COMMA_STR, new EscapedCollection(values))));
        
        // wrap the assignments in an AND node
        JexlNode andNode = JexlNodeFactory.createUnwrappedAndNode(Arrays.asList(fieldAssignment, valuesAssignment));
        
        // now set the source
        setupSource(andNode);
    }
    
    private static class EscapedCollection extends AbstractCollection<String> {
        private Collection<String> values = null;
        
        public EscapedCollection(Collection<String> values) {
            this.values = values;
        }
        
        @Override
        public Iterator<String> iterator() {
            final Iterator<String> delegate = values.iterator();
            return new Iterator<String>() {
                @Override
                public boolean hasNext() {
                    return delegate.hasNext();
                }
                
                @Override
                public String next() {
                    return StringUtils.escapeString(delegate.next(), StringUtils.ESCAPE_CHAR, StringUtils.COMMA);
                }
                
                @Override
                public void remove() {
                    delegate.remove();
                }
            };
        }
        
        @Override
        public int size() {
            return values.size();
        }
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
     * @return A map of parameters for FIELD_PROP, and FST_URI_PROP or VALUES_PROP. URI will be a URI object, and values will be a Set of String
     * @throws URISyntaxException
     */
    public static Map<String,Object> getParameters(JexlNode source) throws URISyntaxException {
        Map<String,Object> returnParameters = new HashMap<>();
        Map<String,Object> parameters = JexlASTHelper.getAssignments(source);
        // turn the FST URI into a URI and the values into a set of values
        for (Map.Entry<String,Object> entry : parameters.entrySet()) {
            if (entry.getKey().equalsIgnoreCase(FST_URI_PROP)) {
                returnParameters.put(FST_URI_PROP, new URI(String.valueOf(entry.getValue())));
            } else if (entry.getKey().equalsIgnoreCase(VALUES_PROP)) {
                returnParameters.put(VALUES_PROP,
                                new HashSet<>(Arrays.asList(StringUtils.split(String.valueOf(entry.getValue()), StringUtils.ESCAPE_CHAR, StringUtils.COMMA))));
            } else if (entry.getKey().equalsIgnoreCase(FIELD_PROP)) {
                returnParameters.put(FIELD_PROP, String.valueOf(entry.getValue()));
            }
        }
        return returnParameters;
    }
    
}
