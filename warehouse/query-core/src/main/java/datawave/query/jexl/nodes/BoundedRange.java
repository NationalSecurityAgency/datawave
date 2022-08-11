package datawave.query.jexl.nodes;

import org.apache.commons.jexl2.parser.JexlNode;

import java.util.function.Function;

/**
 * This is a node that can wrap an expression to mark that the source expression is a bounded range. A bounded range is a range that can be applied to only one
 * value out of a multi-valued field.
 */
public class BoundedRange extends QueryPropertyMarker {
    
    private static final String LABEL = "_Bounded_";
    
    /**
     * Return the label this marker type: {@value #LABEL}. Overrides {@link QueryPropertyMarker#label()}.
     * 
     * @return the label
     */
    public static String label() {
        return LABEL;
    }
    
    /**
     * Create and return a new {@link BoundedRange} with the given source.
     * 
     * @param node
     *            the source node
     * @return the new marker node
     * @see QueryPropertyMarker#create(JexlNode, Function)
     */
    public static BoundedRange create(JexlNode node) {
        return create(node, BoundedRange::new);
    }
    
    public BoundedRange() {
        super();
    }
    
    public BoundedRange(int id) {
        super(id);
    }
    
    /**
     * Returns a new query property marker with the expression <code>(({@value #LABEL} = true) &amp;&amp; ({source}))</code>.
     * 
     * @param node
     *            the source node
     * @see QueryPropertyMarker#QueryPropertyMarker(JexlNode) the super constructor for additional information on the tree structure
     */
    public BoundedRange(JexlNode node) {
        super(node);
    }
    
    /**
     * Returns {@value #LABEL}.
     * 
     * @return the label
     */
    @Override
    public String getLabel() {
        return LABEL;
    }
}
