package datawave.query.jexl.nodes;

import org.apache.commons.jexl2.parser.JexlNode;

import java.util.function.Function;

/**
 * This is a node that can wrap an expression to denote that it is to be used only for index lookup purposes and not for evaluation. Example is for ranges that
 * have multiple normalizations over time, but should only be evaluated using one of them (e.g. numeric).
 */
public class NoEvaluation extends QueryPropertyMarker {
    
    private static final String LABEL = "_NoEval_";
    
    /**
     * Return the label this marker type: {@value #LABEL}. Overrides {@link QueryPropertyMarker#label()}.
     *
     * @return the label
     */
    public static String label() {
        return LABEL;
    }
    
    /**
     * Create and return a new {@link NoEvaluation} with the given source.
     *
     * @param node
     *            the source node
     * @return the new marker node
     * @see QueryPropertyMarker#create(JexlNode, Function)
     */
    public static NoEvaluation create(JexlNode node) {
        return create(node, NoEvaluation::new);
    }
    
    public NoEvaluation() {
        super();
    }
    
    public NoEvaluation(int id) {
        super(id);
    }
    
    /**
     * Returns a new query property marker with the expression <code>(({@value #LABEL} = true) &amp;&amp; ({source}))</code>.
     *
     * @param node
     *            the source node
     * @see QueryPropertyMarker#QueryPropertyMarker(JexlNode) the super constructor for additional information on the tree structure
     */
    public NoEvaluation(JexlNode node) {
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
