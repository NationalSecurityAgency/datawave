package datawave.query.iterator.filter.composite;

import datawave.query.jexl.ArithmeticJexlEngines;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.NormalizedValueArithmetic;
import datawave.query.jexl.visitors.CompositePredicateVisitor;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.MapContext;
import org.apache.commons.jexl2.Script;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.log4j.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This filter is used during field index iteration in order to determin whether the composite terms from a composite indexed field satisfy the range for each
 * composite term found in the query.
 *
 * This class uses a simplified version of the JexlArithmetic which performs string comparisons against the normalized field values.
 */
public class CompositePredicateFilter {
    private static final Logger log = Logger.getLogger(CompositePredicateFilter.class);
    
    protected List<String> compositeFields;
    protected Long transitionDateMillis;
    protected Set<JexlNode> compositePredicates = new HashSet<>();
    protected Script script = null;
    
    public CompositePredicateFilter(List<String> compositeFields, Long transitionDateMillis) {
        this.compositeFields = compositeFields;
        this.transitionDateMillis = transitionDateMillis;
    }
    
    public boolean keep(String[] terms, long timestamp) {
        if (!compositePredicates.isEmpty()) {
            if (script == null)
                script = queryToScript(JexlStringBuildingVisitor.buildQuery(JexlNodeFactory.createAndNode(compositePredicates)));
            
            if (terms.length == compositeFields.size()) {
                MapContext jexlContext = new MapContext();
                for (int i = 0; i < compositeFields.size(); i++)
                    jexlContext.set(compositeFields.get(i), terms[i]);
                
                if (!ArithmeticJexlEngines.isMatched(script.execute(jexlContext))) {
                    if (log.isTraceEnabled())
                        log.trace("Filtered out an entry using the composite range filter iterator: " + jexlContext);
                    return false;
                }
            } else if (!(terms.length == 1 && transitionDateMillis != null && timestamp < transitionDateMillis)) {
                // indicates that we are dealing with data that was ingested
                // before we transitioned this field to a composite field
                return false;
            }
        }
        return true;
    }
    
    public void addCompositePredicates(Set<JexlNode> nodes) {
        for (JexlNode compNode : nodes) {
            Set<JexlNode> compNodes = CompositePredicateVisitor.findCompositePredicates(compNode, compositeFields);
            if (compNodes != null && !compNodes.isEmpty())
                compositePredicates.addAll(compNodes);
        }
    }
    
    private Script queryToScript(String query) {
        JexlEngine engine = new JexlEngine(null, new NormalizedValueArithmetic(), null, null);
        engine.setCache(1024);
        engine.setSilent(false);
        
        // Setting strict to be true causes an Exception when a field
        // in the query does not occur in the document being tested.
        // This doesn't appear to have any unexpected consequences looking
        // at the Interpreter class in JEXL.
        engine.setStrict(false);
        
        return engine.createScript(query);
    }
    
    public Set<JexlNode> getCompositePredicates() {
        return compositePredicates;
    }
    
    public void setCompositePredicates(Set<JexlNode> compositePredicates) {
        this.compositePredicates = compositePredicates;
    }
    
    public List<String> getCompositeFields() {
        return compositeFields;
    }
    
    public void setCompositeFields(List<String> compositeFields) {
        this.compositeFields = compositeFields;
    }
}
