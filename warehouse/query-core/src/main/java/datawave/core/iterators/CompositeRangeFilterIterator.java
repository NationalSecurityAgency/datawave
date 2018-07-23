package datawave.core.iterators;

import datawave.query.composite.CompositeUtils;
import datawave.query.jexl.ArithmeticJexlEngines;
import datawave.query.jexl.NormalizedValueArithmetic;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.MapContext;
import org.apache.commons.jexl2.Script;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;

/**
 * Filters out rows whose composite terms are outside of the range defined by the upper and lower composite bounds.
 *
 */
public class CompositeRangeFilterIterator extends Filter {
    
    private static final Logger log = Logger.getLogger(CompositeRangeFilterIterator.class);
    
    public static final String COMPOSITE_FIELDS = "composite.fields";
    public static final String COMPOSITE_PREDICATE = "composite.predicate";
    public static final String COMPOSITE_TRANSITION_DATE = "composite.transition.date";
    
    protected String[] fieldNames = null;
    protected String compositePredicate = null;
    protected Script compositePredicateScript = null;
    protected Long transitionDateMillis = null;
    
    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        CompositeRangeFilterIterator to = new CompositeRangeFilterIterator();
        to.setSource(getSource().deepCopy(env));
        
        to.fieldNames = new String[fieldNames.length];
        for (int i = 0; i < fieldNames.length; i++)
            to.fieldNames[i] = new String(fieldNames[i]);
        
        to.compositePredicate = new String(compositePredicate);
        
        to.compositePredicateScript = queryToScript(to.compositePredicate);
        
        if (transitionDateMillis != null)
            to.transitionDateMillis = new Long(transitionDateMillis);
        
        return to;
    }
    
    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
        
        final String fields = options.get(COMPOSITE_FIELDS);
        if (fields != null)
            this.fieldNames = fields.split(",");
        
        final String compositePredicate = options.get(COMPOSITE_PREDICATE);
        if (null != compositePredicate) {
            this.compositePredicate = compositePredicate;
            this.compositePredicateScript = queryToScript(compositePredicate);
        }
        
        final String transitionDate = options.get(COMPOSITE_TRANSITION_DATE);
        if (null != transitionDate) {
            this.transitionDateMillis = Long.parseLong(transitionDate);
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
    
    @Override
    public boolean accept(Key key, Value value) {
        String[] terms = key.getRow().toString().split(CompositeUtils.SEPARATOR);
        MapContext jexlContext = new MapContext();
        
        if (terms.length == fieldNames.length) {
            for (int i = 0; i < fieldNames.length; i++)
                jexlContext.set(fieldNames[i], terms[i]);
            
            if (!ArithmeticJexlEngines.isMatched(compositePredicateScript.execute(jexlContext))) {
                if (log.isTraceEnabled())
                    log.trace("Filtered out an entry using the composite range filter iterator: " + jexlContext);
                return false;
            }
            
            return true;
        } else if (terms.length == 1 && transitionDateMillis != null && key.getTimestamp() < transitionDateMillis) {
            return true;
        } else {
            return false;
        }
    }
}
