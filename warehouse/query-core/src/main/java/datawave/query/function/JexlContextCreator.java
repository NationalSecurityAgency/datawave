package datawave.query.function;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import datawave.query.jexl.DatawaveJexlContext;
import datawave.query.attributes.Document;
import datawave.query.util.Tuple3;
import datawave.query.util.Tuples;

import org.apache.accumulo.core.data.Key;
import org.apache.log4j.Logger;

import com.google.common.base.Function;

public class JexlContextCreator implements Function<Tuple3<Key,Document,Map<String,Object>>,Tuple3<Key,Document,DatawaveJexlContext>> {
    
    private static final Logger log = Logger.getLogger(JexlContextCreator.class);
    
    protected Collection<String> variables;
    protected JexlContextValueComparator factory;
    protected Map<String,Object> additionalEntries = new HashMap<>();
    
    public JexlContextCreator(Collection<String> variables, JexlContextValueComparator factory) {
        this.variables = variables != null ? variables : Collections.emptySet();
        this.factory = factory;
    }
    
    public JexlContextCreator() {
        this(Collections.emptySet(), null);
    }
    
    @Override
    public Tuple3<Key,Document,DatawaveJexlContext> apply(Tuple3<Key,Document,Map<String,Object>> from) {
        final DatawaveJexlContext context = this.newDatawaveJexlContext(from);
        
        // We can only recurse over Documents to add them into the DatawaveJexlContext because
        // we need to have fielded values to place them into the Map.
        from.second().visit(variables, context);
        
        // the termOffsetMap is now loaded into the context via the visit method above
        
        // add any additional entries
        for (Entry<String,Object> entry : additionalEntries.entrySet()) {
            if (context.has(entry.getKey())) {
                throw new IllegalStateException("Conflict when merging Jexl contexts!");
            } else {
                context.set(entry.getKey(), entry.getValue());
            }
        }
        
        if (log.isTraceEnabled()) {
            log.trace("Constructed context from index and attribute Documents: " + context);
        }
        
        return Tuples.tuple(from.first(), from.second(), context);
    }
    
    public void addAdditionalEntries(Map<String,Object> additionalEntries) {
        this.additionalEntries.putAll(additionalEntries);
    }
    
    /**
     * Factory method for creating JEXL contexts. May be overridden for creating specialized contexts and/or unit testing purposes.
     * 
     * @param from
     *            A tuple containing information which may be helpful for the creation of a JEXL context
     * @return A JEXL context
     */
    protected DatawaveJexlContext newDatawaveJexlContext(final Tuple3<Key,Document,Map<String,Object>> from) {
        return new DatawaveJexlContext(factory == null ? null : factory.getValueComparator(from));
    }
    
    /**
     * An interface used to create a comparator for ordering lists of values within the jexl context.
     */
    public interface JexlContextValueComparator {
        /**
         * Create a comparator for jexl context value lists. The values are expected to be ValueTuple objects however a jexl context does not enforce this, so
         * {@code Comparator<Object>} is required
         * 
         * @param from
         *            the from tuple
         * @return an object comparator
         */
        Comparator<Object> getValueComparator(final Tuple3<Key,Document,Map<String,Object>> from);
    }
    
}
