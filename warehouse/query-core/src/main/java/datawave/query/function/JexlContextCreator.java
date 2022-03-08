package datawave.query.function;

import com.google.common.base.Function;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Document;
import datawave.query.attributes.PreNormalizedAttribute;
import datawave.query.jexl.DatawaveJexlContext;
import datawave.query.util.Tuple3;
import datawave.query.util.Tuples;
import org.apache.accumulo.core.data.Key;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.Map.Entry;

public class JexlContextCreator implements Function<Tuple3<Key,Document,Map<String,Object>>,Tuple3<Key,Document,DatawaveJexlContext>> {
    
    private static final Logger log = Logger.getLogger(JexlContextCreator.class);
    private static final String BODY_KEY = "BODY";
    
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
        // dedupe potentially unnecessary Attributes via fuzzy matching
        Tuple3<Key,Document,Map<String,Object>> dedupedFrom = dedupeAttributes(from);
        
        final DatawaveJexlContext context = this.newDatawaveJexlContext(dedupedFrom);
        
        // We can only recurse over Documents to add them into the DatawaveJexlContext because
        // we need to have fielded values to place them into the Map.
        dedupedFrom.second().visit(variables, context);
        
        // absorb the supplied map into the context
        for (Entry<String,Object> entry : dedupedFrom.third().entrySet()) {
            if (context.has(entry.getKey())) {
                throw new IllegalStateException("Conflict when merging Jexl contexts!");
            } else {
                context.set(entry.getKey(), entry.getValue());
            }
        }
        
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
        
        return Tuples.tuple(dedupedFrom.first(), dedupedFrom.second(), context);
    }
    
    private Tuple3 dedupeAttributes(Tuple3<Key,Document,Map<String,Object>> rawTuple) {
        Map<String,Attribute<? extends Comparable<?>>> dict = rawTuple.second().getDictionary();
        Set<Attribute<? extends Comparable<?>>> dedupedAttributeSet = new HashSet<Attribute<? extends Comparable<?>>>();
        
        for (Entry<String,Attribute<? extends Comparable<?>>> entry : dict.entrySet()) {
            if (entry.getKey().equals(BODY_KEY)) {
                Attributes rawButes = (Attributes) entry.getValue();
                Set<Attribute<? extends Comparable<?>>> attributeSet = rawButes.getAttributes();
                List<String> valueList = new ArrayList<String>();
                
                // get list of any preNormalizedAttributes since those are the ones
                // we actually care about and want to keep
                attributeSet.forEach(attribute -> {
                    if (attribute instanceof PreNormalizedAttribute) {
                        String value = ((PreNormalizedAttribute) attribute).getValue();
                        valueList.add(value);
                        dedupedAttributeSet.add(attribute);
                    }
                });
                
                // iterate a second time in order to check potential value against the valueList
                attributeSet.forEach(attribute -> {
                    if (!(attribute instanceof PreNormalizedAttribute)) {
                        String questionableValue = attribute.getData().toString();
                        // no match means a probably unique attribute
                        // which we still want to keep, otherwise don't save it
                        if (!valueList.contains(questionableValue)) {
                            dedupedAttributeSet.add(attribute);
                        }
                    }
                });
            }
        }
        
        Attributes replacementAttributes = new Attributes(dedupedAttributeSet, true, true);
        rawTuple.second().replace(BODY_KEY, replacementAttributes, false, false);
        
        return Tuples.tuple(rawTuple.first(), rawTuple.second(), rawTuple.third());
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
         * @return an object comparator
         */
        Comparator<Object> getValueComparator(final Tuple3<Key,Document,Map<String,Object>> from);
    }
    
}
