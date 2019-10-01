package datawave.core.iterators.filter;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * The iterator skips entries in the global index for field names not matching one of a set of matching patterns
 * 
 */
public class GlobalIndexFieldnameMatchingFilter extends Filter {
    
    protected static final Logger log = Logger.getLogger(GlobalIndexFieldnameMatchingFilter.class);
    public static final String LITERAL = "fieldname.literal.";
    public static final String PATTERN = "fieldname.pattern.";
    private Map<String,Pattern> patterns = new HashMap<>();
    private Set<String> literals = new HashSet<>();
    
    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
        
        readOptions(options);
    }
    
    protected void readOptions(Map<String,String> options) {
        int i = 1;
        while (options.containsKey(PATTERN + i)) {
            patterns.put(options.get(PATTERN + i), getPattern(options.get(PATTERN + i)));
            i++;
        }
        i = 1;
        while (options.containsKey(LITERAL + i)) {
            literals.add(options.get(LITERAL + i));
            i++;
        }
        if (patterns.isEmpty() && literals.isEmpty()) {
            throw new IllegalArgumentException("Missing configured patterns for the GlobalIndexFieldnameMatchingFilter: " + options);
        }
        if (log.isDebugEnabled()) {
            log.debug("Set the literals to " + literals);
            log.debug("Set the patterns to " + patterns);
        }
    }
    
    @Override
    public boolean accept(Key k, Value v) {
        // The row is the term
        return matches(k.getColumnFamily().toString());
    }
    
    private Pattern getPattern(String term) {
        return Pattern.compile(term);
    }
    
    private boolean matches(String fieldname) {
        log.trace(fieldname + " -- term");
        
        if (literals.contains(fieldname)) {
            return true;
        }
        
        for (Map.Entry<String,Pattern> entry : patterns.entrySet()) {
            if (entry.getValue().matcher(fieldname).matches()) {
                return true;
            }
        }
        
        return false;
    }
}
