package datawave.core.iterators.filter;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import com.google.protobuf.InvalidProtocolBufferException;
import datawave.ingest.protobuf.Uid;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.log4j.Logger;

/**
 * The iterator skips entries in the global index for entries not matching one of a set of matching patterns
 *
 */
public class GlobalIndexTermMatchingFilter extends Filter {

    protected static final Logger log = Logger.getLogger(GlobalIndexTermMatchingFilter.class);
    public static final String LITERAL = "term.literal.";
    public static final String PATTERN = "term.pattern.";
    public static final String REVERSE_INDEX = "reverse.index";
    private Map<String,Pattern> patterns = new HashMap<>();
    private Set<String> literals = new HashSet<>();
    private boolean reverseIndex = false;
    private String matchedValue = null;

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
            throw new IllegalArgumentException("Missing configured patterns for the GlobalIndexTermMatchingFilter: " + options);
        }
        if (options.containsKey(REVERSE_INDEX)) {
            reverseIndex = Boolean.parseBoolean(options.get(REVERSE_INDEX));
        }
        if (log.isDebugEnabled()) {
            log.debug("Set the literals to " + literals);
            log.debug("Set the patterns to " + patterns);
            log.debug("Set the reverseIndex flag to " + reverseIndex);
        }
    }

    @Override
    public boolean accept(Key k, Value v) {
        // The row is the term
        return matches(k.getRow().toString());
    }

    /**
     * Determine if we have events. For this to be true
     *
     * @param v
     *            a value
     * @return if the value has events
     */
    private boolean hasEvents(final Value v) {
        try {
            Uid.List protobuf = Uid.List.parseFrom(v.get());

            // the protobuf list should be aggregated already
            return protobuf.getIGNORE() || !protobuf.getUIDList().isEmpty();
        } catch (InvalidProtocolBufferException e) {
            // if we cannot parse the protocol buffer, then we
            // won't be able to use it for evaluation anyway
            log.error(e);
            return false;
        }
    }

    private Pattern getPattern(String term) {
        return Pattern.compile(term);
    }

    private boolean matches(String term) {
        matchedValue = null;

        log.trace(term + " -- term");
        if (reverseIndex) {
            StringBuilder buf = new StringBuilder(term);
            term = buf.reverse().toString();

        }

        if (literals.contains(term)) {
            matchedValue = term;
            return true;
        }

        for (Map.Entry<String,Pattern> entry : patterns.entrySet()) {
            if (entry.getValue().matcher(term).matches()) {
                matchedValue = entry.getKey();
                return true;
            }
        }

        return false;
    }

    public String getMatchedValue() {
        return matchedValue;
    }

    public void setMatchedValue(String matchedValue) {
        this.matchedValue = matchedValue;
    }

}
