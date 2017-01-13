package nsa.datawave.query.iterators;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Iterator used to validate index row values against a regular expression.
 *
 * This class will take row value and match it against the configured regex.
 *
 * Can be configured for forward or reverse matching -- if reverse the row value will be inverted prior to matching it against the configured regex.
 *
 */
@Deprecated
public class IndexRegexFilter extends Filter implements OptionDescriber {
    
    private static final Logger log = LoggerFactory.getLogger(IndexRegexFilter.class);
    
    public static final String REGEX_OPT = "irfRegex";
    public static final String REVERSE_OPT = "irReverse";
    
    private Matcher matcher;
    
    private final Text _row = new Text();
    
    private boolean reverse = false;
    
    @Override
    public boolean accept(Key key, Value value) {
        String target = reverse ? StringUtils.reverse(key.getRow(_row).toString()) : key.getRow(_row).toString();
        
        matcher.reset(target);
        
        if (log.isDebugEnabled()) {
            log.debug("accept(): key=" + key);
            log.debug("accept(): matches reverse=" + reverse + " target=" + target + " matcher=" + matcher);
        }
        
        return matcher.matches();
    }
    
    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) {
        reverse = Boolean.parseBoolean(options.get(REVERSE_OPT));
        
        String regex = options.get(REGEX_OPT);
        Pattern pattern = Pattern.compile(regex != null ? regex : ".*");
        matcher = pattern.matcher("");
    }
    
    @Override
    public IteratorOptions describeOptions() {
        Map<String,String> options = new HashMap<>();
        options.put(IndexRegexFilter.REGEX_OPT, "regular expression on row");
        options.put(IndexRegexFilter.REVERSE_OPT, "reverse match regular expression");
        
        return new IteratorOptions("regex", "The RegExFilter/Iterator allows you to filter for key/value pairs based on regular expressions", options, null);
    }
    
    @Override
    public boolean validateOptions(Map<String,String> options) {
        if (options.containsKey(REGEX_OPT))
            Pattern.compile(options.get(REGEX_OPT)).matcher("");
        return true;
    }
}
