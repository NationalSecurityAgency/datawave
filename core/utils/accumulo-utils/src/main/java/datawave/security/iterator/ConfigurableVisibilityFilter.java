package datawave.security.iterator;

import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.accumulo.core.iteratorsImpl.system.VisibilityFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;

public class ConfigurableVisibilityFilter extends WrappingIterator implements OptionDescriber {
    public static final String AUTHORIZATIONS_OPT = "authorizations";
    private SortedKeyValueIterator<Key,Value> delegate;
    
    private static final Logger log = Logger.getLogger(ConfigurableVisibilityFilter.class);
    
    public ConfigurableVisibilityFilter() {}
    
    public ConfigurableVisibilityFilter(SortedKeyValueIterator<Key,Value> iterator, Authorizations authorizations, byte[] defaultVisibility) {
        setSource(iterator);
    }
    
    public ConfigurableVisibilityFilter(ConfigurableVisibilityFilter other, IteratorEnvironment env) {
        this.delegate = other.delegate.deepCopy(env);
        this.setSource(delegate);
    }
    
    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        Authorizations auths = Authorizations.EMPTY;
        if (options.containsKey(AUTHORIZATIONS_OPT))
            auths = new Authorizations(options.get(AUTHORIZATIONS_OPT).split(","));
        log.debug("Using authorizations: " + auths);
        
        delegate = VisibilityFilter.wrap(source, auths, new byte[0]);
        super.init(delegate, options, env);
    }
    
    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        return new ConfigurableVisibilityFilter(this, env);
    }
    
    @Override
    public IteratorOptions describeOptions() {
        IteratorOptions opts = new IteratorOptions(getClass().getSimpleName(),
                        "Filters keys based to return only those whose visibility tests positive against the supplied authorizations", null, null);
        opts.addNamedOption(AUTHORIZATIONS_OPT, "Comma delimited list of scan authorizations");
        return opts;
    }
    
    @Override
    public boolean validateOptions(Map<String,String> options) {
        boolean valid = false;
        String auths = options.get(AUTHORIZATIONS_OPT);
        if (auths != null) {
            try {
                new Authorizations(auths.split(","));
                valid = true;
            } catch (Exception e) {
                // ignore
            }
        }
        return valid;
    }
}
