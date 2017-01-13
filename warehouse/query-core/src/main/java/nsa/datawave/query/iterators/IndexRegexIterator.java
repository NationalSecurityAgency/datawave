package nsa.datawave.query.iterators;

import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SkippingIterator;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

@Deprecated
public class IndexRegexIterator extends SkippingIterator implements OptionDescriber {
    
    private IndexRegexFilter ref = new IndexRegexFilter();
    
    @Override
    public IndexRegexIterator deepCopy(IteratorEnvironment env) {
        return new IndexRegexIterator(this, env);
    }
    
    private IndexRegexIterator(IndexRegexIterator other, IteratorEnvironment env) {
        setSource(other.getSource().deepCopy(env));
        ref = other.ref;
    }
    
    public IndexRegexIterator() {
        
    }
    
    private boolean matches(Key key, Value value) {
        return ref.accept(key, value);
    }
    
    @Override
    protected void consume() throws IOException {
        while (getSource().hasTop() && !matches(getSource().getTopKey(), getSource().getTopValue())) {
            getSource().next();
        }
    }
    
    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
        ref.init(source, options, env);
        consume();
    }
    
    @Override
    public IteratorOptions describeOptions() {
        return ref.describeOptions();
    }
    
    @Override
    public boolean validateOptions(Map<String,String> options) {
        return ref.validateOptions(options);
    }
}
