package datawave.core.iterators;

import datawave.core.iterators.filter.GlobalIndexFieldnameMatchingFilter;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public class GlobalIndexFieldnameMatchingIterator extends GlobalIndexFieldnameMatchingFilter implements SortedKeyValueIterator<Key,Value>, OptionDescriber {
    
    private static final Logger log = Logger.getLogger(GlobalIndexFieldnameMatchingIterator.class);
    
    private SortedKeyValueIterator<Key,Value> source;
    
    private boolean foundMatch = false;
    
    public GlobalIndexFieldnameMatchingIterator() throws IOException {}
    
    public GlobalIndexFieldnameMatchingIterator deepCopy(IteratorEnvironment env) {
        return new GlobalIndexFieldnameMatchingIterator(this, env);
    }
    
    private GlobalIndexFieldnameMatchingIterator(GlobalIndexFieldnameMatchingIterator other, IteratorEnvironment env) {
        setSource(other.getSource().deepCopy(env));
    }
    
    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
        if (!validateOptions(options))
            throw new IOException("Iterator options are not correct");
        setSource(source);
    }
    
    @Override
    public Key getTopKey() {
        Key key = null;
        if (foundMatch) {
            key = getSource().getTopKey();
        }
        return key;
    }
    
    @Override
    public Value getTopValue() {
        if (foundMatch) {
            return getSource().getTopValue();
        } else {
            return null;
        }
    }
    
    @Override
    public boolean hasTop() {
        return foundMatch && getSource().hasTop();
    }
    
    @Override
    public void next() throws IOException {
        getSource().next();
        findTopEntry();
    }
    
    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        getSource().seek(range, columnFamilies, inclusive);
        findTopEntry();
    }
    
    private void findTopEntry() throws IOException {
        foundMatch = false;
        if (log.isTraceEnabled())
            log.trace("has top ? " + getSource().hasTop());
        while (!foundMatch && getSource().hasTop()) {
            Key top = getSource().getTopKey();
            if (log.isTraceEnabled())
                log.trace("top key is " + top);
            if (accept(top, getSource().getTopValue())) {
                foundMatch = true;
            } else {
                getSource().next();
            }
        }
    }
    
    protected void setSource(SortedKeyValueIterator<Key,Value> source) {
        this.source = source;
    }
    
    protected SortedKeyValueIterator<Key,Value> getSource() {
        return source;
    }
    
    @Override
    public IteratorOptions describeOptions() {
        IteratorOptions io = super.describeOptions();
        io.addNamedOption(LITERAL + "i", "A literal fieldname to match");
        io.addNamedOption(PATTERN + "i", "A regex fieldname to match");
        io.setDescription("GlobalIndexFieldnameMatchingIterator uses a set of literals and regexs to match global index key fieldnames");
        return io;
    }
    
}
