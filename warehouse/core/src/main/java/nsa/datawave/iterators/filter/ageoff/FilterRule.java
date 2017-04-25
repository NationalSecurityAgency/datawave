package nsa.datawave.iterators.filter.ageoff;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

/**
 * This interface defines the methods for rules that are defined within a {@code ConfigurableAgeOffFilter} object.
 */
public interface FilterRule {
    /**
     * Used to initialize the the {@code FilterRule} implementation
     * 
     * @param options
     *            {@code Map} object
     */
    public void init(FilterOptions options);
    
    /**
     * Used to test a {@code Key/Value} pair, and returns {@code true} if it is accepted
     * 
     * @return {@code boolean} value.
     */
    public boolean accept(SortedKeyValueIterator<Key,Value> iter);
    
    public FilterRule decorate(Object decoratedObject);
    
    public FilterRule deepCopy(AgeOffPeriod period);
    
    /**
     * @param scanStart
     * @return
     */
    public FilterRule deepCopy(long scanStart);
    
}
