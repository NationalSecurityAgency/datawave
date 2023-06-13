package datawave.query.enrich;

import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.jexl3.parser.ParseException;

/**
 * <p>
 * {@link DataEnricher} is an abstract implementation to enrich results returned by an iterator.
 * </p>
 * 
 * <p>
 * After instantiating a {@link DataEnricher}, the user must call the {@link #init(SortedKeyValueIterator, Map, IteratorEnvironment)} method. This will
 * initialize the {@link DataEnricher} and prepare it to enrich results.
 * </p>
 * 
 * <p>
 * {@link #enrich(Key, Value)} will perform evaluation directly on the provided {@link Key} and {@link Value}.
 * </p>
 * 
 * 
 * 
 */
public abstract class DataEnricher {
    private Value enrichedValue = null;
    
    /**
     * Initializes the DataEnricher. The options map allows DataEnrichers to pass in unique values through a generic interface. The source must be configured
     * directly onto the underlying tablet. In other words, the source cannot have another custom iterator in between itself and the first configured iterator
     * on the table. The enricher must have full free to seek anywhere inside of the current tablet.
     * 
     * @param source
     *            The configured source on the table
     * @param enricherOptions
     *            An options map to pass in DataEnricher specific values
     * @param env
     *            the environment for the iterator
     * @throws ParseException
     *             for issues parsing
     */
    public abstract void init(SortedKeyValueIterator<Key,Value> source, Map<String,Object> enricherOptions, IteratorEnvironment env) throws ParseException;
    
    /**
     * <p>
     * Abstract method that will add additional information to a Value. To not break the SortedKeyValueIterator's contract of returning Key-Value pairs in
     * sorted order, a DataEnricher cannot change any values in the Key. It may only alter what is stored in the Value.
     * </p>
     * 
     * <p>
     * This method will return true if the DataEnricher calculated a new result. Therefore, if no updated value was created, the caller of the DataEnricher does
     * not need to pull the enrichedValue.
     * </p>
     * 
     * @param key
     *            The key to enrich
     * @param value
     *            The value to enrich
     * @return True if an enriched value was calculated, false otherwise
     */
    public abstract boolean enrich(Key key, Value value);
    
    /**
     * Helper to return the enriched value
     * 
     * @return The enriched value
     */
    public Value getEnrichedValue() {
        return this.enrichedValue;
    }
    
    /**
     * Helper to set the enriched value
     * 
     * @param value
     *            The value to set as the enriched value
     */
    protected void setEnrichedValue(Value value) {
        this.enrichedValue = value;
    }
}
