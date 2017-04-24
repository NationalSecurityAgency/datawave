package nsa.datawave.query.filter;

import java.util.Map;

import nsa.datawave.query.parser.EventFields;
import nsa.datawave.query.parser.QueryEvaluator;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.parser.ParseException;

/**
 * <p>
 * {@link DataFilter} is an abstract implementation to filter out results returned by an iterator.
 * </p>
 * 
 * <p>
 * After instantiating a {@link DataFilter}, the user must call the {@link #init(SortedKeyValueIterator, Map<String, Object>)} method. This will initialize the
 * {@link DataFilter} and prepare it to filter results.
 * </p>
 * 
 * <p>
 * The user can then use the {@link #accept(Key, Value)} or {@link #getContextMap(Key, Value)} methods to filter results:
 * <ul>
 * <li>{@link #accept(Key, Value)} will perform evaluation directly on the provided {@link Key} and {@link Value}</li>
 * <li>{@link #getContextMap(Key, Value)} will gather the necessary additional information needed to perform the implemented filtering algorithm into a
 * <code>Map&lt;String, Object&gt;</code> that can be loaded into a {@link JexlContext} and evaluated with a {@link org.apache.commons.jexl2.Expression}</li>
 * </ul>
 * </p>
 * 
 * 
 * 
 */
public abstract class DataFilter {
    
    /**
     * Initialization method for all DataFilters. The {@link SortedKeyValueIterator} should point directly to the current tablet the {@link Scanner} is using
     * (i.e. the source should not be a custom sub-iterator). The query and any unevaluated fields (e.g. "BODY", "HEAD", etc) should also be provided.
     * 
     * @param source
     *            A SortedKeyValueIterator that points directly to the current tablet
     * @param query
     *            The query being operated on
     * @param unevaluatedFields
     *            Any unevaluated fields
     * @throws ParseException
     */
    public abstract void init(SortedKeyValueIterator<Key,Value> source, Map<String,Object> options, QueryEvaluator evaluator) throws ParseException;
    
    /**
     * Given a key/value pair, {@link #accept(Key, Value)} determines if the pair should be returned to the caller or be filtered out and not returned.
     * 
     * @param key
     *            The current {@link Key}
     * @param value
     *            The current {@link Value}
     * @return True if the value should be returned to the caller, false otherwise
     */
    public abstract boolean accept(Key key, Value value);
    
    /**
     * Given a key/value pair, {@link #getContextMap(Key, Value)} generates a <code>Map&lt;String, Object&gt;</code> with the necessary values to needed by Jexl
     * to perform the filtering in a single {@link #Event} evaluation.
     * 
     * @param key
     * @param value
     * @return
     * @see nsa.datawave.query.parser.QueryEvaluator#evaluate(EventFields, Map)
     */
    public abstract Map<String,Object> getContextMap(Key key, Value value);
}
