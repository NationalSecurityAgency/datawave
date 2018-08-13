package datawave.query.testframework;

import java.util.Collection;
import java.util.Set;

/**
 * Resolver for a simple query request. A query may consist of one or more simple queries.
 */
public interface IQueryResolver {
    
    /**
     * Returns the entries where the key/value element from the action match.
     *
     * @param field
     *            defines the field within the raw entries
     * @param value
     *            defines the value for operation
     * @param entries
     *            raw data for analysis
     * @return raw entries that are equal
     */
    Set<RawData> isEqual(String field, String value, Collection<RawData> entries);
    
    /**
     * Returns the entries where the key/value from the action do not match the key/value entries.
     *
     * @param field
     *            defines the field within the raw entries
     * @param value
     *            defines the value for operation
     * @param entries
     *            data for analysis
     * @return raw entries that are not equal
     */
    Set<RawData> notEqual(String field, String value, Collection<RawData> entries);
    
    /**
     * Returns the entries where the key/value regular expression from the action match the key/value entries.
     *
     * @param field
     *            defines the field within the raw entries
     * @param value
     *            defines the value for operation
     * @param entries
     *            data for analysis
     * @return raw entries that are not equal
     */
    Set<RawData> regex(String field, String value, Collection<RawData> entries);
    
    /**
     * Returns the entries where the key/value regular expression from the action do not match the key value entries.
     *
     * @param field
     *            defines the field within the raw entries
     * @param value
     *            defines the value for operation
     * @param entries
     *            data for analysis
     * @return raw entries that are not equal
     */
    Set<RawData> negRegex(String field, String value, Collection<RawData> entries);
    
    /**
     * Returns the entries where the key/value element from the action are greater than the key/value entries.
     *
     * @param field
     *            defines the field within the raw entries
     * @param value
     *            defines the value for operation
     * @param entries
     *            data for analysis
     * @return raw entries that are not equal
     */
    Set<RawData> greater(String field, String value, Collection<RawData> entries);
    
    /**
     * Returns the entries where the key/value element from the action are greater than or equal to the key/value entries.
     *
     * @param field
     *            defines the field within the raw entries
     * @param value
     *            defines the value for operation
     * @param entries
     *            data for analysis
     * @return raw entries that are not equal
     */
    Set<RawData> greaterEqual(String field, String value, Collection<RawData> entries);
    
    /**
     * Returns the entries where the key/value element from the action is less than the key/value entries.
     *
     * @param field
     *            defines the field within the raw entries
     * @param value
     *            defines the value for operation
     * @param entries
     *            data for analysis
     * @return raw entries that are not equal
     */
    Set<RawData> less(String field, String value, Collection<RawData> entries);
    
    /**
     * Returns the entries where the key/value element from the action is less than or equal to the key/value entries.
     *
     * @param field
     *            defines the field within the raw entries
     * @param value
     *            defines the value for operation
     * @param entries
     *            data for analysis
     * @return raw entries that are not equal
     */
    Set<RawData> lessEqual(String field, String value, Collection<RawData> entries);
    
}
