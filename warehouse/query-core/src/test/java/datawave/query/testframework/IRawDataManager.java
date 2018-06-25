package datawave.query.testframework;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.URI;
import java.util.Date;
import java.util.List;
import java.util.Set;

/**
 * Defines the methods that are required to dynamically resolve and validate query results.
 */
public interface IRawDataManager {
    
    String MULTIVALUE_SEP = ";";
    
    /**
     * Returns the field headers for this data manager.
     * 
     * @return headers strings
     */
    List<String> getHeaders();
    
    /**
     * Loads the contents of a raw file into the a set of POJOs.
     *
     * @param file
     *            ingest file name
     * @param datatype
     *            datatype for ingest data
     * @param indexes
     *            configured indexes
     * @throws IOException
     *             error processing ingest file
     */
    void addTestData(URI file, String datatype, Set<String> indexes) throws IOException;
    
    /**
     * Finds all unique keys that match the query from the loaded test data.
     *
     * @param action
     *            defines a simple query action
     * @param startDate
     *            start date for query
     * @param endDate
     *            end date for query
     * @return set of keys expected to match the query
     */
    Set<IRawData> findMatchers(QueryAction action, Date startDate, Date endDate);
    
    /**
     * Returns the type for a field.
     *
     * @param field
     *            name of field
     * @return type
     */
    Type getFieldType(String field);
    
    /**
     * Retrieves the set of key values from the query results dataset.
     *
     * @param entries
     *            raw data entries matching the query
     * @return set of keys for the raw data entries
     */
    Set<String> getKeyField(Set<IRawData> entries);
    
    /**
     * Creates an array that contains a random start and end date based upon the shard dates specified for the raw data.
     *
     * @return [0] => start date; [1] => end date
     */
    Date[] getRandomStartEndDate();
    
    /**
     * Creates an array that contains the start and end date based upon the shard dates specified for the raw data.
     *
     * @return [0] => start date; [1] => end date
     */
    Date[] getShardStartEndDate();
}
