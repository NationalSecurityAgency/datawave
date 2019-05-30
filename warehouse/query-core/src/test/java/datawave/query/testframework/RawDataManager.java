package datawave.query.testframework;

import datawave.data.normalizer.Normalizer;

import java.io.IOException;
import java.net.URI;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Defines the methods that are required to dynamically resolve and validate query results.
 */
public interface RawDataManager {
    
    /**
     * Provide the {@link BaseShardIdRange} values as the default shard date range.
     */
    ShardIdValues SHARD_ID_VALUES = new ShardIdValues(BaseShardIdRange.getShardDates());
    
    // and/or logical strings for use by unit tests
    String AND_OP = " and ";
    String OR_OP = " or ";
    String NOT_OP = " not ";
    String JEXL_AND_OP = " && ";
    String JEXL_OR_OP = " || ";
    // relationship operators
    String GTE_OP = " >= ";
    String LTE_OP = " <= ";
    String GT_OP = " > ";
    String LT_OP = " < ";
    String EQ_OP = " == ";
    String NE_OP = " != ";
    String RE_OP = " =~ ";
    String RN_OP = " !~ ";
    
    // string and char value for multivalue fields
    String MULTIVALUE_SEP = ";";
    char MULTIVALUE_SEP_CHAR = ';';
    
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
     * Retrieves the all of the raw data for the date range
     *
     * @param start
     *            start date
     * @param end
     *            end date
     * @return raw data entries
     */
    Iterator<Map<String,String>> rangeData(Date start, Date end);
    
    /**
     * Retrieves the key field for all of the entries.
     *
     * @param entries
     *            matching query entries
     * @return key field value for all entries
     */
    Set<String> getKeys(Set<Map<String,String>> entries);
    
    /**
     * Retrieves the normalizer class for a field name/.
     *
     * @param field
     *            name of header field
     * @return normalizer for valid field name
     */
    Normalizer getNormalizer(String field);
    
    /**
     * Converts part of a query using ANY_FIELD into an equivalent string that includes all of the indexed fields. This is performed before the JEXL expression
     * is created. It appears to be more complicated to modify the Jexl script after it has been created. Equivalent to {@link #convertAnyField(String, String)}
     * using ${@link #OR_OP}.
     *
     * @param phrase
     *            execution phrase (e.g. " == 'abc'")
     * @return execution JEXL query compatible to ANY_FIELD execution
     */
    String convertAnyField(String phrase);
    
    /**
     * Converts part of a query using ANY_FIELD into an equivalent string that includes all of the indexed fields. This is performed before the JEXL expression
     * is created. It appears to be more complicated to modify the Jexl script after it has been created.
     *
     * @param phrase
     *            execution phrase (e.g. " == 'abc'")
     * @param op
     *            operation to for the actions between executions (or, and)
     * @return execution JEXL query compatible to ANY_FIELD execution
     */
    String convertAnyField(String phrase, String op);
    
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
