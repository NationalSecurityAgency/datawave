package datawave.experimental.fi;

import java.util.Set;
import java.util.SortedSet;

import org.apache.commons.jexl3.parser.ASTJexlScript;

import datawave.query.predicate.TimeFilter;

/**
 * Scans the field index for uids given a jexl script.
 */
public interface UidScanner {

    /**
     * Scan the field index for all document uid that satisfy the query script
     *
     * @param script
     *            the jexl script
     * @param row
     *            the row, or shard
     * @param indexedFields
     *            the set of indexed fields in the query
     * @return a set of uids that satisfy the query
     */
    SortedSet<String> scan(ASTJexlScript script, String row, Set<String> indexedFields);

    /**
     * TimeFilter used to filter keys when the date range is intra-day
     *
     * @param timeFilter
     *            a time filter
     */
    void withTimeFilter(TimeFilter timeFilter);

    /**
     * Set of datatypes to filter on. Null or empty set means allow all datatypes
     *
     * @param datatypeFilter
     *            a set of datatypes to filter on
     */
    void withDatatypeFilter(Set<String> datatypeFilter);

    /**
     * Should this scanner log summary stats
     *
     * @param logStats
     *            flag for logging
     */
    void setLogStats(boolean logStats);
}
