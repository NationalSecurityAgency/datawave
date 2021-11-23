package datawave.experimental.fi;

import java.util.Set;

import org.apache.commons.jexl2.parser.ASTJexlScript;

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
    Set<String> scan(ASTJexlScript script, String row, Set<String> indexedFields);
}
