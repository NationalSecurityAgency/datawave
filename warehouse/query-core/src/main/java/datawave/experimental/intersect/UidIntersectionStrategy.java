package datawave.experimental.intersect;

import org.apache.commons.jexl2.parser.ASTJexlScript;

import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

/**
 * Interface used for intersection uids from the field index
 */
public interface UidIntersectionStrategy {
    
    SortedSet<String> intersect(ASTJexlScript script, Map<String,Set<String>> nodesToUids);
}
