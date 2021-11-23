package datawave.experimental.intersect;

import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

import org.apache.commons.jexl2.parser.ASTJexlScript;

/**
 * Interface used for intersection uids from the field index
 */
public interface UidIntersectionStrategy {

    SortedSet<String> intersect(ASTJexlScript script, Map<String,Set<String>> nodesToUids);
}
