package datawave.experimental.fi;

import org.apache.commons.jexl2.parser.JexlNode;

import java.util.Map;
import java.util.Set;

/**
 * Do things like naive scanning, pruning scanning, etc
 */
public interface FiScannerStrategy {
    
    Map<String,Set<String>> scanFieldIndexForTerms(String shard, Set<JexlNode> terms, Set<String> indexedFields);
}
