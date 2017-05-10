package datawave.query.index.lookup;

import org.apache.commons.jexl2.parser.JexlNode;

import java.util.*;

/**
 * Classes that implement this interface can be used to map uids when intersecting two lists of uids.
 */
public interface UidIntersector {
    /**
     * Merge two index infos into one.
     * 
     * @param uids1
     * @param uids2
     * @param delayedNodes
     *            list of delayed nodes which is merged in with the node list maintained in the IndexMatches
     * @return A intersected IndexInfo
     */
    public Set<IndexMatch> intersect(Set<IndexMatch> uids1, Set<IndexMatch> uids2, List<JexlNode> delayedNodes);
    
}
