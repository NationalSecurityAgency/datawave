package datawave.query.index.lookup;

import java.util.List;
import java.util.Set;

import org.apache.commons.jexl2.parser.JexlNode;

/**
 * Classes that implement this interface can be used to map uids when intersecting two lists of uids.
 */
public interface UidIntersector {
    /**
     * Merge two index infos into one.
     *
     * @param uids1
     *            first set of uids
     * @param uids2
     *            second set of uids
     * @param delayedNodes
     *            list of delayed nodes which is merged in with the node list maintained in the IndexMatches
     * @return A intersected IndexInfo
     */
    Set<IndexMatch> intersect(Set<IndexMatch> uids1, Set<IndexMatch> uids2, List<JexlNode> delayedNodes);

}
