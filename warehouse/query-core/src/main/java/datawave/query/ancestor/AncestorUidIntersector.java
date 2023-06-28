package datawave.query.ancestor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.jexl2.parser.JexlNode;

import com.google.common.collect.Sets;

import datawave.data.hash.UIDConstants;
import datawave.query.index.lookup.IndexMatch;
import datawave.query.index.lookup.IndexMatchType;
import datawave.query.index.lookup.UidIntersector;
import datawave.query.language.parser.jexl.JexlNodeSet;
import datawave.query.tld.TLD;
import datawave.query.util.Tuple2;

/**
 * This is a uid intersection class that handles the concept of the ancestor query logic.
 */
public class AncestorUidIntersector implements UidIntersector {
    @Override
    public Set<IndexMatch> intersect(Set<IndexMatch> uids1, Set<IndexMatch> uids2, List<JexlNode> delayedNodes) {
        /*
         * C) Both are small, so we have an easy case where we can prune much of this sub query. Must propagate delayed nodes, though.
         */

        // create a map of correlated UIDS mapped to the root uid. The values keep the two lists of uids separate
        Map<String,Tuple2<ArrayList<IndexMatch>,ArrayList<IndexMatch>>> correlatedUids = new HashMap<>();

        // put the first set of uids in the correlated list
        for (IndexMatch match1 : uids1) {
            String baseUid = TLD.parseRootPointerFromId(match1.getUid());
            Tuple2<ArrayList<IndexMatch>,ArrayList<IndexMatch>> indexMatchLists = correlatedUids.get(baseUid);
            if (indexMatchLists == null) {
                indexMatchLists = new Tuple2<>(new ArrayList<>(), new ArrayList<>());
                correlatedUids.put(baseUid, indexMatchLists);
            }
            indexMatchLists.first().add(match1);
        }

        // put the second set of uids in the correlated list
        for (IndexMatch match2 : uids2) {
            String baseUid = TLD.parseRootPointerFromId(match2.getUid());
            Tuple2<ArrayList<IndexMatch>,ArrayList<IndexMatch>> indexMatchLists = correlatedUids.get(baseUid);
            if (indexMatchLists == null) {
                indexMatchLists = new Tuple2<>(new ArrayList<>(), new ArrayList<>());
                correlatedUids.put(baseUid, indexMatchLists);
            }
            indexMatchLists.second().add(match2);
        }

        // now for each base uid, if we have uids in the two lists then remap them to the descendent furthest from the root
        Set<IndexMatch> matches = new HashSet<>();
        for (Tuple2<ArrayList<IndexMatch>,ArrayList<IndexMatch>> indexMatchLists : correlatedUids.values()) {
            if (!indexMatchLists.first().isEmpty() && !indexMatchLists.second().isEmpty()) {
                for (IndexMatch uid1 : indexMatchLists.first()) {
                    for (IndexMatch uid2 : indexMatchLists.second()) {
                        // if uid1 starts with uid2, then uid1 is a descendent of uid2
                        if (uid1.getUid().startsWith(uid2.getUid() + UIDConstants.DEFAULT_SEPARATOR) || uid1.getUid().equals(uid2.getUid())) {
                            JexlNodeSet nodeSet = new JexlNodeSet();
                            nodeSet.add(uid1.getNode());
                            nodeSet.add(uid2.getNode());
                            nodeSet.addAll(delayedNodes);
                            IndexMatch currentMatch = new IndexMatch(Sets.newHashSet(nodeSet.getNodes()), uid1.getUid(), IndexMatchType.AND);
                            matches = reduce(matches, currentMatch);
                        }
                        // if uid2 starts with uid1, then uid2 is a descendent of uid1
                        else if (uid2.getUid().startsWith(uid1.getUid() + UIDConstants.DEFAULT_SEPARATOR)) {
                            JexlNodeSet nodeSet = new JexlNodeSet();
                            nodeSet.add(uid1.getNode());
                            nodeSet.add(uid2.getNode());
                            nodeSet.addAll(delayedNodes);
                            IndexMatch currentMatch = new IndexMatch(Sets.newHashSet(nodeSet), uid2.getUid(), IndexMatchType.AND);
                            matches = reduce(matches, currentMatch);
                        }
                    }
                }
            }
        }

        return matches;
    }

    private Set<IndexMatch> reduce(Set<IndexMatch> matches, IndexMatch currentMatch) {
        Set<IndexMatch> result = Sets.newHashSet();
        boolean conflict = false;
        for (IndexMatch match : matches) {
            if (!match.getUid().startsWith(currentMatch.getUid() + UIDConstants.DEFAULT_SEPARATOR) || match.getUid().equals(currentMatch.getUid())) {
                result.add(match);
            }

            if (currentMatch.getUid().startsWith(match.getUid() + UIDConstants.DEFAULT_SEPARATOR) || match.getUid().equals(currentMatch.getUid())) {
                conflict = true;
            }
        }

        if (!conflict) {
            result.add(currentMatch);
        }

        return result;
    }
}
