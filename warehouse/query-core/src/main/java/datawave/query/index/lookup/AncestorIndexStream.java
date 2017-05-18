package datawave.query.index.lookup;

import datawave.query.util.Tuple2;
import org.apache.commons.jexl2.parser.JexlNode;

import java.util.*;

/**
 * Specialty AncestorIndexStream implementation. Wraps existing IndexStream delegate to prevent returning overlapping ranges from ancestors during expansions
 * which would otherwise result in duplicate hits
 */
public class AncestorIndexStream implements IndexStream {
    private final IndexStream delegate;
    private final JexlNode parent;
    
    public AncestorIndexStream(IndexStream delegate) {
        this(delegate, null);
    }
    
    public AncestorIndexStream(IndexStream delegate, JexlNode parent) {
        this.delegate = delegate;
        this.parent = parent;
    }
    
    @Override
    public StreamContext context() {
        return delegate.context();
    }
    
    @Override
    public String getContextDebug() {
        return delegate.getContextDebug();
    }
    
    @Override
    public JexlNode currentNode() {
        return delegate.currentNode();
    }
    
    @Override
    public Tuple2<String,IndexInfo> peek() {
        return removeOverlappingRanges(delegate.peek());
    }
    
    @Override
    public boolean hasNext() {
        return delegate.hasNext();
    }
    
    @Override
    public Tuple2<String,IndexInfo> next() {
        return removeOverlappingRanges(delegate.next());
    }
    
    @Override
    public void remove() {
        delegate.remove();
    }
    
    /**
     * For a given tuple, remove any matches that are descendants of other matches. This prevents the same uid from being evaluated multiple times and
     * potentially returning duplicate documents
     * 
     * @param tuple
     * @return
     */
    private Tuple2<String,IndexInfo> removeOverlappingRanges(Tuple2<String,IndexInfo> tuple) {
        IndexInfo info = tuple.second();
        Set<IndexMatch> matches = info.uids();
        
        Map<MatchGroup,Set<IndexMatch>> nodeMap = new HashMap<>();
        for (IndexMatch match : matches) {
            MatchGroup matchGroup = new MatchGroup(match);
            
            Set<IndexMatch> existing = nodeMap.get(matchGroup);
            if (existing == null) {
                existing = new TreeSet<>();
                nodeMap.put(matchGroup, existing);
            }
            
            boolean add = true;
            Iterator<IndexMatch> existingIterator = existing.iterator();
            while (existingIterator.hasNext() && add) {
                IndexMatch indexMatch = existingIterator.next();
                if (match.getUid().indexOf(indexMatch.getUid()) > -1) {
                    add = false;
                }
            }
            
            if (add) {
                existing.add(match);
            }
        }
        
        // aggregate all the TreeSets into a single set
        Set<IndexMatch> allMatches = new TreeSet<>();
        for (Set<IndexMatch> nodeMatches : nodeMap.values()) {
            allMatches.addAll(nodeMatches);
        }
        IndexInfo newInfo = new IndexInfo(allMatches);
        newInfo.myNode = info.myNode;
        
        return new Tuple2<>(tuple.first(), newInfo);
    }
    
    private static class MatchGroup {
        Collection<String> nodeStrings;
        
        public MatchGroup(IndexMatch match) {
            nodeStrings = match.nodeStrings;
        }
        
        @Override
        public int hashCode() {
            int hashCode = 0;
            for (String nodeString : nodeStrings) {
                hashCode += nodeString.hashCode();
            }
            return hashCode;
        }
        
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof MatchGroup)) {
                return false;
            }
            
            MatchGroup matchGroup = (MatchGroup) obj;
            return new HashSet(nodeStrings).equals(new HashSet(matchGroup.nodeStrings));
        }
    }
}
