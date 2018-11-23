package datawave.query.discovery;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import datawave.marking.MarkingFunctions;
import datawave.marking.VisibilityFlattener;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.ColumnVisibility.Node;
import org.apache.accumulo.core.security.ColumnVisibility.NodeType;
import org.apache.accumulo.core.security.VisibilityEvaluator;
import org.apache.accumulo.core.security.VisibilityParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;

public class VisibilityPruningIterator extends WrappingIterator implements OptionDescriber {
    public static final String BLACKLIST = "blacklist", AUTHORIZATIONS = "auths";
    
    private static final Logger log = Logger.getLogger(VisibilityPruningIterator.class);
    
    protected static final MarkingFunctions markingFunctions = MarkingFunctions.Factory.createMarkingFunctions();
    
    private ImmutableSet<String> blacklist;
    private ImmutableSet<Authorizations> authorizations;
    private final Text visibilityBuffer = new Text();
    
    public VisibilityPruningIterator() {}
    
    public VisibilityPruningIterator(VisibilityPruningIterator o) {
        blacklist = o.blacklist;
        authorizations = o.authorizations;
    }
    
    @Override
    public VisibilityPruningIterator deepCopy(IteratorEnvironment env) {
        VisibilityPruningIterator i = new VisibilityPruningIterator(this);
        i.setSource(getSource().deepCopy(env));
        return i;
    }
    
    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
        validateOptions(options);
    }
    
    @Override
    public IteratorOptions describeOptions() {
        return new IteratorOptions(this.getClass().getName(), "Prunes the visibility label of undesirable visibilities and visibility substrings.",
                        ImmutableMap.of(BLACKLIST, "CSV list of visibility labels to filter out"), null);
    }
    
    @Override
    public boolean validateOptions(Map<String,String> options) {
        String arg = options.get(BLACKLIST);
        blacklist = parseBlacklist(arg == null ? "" : arg);
        arg = options.get(AUTHORIZATIONS);
        authorizations = parseAuthorizations(arg == null ? "" : arg);
        return true;
    }
    
    @Override
    public Key getTopKey() {
        Key top = getSource().getTopKey();
        ColumnVisibility newVisibility = pruneVisibility(blacklist, new ColumnVisibility(top.getColumnVisibility(visibilityBuffer)), authorizations);
        byte[] newFlatVisibility = markingFunctions.flatten(newVisibility);
        return new Key(top.getRowData().getBackingArray(), top.getRowData().offset(), top.getRowData().length(), top.getColumnFamilyData().getBackingArray(),
                        top.getColumnFamilyData().offset(), top.getColumnFamilyData().length(), top.getColumnQualifierData().getBackingArray(), top
                                        .getColumnQualifierData().offset(), top.getColumnQualifierData().length(), newFlatVisibility, 0,
                        newFlatVisibility.length, top.getTimestamp());
    }
    
    public static ImmutableSet<String> parseBlacklist(String csv) {
        String[] list = StringUtils.split(csv, ',');
        ImmutableSet.Builder<String> builder = ImmutableSortedSet.naturalOrder();
        for (String item : list)
            builder.add(item);
        return builder.build();
    }
    
    public static ImmutableSet<Authorizations> parseAuthorizations(String serializedAuths) {
        ImmutableSet.Builder<Authorizations> builder = ImmutableSet.builder();
        for (String singleAuthSet : StringUtils.split(serializedAuths, ';')) {
            String[] auths = StringUtils.split(singleAuthSet, ',');
            builder.add(new Authorizations(auths));
        }
        return builder.build();
    }
    
    public static ColumnVisibility pruneVisibility(Set<String> blacklist, ColumnVisibility visibility, Collection<Authorizations> authorizations) {
        Node node = visibility.getParseTree();
        
        if (node.getType() == NodeType.OR) {
            if (log.isTraceEnabled()) {
                log.trace("Top level OR Node, removing unsatisfied branches from: " + visibility);
            }
            byte[] expression = visibility.getExpression();
            if (authorizations != null) {
                HashSet<VisibilityEvaluator> ve = new HashSet<>();
                for (Authorizations a : authorizations)
                    ve.add(new VisibilityEvaluator(a));
                removeUnsatisfiedTopLevelOrNodes(ve, expression, node);
            }
            // re-assigning visibility and modifiableNode with changes
            visibility = VisibilityFlattener.flatten(node, expression, false);
            if (log.isTraceEnabled()) {
                log.trace("removed unsatisfied branches, visibility now: " + visibility);
            }
            node = visibility.getParseTree();
        }
        
        byte[] expression = visibility.getExpression();
        
        if (!blacklist.isEmpty()) {
            if (log.isTraceEnabled()) {
                log.trace("removing undisplayedVisibilities: " + blacklist + " from " + visibility);
            }
            removeUndisplayedVisibilities(blacklist, expression, node);
            visibility = VisibilityFlattener.flatten(node, expression, false);
            if (log.isTraceEnabled()) {
                log.trace("removed undisplayedVisibilities: " + blacklist + ", visibility now " + visibility);
            }
        }
        
        return visibility;
    }
    
    public static void removeUndisplayedVisibilities(Set<String> blacklist, byte[] expression, Node node) {
        List<Node> children = node.getChildren();
        // walk backwards so we don't change the index that we need to remove
        int lastNode = children.size() - 1;
        for (int x = lastNode; x >= 0; x--) {
            Node currNode = children.get(x);
            if (currNode.getType() == NodeType.TERM) {
                String nodeString = termNodeToString(expression, currNode);
                if (blacklist.contains(nodeString)) {
                    children.remove(x);
                }
            } else {
                removeUndisplayedVisibilities(blacklist, expression, currNode);
                // if all children of this NodeType.AND or NodeType.OR node have been removed, remove the node itself.
                if (currNode.getChildren().size() == 0) {
                    children.remove(x);
                }
            }
        }
    }
    
    public static String termNodeToString(byte[] expression, Node termNode) {
        int start = termNode.getTermStart();
        int end = termNode.getTermEnd();
        return new String(expression, start, end - start);
    }
    
    public static void removeUnsatisfiedTopLevelOrNodes(Set<VisibilityEvaluator> ve, byte[] expression, Node node) {
        if (node.getType() == NodeType.OR) {
            List<Node> children = node.getChildren();
            int lastNode = children.size() - 1;
            for (int x = lastNode; x >= 0; x--) {
                Node currNode = children.get(x);
                boolean remove = isUnsatisfied(x, ve, currNode, expression);
                if (remove) {
                    children.remove(x);
                }
            }
        }
    }
    
    public static boolean isUnsatisfied(int position, Set<VisibilityEvaluator> ve, Node currNode, byte[] expression) {
        boolean unsatisfied = false;
        try {
            ColumnVisibility currVis = VisibilityFlattener.flatten(currNode, expression, false);
            for (VisibilityEvaluator v : ve) {
                if (!v.evaluate(currVis))
                    unsatisfied = true;
            }
        } catch (VisibilityParseException e) {
            log.trace(e);
            
        }
        return unsatisfied;
    }
}
