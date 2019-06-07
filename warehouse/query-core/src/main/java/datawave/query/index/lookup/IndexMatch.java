package datawave.query.index.lookup;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;

import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.hadoop.io.WritableComparable;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;

public class IndexMatch implements WritableComparable<IndexMatch> {
    
    protected String shard;
    protected String uid;
    protected Collection<String> nodeStrings;
    protected Collection<JexlNode> myNodes;
    protected IndexMatchType type;
    
    public IndexMatch(final String uid) {
        this(uid, null);
    }
    
    protected IndexMatch() {
        this("", null);
    }
    
    public IndexMatch(final String uid, final JexlNode myNode) {
        this(uid, myNode, IndexMatchType.OR);
    }
    
    public IndexMatch(final String uid, final JexlNode myNode, final String shard) {
        this(uid, myNode);
        this.shard = shard;
    }
    
    public IndexMatch(final String uid, final JexlNode myNode, final IndexMatchType type) {
        this.uid = uid;
        this.myNodes = new HashSet<>();
        this.nodeStrings = new HashSet<>();
        if (null != myNode)
            add(myNode);
        this.type = type;
        this.shard = "";
    }
    
    public IndexMatch(Set<JexlNode> nodes, String uid, final IndexMatchType type) {
        this.uid = uid;
        this.myNodes = new HashSet<>(nodes.size());
        this.nodeStrings = new HashSet<>(nodes.size());
        if (null != nodes) {
            for (JexlNode node : nodes) {
                add(node);
            }
        }
        this.type = type;
        this.shard = "";
    }
    
    private boolean contains(JexlNode node) {
        return nodeStrings.contains(JexlStringBuildingVisitor.buildQueryWithoutParse(node));
    }
    
    public String getUid() {
        return uid;
    }
    
    public JexlNode getNode() {
        if (myNodes.size() == 1) {
            return myNodes.iterator().next();
        } else if (myNodes.isEmpty())
            return null;
        
        switch (type) {
        
            case AND:
                return JexlNodeFactory.createAndNode(myNodes);
            case OR:
            default:
                return JexlNodeFactory.createUnwrappedOrNode(myNodes);
        }
    }
    
    @Override
    public int hashCode() {
        return uid.hashCode();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (null == obj)
            return false;
        if (obj instanceof IndexMatch) {
            return uid.equals(((IndexMatch) obj).uid);
        }
        return false;
    }
    
    public static class IndexUid implements Function<IndexMatch,String> {
        public String apply(IndexMatch indexInfo) {
            return indexInfo.uid;
        }
    }
    
    public static class UidIndex implements Function<String,IndexMatch> {
        public IndexMatch apply(String indexInfo) {
            return new IndexMatch(indexInfo);
        }
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    @Override
    public int compareTo(IndexMatch other) {
        Preconditions.checkNotNull(other);
        return uid.compareTo(other.uid);
    }
    
    public void setType(IndexMatchType type) {
        this.type = type;
    }
    
    /**
     * @param node
     */
    public void set(JexlNode node) {
        myNodes.clear();
        nodeStrings.clear();
        add(node);
    }
    
    /**
     * @param node
     */
    public void add(JexlNode node) {
        if (!contains(node)) {
            nodeStrings.add(JexlStringBuildingVisitor.buildQueryWithoutParse(node));
            myNodes.add(node);
        }
    }
    
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(uid + " - {");
        for (JexlNode node : myNodes) {
            builder.append(JexlStringBuildingVisitor.buildQuery(node)).append(" ");
        }
        builder.append("}");
        
        return builder.toString();
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        uid = in.readUTF();
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(uid);
    }
}
