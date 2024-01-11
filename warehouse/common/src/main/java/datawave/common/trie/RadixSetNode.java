package datawave.common.trie;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

class RadixSetNode implements Serializable {
    protected String key;

    protected Object payload;

    protected boolean leaf;

    protected RadixSetNode[] children;

    public static NodeComparator comparator = new NodeComparator();

    public RadixSetNode(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public Object getPayload() {
        return payload;
    }

    public void setPayload(Object payload) {
        this.payload = payload;
    }

    public RadixSetNode[] getChildren() {
        return children;
    }

    public void clearChildren() {
        children = null;
    }

    public void addChild(RadixSetNode child) {
        if (children == null) {
            children = new RadixSetNode[] {child};
        } else {
            RadixSetNode[] newChildren = new RadixSetNode[children.length + 1];
            System.arraycopy(children, 0, newChildren, 0, children.length);
            newChildren[children.length] = child;
            Arrays.sort(newChildren, comparator);
            children = newChildren;
        }
    }

    public void setChildren(RadixSetNode[] children) {
        this.children = children;
    }

    public List<RadixSetNode> children() {
        if (children == null) {
            return Collections.emptyList();
        } else {
            return Arrays.asList(children);
        }
    }

    public boolean isLeaf() {
        return leaf;
    }

    public void setIsLeaf() {
        setIsLeaf(true);
    }

    public void setIsLeaf(boolean leaf) {
        this.leaf = leaf;
    }

    @Override
    public String toString() {
        return key;
    }

    public static class NodeComparator implements Comparator<RadixSetNode> {
        @Override
        public int compare(RadixSetNode one, RadixSetNode other) {
            return one.getKey().compareTo(other.getKey());
        }
    }

}
