package datawave.query.language.tree;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import datawave.query.search.FieldedTerm;

import org.apache.log4j.Logger;

/**
 * This class serves as the basis for all nodes in the query tree, so all query nodes must extend this class.
 */
public abstract class QueryNode implements Cloneable {

    protected List<Optimization> optimizations = new ArrayList<>();

    protected QueryNode parent;
    protected List<QueryNode> children;
    protected int numSelectors = 1;

    private static final Logger log = Logger.getLogger(QueryNode.class.getName());
    protected String originalQuery = null;
    protected Set<FieldedTerm> positiveFilters = new HashSet<>();
    protected Set<FieldedTerm> negativeFilters = new HashSet<>();

    public QueryNode(QueryNode... children) {
        this.children = new LinkedList<>();
        for (QueryNode child : children) {
            if (child != null) {
                this.children.add(child);
                child.setParent(this);
            }
        }
    }

    public void setChildren(List<QueryNode> children) {
        this.children = children;

        for (QueryNode child : children) {
            if (child != null) {
                child.setParent(this);
            }
        }
    }

    public List<QueryNode> getChildren() {
        return children;
    }

    public void deleteChildren() {
        this.children = new LinkedList<>();
    }

    public void addChild(QueryNode child) {
        if (child != null) {
            if (this.children == null)
                this.children = new LinkedList<>();
            this.children.add(child);
        }
    }

    public void addOptimization(Optimization optimization) {
        this.optimizations.add(optimization);
    }

    public void setOptimizations(List<Optimization> optimizations) {
        this.optimizations.clear();
        this.optimizations.addAll(optimizations);
    }

    /**
     * determines if this nodes parent is a different operation. used to determine when to modify the weights
     *
     * @return if this nodes parent is a different operation
     */
    protected abstract boolean isParentDifferent();

    /**
     * set the number of selector nodes(leaves) in the tree so document scores can later be calculated
     *
     * @param num
     *            int Number of selectors
     */
    public void setNumLeaves(int num) {
        numSelectors = num;
    }

    /**
     * @return true if this node is a selector
     */
    public boolean isLeaf() {
        return children.isEmpty();
    }

    /**
     * sets the parent of this node
     *
     * @param p
     *            a query node
     */
    private void setParent(QueryNode p) {
        parent = p;
    }

    /**
     * @return the type(if node is an op) or contents(if node is a selector)
     */
    @Override
    public abstract String toString();

    /**
     * @return preorder traversal of this node's subtree
     */
    public String getContents() {
        StringBuilder s = new StringBuilder("[");
        s.append(this);
        for (QueryNode child : children) {
            s.append(",");
            s.append(child.getContents());
        }
        s.append("]");
        if (!positiveFilters.isEmpty()) {
            s.append("[posFilter: ");
            boolean firstPos = true;
            for (FieldedTerm ft : positiveFilters) {
                if (!firstPos) {
                    s.append(",");
                }
                s.append(ft);
                firstPos = false;
            }

            s.append("]");
        }
        if (!negativeFilters.isEmpty()) {
            s.append("[negFilter: ");
            boolean firstNeg = true;
            for (FieldedTerm ft : negativeFilters) {
                if (!firstNeg) {
                    s.append(",");
                }
                s.append(ft.getField());
                s.append(":");
                s.append(ft.getSelector());
                firstNeg = false;
            }

            s.append("]");
        }

        return s.toString();
    }

    /**
     * Makes a copy of the tree
     *
     * @return a copy of the tree represented by this QueryNode
     */
    public abstract QueryNode clone();

    public String getOriginalQuery() {
        return this.originalQuery;
    }

    public void setOriginalQuery(String origQuery) {
        this.originalQuery = origQuery;
    }

    public Set<FieldedTerm> getPositiveFilters() {
        return positiveFilters;
    }

    public void setPositiveFilters(Set<FieldedTerm> positiveFilters) {
        this.positiveFilters = positiveFilters;
    }

    public Set<FieldedTerm> getNegativeFilters() {
        return negativeFilters;
    }

    public void setNegativeFilters(Set<FieldedTerm> negativeFilters) {
        this.negativeFilters = negativeFilters;
    }
}
