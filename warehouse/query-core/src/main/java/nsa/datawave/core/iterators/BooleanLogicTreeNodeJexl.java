/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nsa.datawave.core.iterators;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.TreeNode;

import nsa.datawave.query.iterators.JumpingIterator;
import nsa.datawave.query.iterators.JumpSeek;
import nsa.datawave.query.parser.JexlOperatorConstants;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.jexl2.parser.ParserTreeConstants;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

/**
 *
 *
 */
@Deprecated
public class BooleanLogicTreeNodeJexl extends DefaultMutableTreeNode implements JumpSeek<Key> {
    
    private static final long serialVersionUID = 1L;
    protected static final Logger log = Logger.getLogger(BooleanLogicTreeNodeJexl.class);
    private Key myTopKey = null;
    private Key advanceKey = null;
    private Text fValue = null;
    private Text fName = null;
    // this is the negated flag in the underlying query
    private boolean queryNodeNegated = false;
    // this is what the BooleanLogicIteratorJexl is handling the negation as
    private boolean negated = false;
    private int type;
    private boolean done = false;
    private boolean valid = false;
    private boolean rollUp = false;
    private String fOperator = null;
    private boolean childrenAllNegated = false;
    private HashSet<Key> uids;
    private Text upperBound = null;
    private boolean upperInclusive = true;
    private Text lowerBound = null;
    private boolean lowerInclusive = true;
    private boolean rangeNode = false;
    public static final String NULL_BYTE_STRING = "\0";
    private List<BooleanLogicTreeNodeJexl> rolledUpChildren;
    
    public BooleanLogicTreeNodeJexl() {
        super();
        uids = new HashSet<>();
    }
    
    public BooleanLogicTreeNodeJexl(int type) {
        this(type, false);
    }
    
    public BooleanLogicTreeNodeJexl(int type, boolean negate) {
        this(type, null, null, negate);
    }
    
    public BooleanLogicTreeNodeJexl(int type, String fieldName, String fieldValue) {
        this(type, fieldName, fieldValue, false);
    }
    
    public BooleanLogicTreeNodeJexl(int type, String fieldName, String fieldValue, boolean negated) {
        super();
        this.type = type;
        if (fieldValue != null) {
            this.fValue = new Text(fieldValue);
        }
        if (fieldName != null) {
            this.fName = new Text(fieldName);
        }
        uids = new HashSet<>();
        this.queryNodeNegated = this.negated = negated;
        setOperator();
    }
    
    public BooleanLogicTreeNodeJexl(int type, String fieldName, String lowerValue, boolean lowerInclusive, String upperValue, boolean upperInclusive,
                    boolean negated) {
        super();
        this.type = type;
        this.rangeNode = true;
        if (lowerValue != null) {
            this.lowerBound = new Text(lowerValue);
        }
        this.lowerInclusive = lowerInclusive;
        if (upperValue != null) {
            this.upperBound = new Text(upperValue);
        }
        this.upperInclusive = upperInclusive;
        if (fieldName != null) {
            this.fName = new Text(fieldName);
        }
        uids = new HashSet<>();
        this.queryNodeNegated = this.negated = negated;
        setOperator();
    }
    
    public void setValid(boolean b) {
        this.valid = b;
    }
    
    public boolean isValid() {
        return this.valid;
    }
    
    public void setType(int t) {
        this.type = t;
    }
    
    public int getType() {
        return this.type;
    }
    
    public void setChildrenAllNegated(boolean childrenAllNegated) {
        this.childrenAllNegated = childrenAllNegated;
    }
    
    public boolean isChildrenAllNegated() {
        return childrenAllNegated;
    }
    
    public List<BooleanLogicTreeNodeJexl> getRolledUpChildren() {
        return rolledUpChildren;
    }
    
    @SuppressWarnings("unchecked")
    public void rollUpChildren() {
        if (rolledUpChildren == null) {
            rolledUpChildren = new ArrayList<>(super.children.size());
        }
        
        rolledUpChildren.addAll(super.children);
        super.removeAllChildren();
    }
    
    public void setAdvanceKey(Key advanceKey) {
        this.advanceKey = advanceKey;
    }
    
    public Key getAdvanceKey() {
        return advanceKey;
    }
    
    public void setTopKey(Key id) {
        this.myTopKey = id;
    }
    
    public Key getTopKey() {
        return myTopKey;
    }
    
    public void setDone(boolean done) {
        this.done = done;
    }
    
    public boolean isDone() {
        return done;
    }
    
    public void setRollUp(boolean rollUp) {
        this.rollUp = rollUp;
    }
    
    public boolean isRollUp() {
        return rollUp;
    }
    
    public Text getFieldValue() {
        return fValue;
    }
    
    public void setFieldValue(Text term) {
        this.fValue = term;
    }
    
    public Text getFieldName() {
        return fName;
    }
    
    public void setFieldName(Text dataLocation) {
        this.fName = dataLocation;
    }
    
    public boolean isQueryNodeNegated() {
        return queryNodeNegated;
    }
    
    public void setQueryNodeNegated(boolean queryNodeNegated) {
        this.queryNodeNegated = queryNodeNegated;
    }
    
    public boolean isNegated() {
        return negated;
    }
    
    public void setNegated(boolean negated) {
        this.negated = negated;
    }
    
    public String getFieldOperator() {
        return fOperator;
    }
    
    private void setOperator() {
        this.fOperator = JexlOperatorConstants.getOperator(type);
        if (negated && this.fOperator.equals("!=")) {
            this.fOperator = JexlOperatorConstants.getOperator(JexlOperatorConstants.JJTEQNODE);
        }
    }
    
    public Text getLowerBound() {
        return lowerBound;
    }
    
    public void setLowerBound(Text lowerBound) {
        this.lowerBound = lowerBound;
    }
    
    public Text getUpperBound() {
        return upperBound;
    }
    
    public void setUpperBound(Text upperBound) {
        this.upperBound = upperBound;
    }
    
    public boolean isUpperInclusive() {
        return upperInclusive;
    }
    
    public void setUpperInclusive(boolean upperInclusive) {
        this.upperInclusive = upperInclusive;
    }
    
    public boolean isLowerInclusive() {
        return lowerInclusive;
    }
    
    public void setLowerInclusive(boolean lowerInclusive) {
        this.lowerInclusive = lowerInclusive;
    }
    
    public boolean isRangeNode() {
        return rangeNode;
    }
    
    public void setRangeNode(boolean rangeNode) {
        this.rangeNode = rangeNode;
    }
    
    public String getContents() {
        StringBuilder s = new StringBuilder("[");
        s.append(toString());
        
        if (children != null) {
            Enumeration<?> e = this.children();
            while (e.hasMoreElements()) {
                BooleanLogicTreeNodeJexl n = (BooleanLogicTreeNodeJexl) e.nextElement();
                s.append(",");
                s.append(n.getContents());
            }
        }
        s.append("]");
        return s.toString();
    }
    
    public String printNode() {
        StringBuilder s = new StringBuilder("[");
        s.append("Full Location & Term = ");
        if (this.fName != null) {
            s.append(this.fName.toString());
        } else {
            s.append("BlankDataLocation");
        }
        s.append("  ");
        if (this.fValue != null) {
            s.append(this.fValue.toString());
        } else {
            s.append("BlankTerm");
        }
        s.append("]");
        return s.toString();
    }
    
    @Override
    public String toString() {
        String uidStr = "none";
        if (myTopKey != null) {
            String cf = myTopKey.getColumnFamily().toString();
            
            uidStr = cf;
        }
        StringBuilder str = new StringBuilder();
        if (isRangeNode()) {
            str.append(fName).append(':').append(lowerInclusive ? '[' : '(').append(lowerBound).append(", ").append(upperBound)
                            .append(upperInclusive ? ']' : ')').append(", uid=").append(uidStr).append(" , negation=").append(this.isNegated());
        } else {
            switch (type) {
                case ParserTreeConstants.JJTEQNODE:
                case ParserTreeConstants.JJTNENODE:
                    str.append(fName).append(':').append(fValue).append(", uid=").append(uidStr).append(" , negation=").append(this.isQueryNodeNegated())
                                    .append(" and handled as ").append(this.isNegated());
                    break;
                case ParserTreeConstants.JJTERNODE:
                case ParserTreeConstants.JJTNRNODE:
                    str.append(fName).append(':').append(fValue).append(", uid=").append(uidStr).append(" , negation=").append(this.isQueryNodeNegated())
                                    .append(" and handled as ").append(this.isNegated());
                    break;
                case ParserTreeConstants.JJTLENODE:
                    str.append("<=:").append(fName).append(':').append(fValue).append(", uid=").append(uidStr).append(" , negation=")
                                    .append(this.isQueryNodeNegated()).append(" and handled as ").append(this.isNegated());
                    break;
                case ParserTreeConstants.JJTLTNODE:
                    str.append("<:").append(fName).append(':').append(fValue).append(", uid=").append(uidStr).append(" , negation=")
                                    .append(this.isQueryNodeNegated()).append(" and handled as ").append(this.isNegated());
                    break;
                case ParserTreeConstants.JJTGENODE:
                    str.append(">=:").append(fName).append(':').append(fValue).append(", uid=").append(uidStr).append(" , negation=")
                                    .append(this.isQueryNodeNegated()).append(" and handled as ").append(this.isNegated());
                    break;
                case ParserTreeConstants.JJTGTNODE:
                    str.append(">:").append(fName).append(':').append(fValue).append(", uid=").append(uidStr).append(" , negation=")
                                    .append(this.isQueryNodeNegated()).append(" and handled as ").append(this.isNegated());
                    break;
                case ParserTreeConstants.JJTJEXLSCRIPT:
                    str.append("HEAD: uid=").append(uidStr).append(", valid=").append(isValid());
                    break;
                case ParserTreeConstants.JJTANDNODE:
                    str.append("AND: uid=").append(uidStr).append(", valid=").append(isValid()).append(" , negation=").append(this.isQueryNodeNegated())
                                    .append(" and handled as ").append(this.isNegated());
                    break;
                case ParserTreeConstants.JJTNOTNODE:
                    str.append("NOT");
                    break;
                case ParserTreeConstants.JJTORNODE:
                    str.append("OR: uid=").append(uidStr).append(", valid=").append(isValid()).append(" , negation=").append(this.isQueryNodeNegated())
                                    .append(" and handled as ").append(this.isNegated());
                    break;
                default:
                    System.out.println("Problem in BLTNODE.toString()");
                    return null;
            }
        }
        // replace null chars with \x00
        int index = str.indexOf("\0");
        while (index >= 0) {
            str.replace(index, index + 1, "\\x00");
            index = str.indexOf("\0", index);
        }
        return str.toString();
    }
    
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        
        // always start fresh
        this.setTopKey(null);
        this.setDone(false);
        
        // get my user object which should be an iterator
        @SuppressWarnings("unchecked")
        SortedKeyValueIterator<Key,Value> iter = (SortedKeyValueIterator<Key,Value>) this.getUserObject();
        if (iter != null) {
            
            iter.seek(range, columnFamilies, inclusive);
            
            if (iter.hasTop()) {
                this.setTopKey(iter.getTopKey());
                if (log.isDebugEnabled()) {
                    log.debug("BLTNODE.seek() -> found: " + this.getTopKey());
                }
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("BLTNODE.seek() -> hasTop::false");
                }
                this.setDone(true);
            }
        } else {
            if (log.isDebugEnabled()) {
                log.debug("BLTNODE.seek(), The iterator was null!");
            }
            this.setTopKey(null);
        }
    }
    
    public String buildTreePathString(TreeNode[] path) {
        StringBuilder s = new StringBuilder("[");
        for (TreeNode p : path) {
            s.append(p.toString());
            s.append(",");
        }
        s.deleteCharAt(s.length() - 1);
        s.append("]");
        return s.toString();
    }
    
    public void next() throws IOException {
        
        // always start fresh
        this.setTopKey(null);
        
        if (log.isDebugEnabled()) {
            TreeNode[] path = this.getPath();
            log.debug("BLTNODE.next() path-> " + this.buildTreePathString(path));
        }
        
        // have I been marked as done?
        if (this.isDone()) {
            if (log.isDebugEnabled()) {
                log.debug("I've been marked as done, returning");
            }
            return;
        }
        
        @SuppressWarnings("unchecked")
        SortedKeyValueIterator<Key,Value> iter = (SortedKeyValueIterator<Key,Value>) this.getUserObject();
        iter.next();
        
        if (iter.hasTop()) {
            
            this.setTopKey(iter.getTopKey());
            
            if (log.isDebugEnabled()) {
                log.debug("BLTNODE.next() -> found: " + this.getTopKey());
            }
        } else {
            // no top value has been returned, I'm done.
            if (log.isDebugEnabled()) {
                log.debug("BLTNODE.next() -> Nothing found");
            }
            this.setTopKey(null);
            this.setDone(true);
        }
        
    }
    
    @Override
    public boolean jump(Key jumpKey) throws IOException {
        boolean ok = true;
        if (this.getUserObject() instanceof JumpingIterator) {
            @SuppressWarnings("unchecked")
            JumpingIterator<Key,?> iter = (JumpingIterator<Key,?>) this.getUserObject();
            ok = iter.jump(jumpKey);
            if (iter.hasTop()) {
                this.setTopKey(iter.getTopKey());
                if (log.isDebugEnabled()) {
                    log.debug("BLTNODE.jump() -> found: " + this.getTopKey());
                }
            } else {
                if (log.isDebugEnabled()) {
                    log.debug(iter.getClass().getSimpleName() + " does not have top after jump, marking done.");
                }
                this.setTopKey(null);
                this.setDone(true);
            }
        }
        return ok;
    }
    
    public void addToSet(Key i) {
        uids.add(i);
    }
    
    public void reSet() {
        uids = new HashSet<>();
    }
    
    public boolean inSet(Key t) {
        return uids.contains(t);
    }
    
    public Iterator<Key> getSetIterator() {
        return uids.iterator();
    }
    
    public HashSet<Key> getIntersection(HashSet<Key> h) {
        h.retainAll(uids);
        return h;
    }
    
    public Key getMinUniqueID() {
        Iterator<Key> iter = uids.iterator();
        Key min = null;
        while (iter.hasNext()) {
            Key t = iter.next();
            if (log.isDebugEnabled()) {
                log.debug("OR set member: " + t);
            }
            if (t != null) {
                if (min == null) {
                    min = t;
                } else if (t.compareTo(min) < 0) {
                    min = t;
                }
            }
        }
        return min;
    }
    
    public boolean hasTop() {
        // This part really needs to be cleaned up.
        // It was created before I knew what was being passed back.
        if (this.getType() == ParserTreeConstants.JJTORNODE) {
            // Are you a Logical OR or an OR Iterator
            if (!this.isLeaf()) { // logical construct
                // I have a set of keys
                return this.uids.size() > 0;
            } else { // or iterator, you only have possible key
                return this.getTopKey() != null;
            }
        } else {
            return this.getTopKey() != null;
        }
    }
    
}
