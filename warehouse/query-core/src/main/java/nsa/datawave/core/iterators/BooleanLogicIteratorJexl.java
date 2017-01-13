package nsa.datawave.core.iterators;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.UUID;

import nsa.datawave.query.QueryParameters;
import nsa.datawave.query.config.GenericShardQueryConfiguration;
import nsa.datawave.query.exceptions.BooleanLogicFatalQueryException;
import nsa.datawave.query.iterators.JumpingIterator;
import nsa.datawave.query.parser.DatawaveQueryAnalyzer;
import nsa.datawave.query.parser.DatawaveTreeNode;
import nsa.datawave.query.parser.JavaRegexAnalyzer.JavaRegexParseException;
import nsa.datawave.query.util.StringTuple;
import nsa.datawave.util.StringUtils;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.commons.jexl2.parser.ParserTreeConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

/**
 *
 *
 */
@Deprecated
public class BooleanLogicIteratorJexl implements JumpingIterator<Key,Value>, OptionDescriber {
    
    private static final Collection<ByteSequence> EMPTY_COL_FAMS = new ArrayList<>();
    protected static final Logger log = Logger.getLogger(BooleanLogicIteratorJexl.class);
    public static final String BASE_CACHE_DIR = "BASE_CACHE_DIR";
    public static final String FIELD_INDEX_QUERY = "FIELD_INDEX_QUERY";
    public static final String NULL_BYTE_STRING = EvaluatingIterator.NULL_BYTE_STRING;
    public static final String ONE_BYTE_STRING = "\u0001";
    public static final String FIELD_NAME_PREFIX = EvaluatingIterator.FI_PREFIX;
    public static final Value NULL_BYTE_VALUE = new Value(new byte[0]);
    // --------------------------------------------------------------------------
    protected Text nullText = new Text();
    private Key topKey = null;
    private Value topValue = null;
    private SortedKeyValueIterator<Key,Value> sourceIterator;
    private BooleanLogicTreeNodeJexl root;
    private PriorityQueue<BooleanLogicTreeNodeJexl> positives = new PriorityQueue<>(10, new BooleanLogicTreeNodeComparator());
    protected Set<String> unevaluatedFields = new HashSet<>();
    private ArrayList<BooleanLogicTreeNodeJexl> negatives = new ArrayList<>();
    private String updatedQuery;
    private Range overallRange = null;
    private boolean initialized = false;
    private int matched_events = 0;
    private int total_events = 0;
    private IteratorEnvironment env;
    private FileSystem fs = null;
    private Path baseRegexCacheDir = null;
    
    public BooleanLogicIteratorJexl() {}
    
    public BooleanLogicIteratorJexl(BooleanLogicIteratorJexl other, IteratorEnvironment env) {
        if (other.sourceIterator != null) {
            this.sourceIterator = other.sourceIterator.deepCopy(env);
        }
        this.updatedQuery = other.updatedQuery;
        this.fs = other.fs;
        this.baseRegexCacheDir = other.baseRegexCacheDir;
    }
    
    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        return new BooleanLogicIteratorJexl(this, env);
    }
    
    /**
     * <b>init</b> is responsible for setting up the iterator. It will pull the serialized boolean parse tree from the options mapping and construct the
     * appropriate sub-iterators (i.e. ModifiedIntersectingIterator, ModifiedOrIterator).
     *
     * Once initialized, this iterator will automatically seek to the first matching instance. If no top key exists, that means an event matching the boolean
     * logic did not exist on the shard. Subsequent calls to next will move the iterator and all sub-iterators to the next match.
     *
     * @param source
     *            The underlying SortedkeyValueIterator.
     * @param options
     *            A Map<String, String> of options.
     * @param env
     *            The iterator environment
     * @throws IOException
     */
    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        validateOptions(options);
        if (log.isDebugEnabled()) {
            log.debug("Congratulations, you've reached the BooleanLogicIterator.init method");
        }
        // Copy the source iterator
        this.env = env;
        sourceIterator = source.deepCopy(env);
    }
    
    private void initialize() throws IOException {
        
        try {
            // Step 1: Parse the query
            if (log.isDebugEnabled()) {
                log.debug("\tParsing query: " + this.updatedQuery);
            }
            DatawaveQueryAnalyzer analyzer = new DatawaveQueryAnalyzer();
            DatawaveTreeNode treeroot = analyzer.parseJexlQuery(updatedQuery);
            log.debug("parsedTree: " + treeroot.getContents());
            
            // need to build the query tree based on jexl parsing.
            // Step 2: refactor QueryTree - inplace modification
            if (log.isTraceEnabled()) {
                log.trace("\ttransformTreeNode: transforming abstract syntax tree into BooleanLogic tree");
            }
            this.root = transformTreeNode(treeroot);
            
            if (log.isTraceEnabled()) {
                log.trace("\trefactorTree");
            }
            this.root = refactorTree(this.root);
            
            if (log.isTraceEnabled()) {
                log.trace("\tcollapseBranches");
            }
            collapseBranches(root);
            
            // Step 3: create iterators where we need them.
            createIteratorTree(this.root, env);
            if (log.isDebugEnabled()) {
                log.debug("Query tree after iterator creation:\n\t" + this.root.getContents());
            }
            // Step 4: split the positive and negative leaves
            splitLeaves(this.root);
            
            this.initialized = true;
            
        } catch (ParseException ex) {
            log.error("JEXL - ParseException in init: " + ex);
            throw new IOException("Failed to parse query", ex);
        } catch (BooleanLogicFatalQueryException | JavaRegexParseException ex) {
            log.error("DatawaveFatalQueryException: " + ex.getMessage());
            throw new IOException(ex.getMessage());
        }
    }
    
    /* *************************************************************************
     * Methods for sub iterator creation.
     */
    @SuppressWarnings("unchecked")
    private void createIteratorTree(BooleanLogicTreeNodeJexl root, IteratorEnvironment env) throws IOException, JavaRegexParseException {
        if (log.isDebugEnabled()) {
            log.debug("BoolLogic createIteratorTree()");
        }
        // Walk the tree, if all of your children are leaves, roll you into the
        // appropriate iterator.
        List<BooleanLogicTreeNodeJexl> backwards = Collections.list(root.breadthFirstEnumeration());
        for (int i = backwards.size() - 1; i >= 0; i--) {
            BooleanLogicTreeNodeJexl node = backwards.get(i);
            if (!node.isLeaf() && node.getType() != ParserTreeConstants.JJTJEXLSCRIPT) {
                // try to roll up.
                if (canRollUp(node)) {
                    log.debug("canRollUp true");
                    node.setRollUp(true);
                    if (node.getType() == ParserTreeConstants.JJTANDNODE) {
                        node.setUserObject(createIntersectingIterator(node, env));
                    } else if (node.getType() == ParserTreeConstants.JJTORNODE) {
                        node.setUserObject(createOrIterator(node, env));
                    } else {
                        // throw an error.
                        log.error("createIteratorTree, encounterd a node type I do not know about: " + node.getType());
                        log.error("createIteratorTree, node contents:  " + node.getContents());
                    }
                    node.rollUpChildren();
                } else if (node.getType() == ParserTreeConstants.JJTANDNODE) {
                    if (log.isDebugEnabled()) {
                        log.debug("AND");
                    }
                    // inside of an AND we can potentially pull it's single (simple non-negated field equility) iterators into one
                    Set<BooleanLogicTreeNodeJexl> kidSet = new HashSet<>();
                    Enumeration<BooleanLogicTreeNodeJexl> andKids = node.children();
                    while (andKids.hasMoreElements()) {
                        BooleanLogicTreeNodeJexl kid = andKids.nextElement();
                        if (kid.getType() == ParserTreeConstants.JJTEQNODE && !kid.isNegated()) {
                            kidSet.add(kid);
                        }
                    }
                    if (kidSet.size() > 1) {
                        BooleanLogicTreeNodeJexl newAndNode = new BooleanLogicTreeNodeJexl(ParserTreeConstants.JJTANDNODE);
                        for (BooleanLogicTreeNodeJexl kid : kidSet) {
                            node.remove(kid);
                            newAndNode.add(kid);
                        }
                        newAndNode.setUserObject(createIntersectingIterator(newAndNode, env));
                        newAndNode.setRollUp(true);
                        newAndNode.rollUpChildren();
                        node.add(newAndNode);
                    }
                    if (log.isDebugEnabled()) {
                        log.debug("outcome: " + node.getContents());
                    }
                }
            }
        }
        
        // now for remaining leaves, create basic iterators.
        // you can add in specialized iterator mappings here if necessary.
        Enumeration<BooleanLogicTreeNodeJexl> dfe = root.depthFirstEnumeration();
        while (dfe.hasMoreElements()) {
            BooleanLogicTreeNodeJexl node = dfe.nextElement();
            if (node.isLeaf() && node.getType() != ParserTreeConstants.JJTANDNODE && node.getType() != ParserTreeConstants.JJTORNODE) {
                node.setUserObject(createDatawaveFieldIndexIterator(node, env));
                
                // now that we have set the underlying iterator, reset the negated flag appropriately
                if (node.isNegated()) {
                    if (node.isRangeNode()) {
                        node.setNegated(false);
                    }
                    if (node.getType() == ParserTreeConstants.JJTERNODE || node.getType() == ParserTreeConstants.JJTNRNODE) {
                        node.setNegated(false);
                    }
                } else {
                    if (node.getType() == ParserTreeConstants.JJTNENODE) {
                        node.setNegated(true);
                    }
                }
            }
        }
        
        // now fix all of the childrenAllNegated flags
        dfe = root.depthFirstEnumeration();
        while (dfe.hasMoreElements()) {
            BooleanLogicTreeNodeJexl node = dfe.nextElement();
            if (node.isLeaf()) {
                continue;
            }
            
            Enumeration<BooleanLogicTreeNodeJexl> children = node.children();
            boolean allNegative = true;
            while (children.hasMoreElements()) {
                BooleanLogicTreeNodeJexl c = children.nextElement();
                if (!c.isNegated()) {
                    allNegative = false;
                    break;
                }
            }
            node.setChildrenAllNegated(allNegative);
        }
        
    }
    
    private IntersectingIteratorJexl createIntersectingIterator(BooleanLogicTreeNodeJexl node, IteratorEnvironment env) throws IOException {
        if (log.isDebugEnabled()) {
            log.debug("\tcreateIntersectingIterator(node)");
        }
        Text[] columnFamilies = new Text[node.getChildCount()];
        Text[] termValues = new Text[node.getChildCount()];
        boolean[] negationMask = new boolean[node.getChildCount()];
        @SuppressWarnings("unchecked")
        Enumeration<BooleanLogicTreeNodeJexl> children = node.children();
        int i = 0;
        while (children.hasMoreElements()) {
            BooleanLogicTreeNodeJexl child = children.nextElement();
            columnFamilies[i] = child.getFieldName();
            termValues[i] = child.getFieldValue();
            negationMask[i] = child.isNegated();
            i++;
        }
        
        IntersectingIteratorJexl ii = new IntersectingIteratorJexl();
        Map<String,String> options = new HashMap<>();
        options.put(IntersectingIteratorJexl.columnFamiliesOptionName, IntersectingIteratorJexl.encodeColumns(columnFamilies));
        options.put(IntersectingIteratorJexl.termValuesOptionName, IntersectingIteratorJexl.encodeTermValues(termValues));
        options.put(IntersectingIteratorJexl.notFlagsOptionName, IntersectingIteratorJexl.encodeBooleans(negationMask));
        
        ii.init(sourceIterator.deepCopy(env), options, env);
        return ii;
    }
    
    private OrIteratorJexl createOrIterator(BooleanLogicTreeNodeJexl node, IteratorEnvironment env) throws IOException {
        if (log.isDebugEnabled()) {
            log.debug("\tcreateOrIterator(node)");
        }
        
        Multimap<Text,Text> orMap = HashMultimap.create();
        
        @SuppressWarnings("unchecked")
        Enumeration<BooleanLogicTreeNodeJexl> children = node.children();
        while (children.hasMoreElements()) {
            BooleanLogicTreeNodeJexl child = children.nextElement();
            orMap.put(child.getFieldName(), child.getFieldValue());
        }
        
        OrIteratorJexl iter = new OrIteratorJexl(orMap);
        iter.init(sourceIterator, null, env);
        
        return iter;
    }
    
    /*
     * This takes the place of the SortedKeyIterator used previously. This iterator is bound to the DataWave shard table structure. When next is called it will
     * jump rows as necessary internally versus needing to do it externally as was the case with the SortedKeyIterator.
     */
    private WrappingIterator createDatawaveFieldIndexIterator(BooleanLogicTreeNodeJexl node, IteratorEnvironment env) throws IOException,
                    JavaRegexParseException {
        if (log.isDebugEnabled()) {
            log.debug("\tcreateDatawaveFieldIndexIterator(node)");
            log.debug("\t\tfName: " + node.getFieldName() + " , fValue: " + node.getFieldValue() + " , operator: " + node.getFieldOperator());
        }
        
        WrappingIterator iter = null;
        if (node.getType() == ParserTreeConstants.JJTEQNODE) {
            iter = new DatawaveFieldIndexIteratorJexl(node.getFieldName(), node.getFieldValue(), null, null, node.isNegated());
        } else if (node.getType() == ParserTreeConstants.JJTERNODE) {
            // TODO make the scanThreshold configurable
            // TODO make the scanTimeout configurable
            iter = new DatawaveFieldIndexRegexIteratorJexl(node.getFieldName(), node.getFieldValue(), null, null, node.isNegated(), 100000L, 1000L * 60 * 60,
                            10000, 11, fs, getTemporaryCacheDir(node), false);
        } else if (node.isRangeNode()) {
            // TODO make the scanThreshold configurable
            // TODO make the scanTimeout configurable
            iter = new DatawaveFieldIndexRangeIteratorJexl(node.getFieldName(), node.getLowerBound(), node.isLowerInclusive(), node.getUpperBound(),
                            node.isUpperInclusive(), null, null, node.isNegated(), 100000L, 1000L * 60 * 60, 10000, 11, fs, getTemporaryCacheDir(node), false);
        }
        
        Map<String,String> options = new HashMap<>();
        if (iter != null) {
            iter.init(sourceIterator.deepCopy(env), options, env);
        }
        
        return iter;
    }
    
    /**
     * Create a cache directory path for a specified regex or range node
     *
     * @param node
     * @return A path
     * @throws IOException
     */
    private Path getTemporaryCacheDir(BooleanLogicTreeNodeJexl node) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try {
            bytes.write(node.getFieldName().getBytes());
            if (node.isRangeNode()) {
                bytes.write(node.getLowerBound().getBytes());
                bytes.write(node.getUpperBound().getBytes());
            } else {
                bytes.write(node.getFieldValue().getBytes());
            }
        } catch (IOException ioe) {
            // cannot happen with a ByteArrayOutputStream()
            throw new IllegalStateException("ByteArrayOutputStream failed to take my bytes", ioe);
        }
        Path cacheDir = new Path(this.baseRegexCacheDir, UUID.nameUUIDFromBytes(bytes.toByteArray()).toString());
        // ensure the directory is created, otherwise the iterator will think the query is cancelled
        this.fs.mkdirs(cacheDir);
        return cacheDir;
    }
    
    /* *************************************************************************
     * Methods for testing the tree WRT boolean logic.
     */
    // After all iterator pointers have been advanced, test if the current
    // record passes the boolean logic.
    private boolean testTreeState() {
        if (log.isDebugEnabled()) {
            log.debug("BoolLogic testTreeState() begin");
        }
        Enumeration<?> dfe = this.root.depthFirstEnumeration();
        while (dfe.hasMoreElements()) {
            BooleanLogicTreeNodeJexl node = (BooleanLogicTreeNodeJexl) dfe.nextElement();
            if (!node.isLeaf()) {
                
                int type = node.getType();
                if (type == ParserTreeConstants.JJTANDNODE) {
                    handleAND(node);
                } else if (type == ParserTreeConstants.JJTORNODE) {
                    handleOR(node);
                } else if (type == ParserTreeConstants.JJTJEXLSCRIPT) {
                    handleHEAD(node);
                } else if (type == ParserTreeConstants.JJTNOTNODE) {
                    // there should not be any "NOT"s.
                }
            } else {
                // it is a leaf, if it is an AND or OR do something
                if (node.getType() == ParserTreeConstants.JJTORNODE) {
                    node.setValid(node.hasTop());
                    node.reSet();
                    node.addToSet(node.getTopKey());
                    
                } else if (node.getType() == ParserTreeConstants.JJTANDNODE || node.getType() == ParserTreeConstants.JJTEQNODE
                                || node.getType() == ParserTreeConstants.JJTERNODE || node.getType() == ParserTreeConstants.JJTLENODE
                                || node.getType() == ParserTreeConstants.JJTLTNODE || node.getType() == ParserTreeConstants.JJTGENODE
                                || node.getType() == ParserTreeConstants.JJTGTNODE || node.isRangeNode()) {
                    // sub iterator guarantees it is in its internal range,
                    // otherwise, no top.
                    node.setValid(node.hasTop());
                }
            }
        }
        
        if (log.isDebugEnabled()) {
            log.debug("BoolLogic.testTreeState end, treeState:: " + this.root.getContents() + "  , valid: " + root.isValid());
        }
        return this.root.isValid();
    }
    
    private void handleHEAD(BooleanLogicTreeNodeJexl node) {
        @SuppressWarnings("unchecked")
        Enumeration<BooleanLogicTreeNodeJexl> children = node.children();
        while (children.hasMoreElements()) {
            BooleanLogicTreeNodeJexl child = children.nextElement();
            
            if (child.getType() == ParserTreeConstants.JJTANDNODE) {
                node.setValid(child.isValid());
                node.setTopKey(child.getTopKey());
            } else if (child.getType() == ParserTreeConstants.JJTORNODE) {
                node.setValid(child.isValid());
                node.setTopKey(child.getTopKey());
            } else if (child.getType() == ParserTreeConstants.JJTEQNODE || child.getType() == ParserTreeConstants.JJTERNODE
                            || child.getType() == ParserTreeConstants.JJTGTNODE || child.getType() == ParserTreeConstants.JJTGENODE
                            || child.getType() == ParserTreeConstants.JJTLTNODE || child.getType() == ParserTreeConstants.JJTLENODE || child.isRangeNode()) {
                
                node.setTopKey(child.getTopKey());
                node.setValid(node.getTopKey() != null);
                
            }
        }// end while
        
        // I have to be valid AND have a top key
        if (node.isValid() && !node.hasTop()) {
            node.setValid(false);
        }
    }
    
    private void handleAND(BooleanLogicTreeNodeJexl me) {
        if (log.isTraceEnabled()) {
            log.trace("handleAND::" + me.getContents());
        }
        @SuppressWarnings("unchecked")
        Enumeration<BooleanLogicTreeNodeJexl> children = me.children();
        me.setValid(true); // it's easier to prove false than true
        
        HashSet<Key> goodSet = new HashSet<>();
        HashSet<Key> badSet = new HashSet<>();
        BooleanLogicTreeNodeJexl child;
        while (children.hasMoreElements()) {
            child = children.nextElement();
            
            if (child.getType() == ParserTreeConstants.JJTEQNODE || child.getType() == ParserTreeConstants.JJTNENODE
                            || child.getType() == ParserTreeConstants.JJTERNODE || child.getType() == ParserTreeConstants.JJTNRNODE || child.isRangeNode()
                            || child.getType() == ParserTreeConstants.JJTANDNODE) {
                
                // regex and ranges are always treated as non-negative because the underlying
                // iterators take care of the negation
                if (child.isNegated()) {
                    if (child.hasTop()) {
                        badSet.add(child.getTopKey());
                        if (goodSet.contains(child.getTopKey())) {
                            me.setValid(false);
                            return;
                        }
                        if (child.isValid()) {
                            me.setValid(false);
                            return;
                        }
                    }
                } else {
                    if (child.hasTop()) {
                        if (log.isTraceEnabled()) {
                            log.trace("handleAND, child node: " + child.getContents());
                        }
                        // if you're in the bad set, you're done.
                        if (badSet.contains(child.getTopKey())) {
                            if (log.isTraceEnabled()) {
                                log.trace("handleAND, child is in bad set, setting parent false");
                            }
                            me.setValid(false);
                            return;
                        }
                        
                        // if good set is empty, add it.
                        if (goodSet.isEmpty()) {
                            if (log.isTraceEnabled()) {
                                log.trace("handleAND, goodSet is empty, adding child: " + child.getContents());
                            }
                            goodSet.add(child.getTopKey());
                        } else {
                            // must be in the good set & not in the bad set
                            // if either fails, I'm false.
                            if (!goodSet.contains(child.getTopKey())) {
                                if (log.isTraceEnabled()) {
                                    log.trace("handleAND, goodSet is not empty, and does NOT contain child, setting false.  child: " + child.getContents());
                                }
                                me.setValid(false);
                                return;
                            } else {
                                // trim the good set to this one value
                                // (handles the case were the initial encounters were ORs)
                                goodSet.clear();
                                goodSet.add(child.getTopKey());
                                if (log.isTraceEnabled()) {
                                    log.trace("handleAND, child in goodset, trim to this value: " + child.getContents());
                                }
                            }
                        }
                    } else {
                        // test if its children are all false
                        if (child.getChildCount() > 0) {
                            @SuppressWarnings("unchecked")
                            Enumeration<BooleanLogicTreeNodeJexl> subchildren = child.children();
                            boolean allFalse = true;
                            BooleanLogicTreeNodeJexl subchild;
                            while (subchildren.hasMoreElements()) {
                                subchild = subchildren.nextElement();
                                if (!subchild.isNegated()) {
                                    allFalse = false;
                                    break;
                                } else if (subchild.isNegated() && subchild.hasTop()) {
                                    allFalse = false;
                                    break;
                                }
                            }
                            if (!allFalse) {
                                me.setValid(false);
                                return;
                            }
                        } else {
                            // child returned a null value and is not a negation, this in turn makes me false.
                            me.setValid(false);
                            return;
                        }
                    }
                }
                
            } else if (child.getType() == ParserTreeConstants.JJTORNODE) {// BooleanLogicTreeNode.NodeType.OR) {
            
                // NOTE: The OR may be an OrIterator in which case it will only produce
                // a single unique identifier, or it may be a pure logical construct and
                // be capable of producing multiple unique identifiers.
                // This should handle all cases.
                Iterator<Key> iter = child.getSetIterator();
                boolean goodSetEmpty = goodSet.isEmpty();
                boolean matchedOne = false;
                boolean pureNegations = true;
                if (!child.isValid()) {
                    if (log.isTraceEnabled()) {
                        log.trace("handleAND, child is an OR and it is not valid, setting false, ALL NEGATED?: " + child.isChildrenAllNegated());
                    }
                    me.setValid(false); // I'm an AND if one of my children is false, I'm false.
                    return;
                } else if (child.isValid() && !child.hasTop()) {
                    // pure negation, do nothing
                } else if (child.isValid() && child.hasTop()) { // I need to match one
                    if (log.isTraceEnabled()) {
                        log.trace("handleAND, child OR, valid and has top, means not pureNegations");
                    }
                    pureNegations = false;
                    Key key;
                    while (iter.hasNext()) {
                        key = iter.next();
                        if (child.isNegated()) {
                            badSet.add(key);
                            if (goodSet.contains(key)) {
                                if (log.isTraceEnabled()) {
                                    log.trace("handleAND, child OR, goodSet contains bad value: " + key);
                                }
                                me.setValid(false);
                                return;
                            }
                        } else {
                            // if the good set is empty, then push all of my ids.
                            if (goodSetEmpty && !badSet.contains(key)) {
                                goodSet.add(key);
                                matchedOne = true;
                            } else {
                                // I need at least one to match
                                if (goodSet.contains(key)) {
                                    matchedOne = true;
                                }
                            }
                        }
                    }
                }
                
                // is the goodSet still empty? that means were were only negations
                // otherwise, if it's not empty and we didn't match one, false
                if (child.isNegated()) {
                    // we're ok
                } else {
                    if (goodSet.isEmpty() && !pureNegations) {
                        if (log.isTraceEnabled()) {
                            log.trace("handleAND, child OR, empty goodset && !pureNegations, set false");
                        }
                        // that's bad, we weren't negated, should've pushed something in there.
                        me.setValid(false);
                        return;
                    } else if (!goodSet.isEmpty() && !pureNegations) { // goodSet contains values.
                        if (!matchedOne) { // but we didn't match any.
                            if (log.isTraceEnabled()) {
                                log.trace("handleAND, child OR, goodSet had values but I didn't match any, false");
                            }
                            me.setValid(false);
                            return;
                        }
                        
                        // we matched something, trim the set.
                        // i.e. two child ORs
                        goodSet = child.getIntersection(goodSet);
                    }
                }
                
            }
        }// end while
        
        if (goodSet.isEmpty()) {
            if (log.isTraceEnabled()) {
                log.trace("handleAND-> goodSet is empty, pure negations?");
            }
        } else {
            me.setTopKey(Collections.min(goodSet));
            if (log.isTraceEnabled()) {
                log.trace("End of handleAND, this node's topKey: " + me.getTopKey());
            }
        }
    }
    
    private void handleOR(BooleanLogicTreeNodeJexl me) {
        if (log.isTraceEnabled()) {
            log.trace("handleOR method");
        }
        @SuppressWarnings("unchecked")
        Enumeration<BooleanLogicTreeNodeJexl> children = me.children();
        // I'm an OR node, need at least one positive.
        me.setValid(false);
        me.reSet();
        me.setTopKey(null);
        boolean allNegated = true;
        BooleanLogicTreeNodeJexl child;
        while (children.hasMoreElements()) {
            // 3 cases for child: SEL, AND, OR
            // and negation
            child = children.nextElement();
            if (child.getType() == ParserTreeConstants.JJTEQNODE || child.getType() == ParserTreeConstants.JJTNENODE
                            || child.getType() == ParserTreeConstants.JJTERNODE || child.getType() == ParserTreeConstants.JJTNRNODE || child.isRangeNode()
                            || child.getType() == ParserTreeConstants.JJTANDNODE) {
                
                if (child.hasTop()) {
                    if (child.isNegated()) {
                        // do nothing.
                    } else {
                        allNegated = false;
                        // I have something add it to my set.
                        if (child.isValid()) {
                            me.addToSet(child.getTopKey());
                        }
                    }
                } else if (!child.isNegated()) { // I have a non-negated child
                    allNegated = false;
                    // that child could be pure negations in which case I'm true
                    me.setValid(child.isValid());
                }
                
            } else if (child.getType() == ParserTreeConstants.JJTORNODE) {
                if (child.hasTop()) {
                    if (!child.isNegated()) {
                        allNegated = false;
                        // add its rowIds to my rowIds
                        Iterator<Key> iter = child.getSetIterator();
                        Key k;
                        while (iter.hasNext()) {
                            k = iter.next();
                            if (k != null) {
                                me.addToSet(k);
                            }
                        }
                    }
                } else {
                    // Or node that doesn't have a top, check if it's valid or not
                    // because it could be pure negations itself.
                    if (child.isValid()) {
                        me.setValid(true);
                    }
                }
            }
        }// end while
        
        if (allNegated) {
            // do all my children have top?
            @SuppressWarnings("unchecked")
            Enumeration<BooleanLogicTreeNodeJexl> moreChildren = me.children();
            children = moreChildren;
            while (children.hasMoreElements()) {
                child = children.nextElement();
                if (!child.hasTop()) {
                    me.setValid(true);
                    me.setTopKey(null);
                    return;
                }
            }
            me.setValid(false);
            
        } else {
            Key k = me.getMinUniqueID();
            if (k == null) {
                me.setValid(false);
            } else {
                me.setValid(true);
                me.setTopKey(k);
            }
        }
        
        if (log.isTraceEnabled()) {
            if (me.hasTop()) {
                log.trace("end handleOR, key: " + me.getTopKey());
            } else {
                log.trace("no key from handleOR");
            }
        }
    }
    
    /* *************************************************************************
     * Utility methods.
     */
    
    public BooleanLogicTreeNodeJexl transformTreeNode(DatawaveTreeNode node) {
        if (node.getType() == ParserTreeConstants.JJTEQNODE || node.getType() == ParserTreeConstants.JJTNENODE
                        || node.getType() == ParserTreeConstants.JJTERNODE || node.getType() == ParserTreeConstants.JJTNRNODE) {
            
            String fName = node.getFieldName();
            if (!fName.startsWith(FIELD_NAME_PREFIX)) {
                fName = FIELD_NAME_PREFIX + fName;
            }
            int type = node.getType();
            boolean negated = node.isNegated();
            if (type == ParserTreeConstants.JJTNENODE) {
                type = ParserTreeConstants.JJTEQNODE;
                negated = !negated;
            } else if (type == ParserTreeConstants.JJTNRNODE) {
                type = ParserTreeConstants.JJTERNODE;
                negated = !negated;
            }
            BooleanLogicTreeNodeJexl child = new BooleanLogicTreeNodeJexl(type, fName, node.getFieldValue(), negated);
            return child;
        } else if (node.isRangeNode()) {
            String fName = node.getFieldName();
            if (!fName.startsWith(FIELD_NAME_PREFIX)) {
                fName = FIELD_NAME_PREFIX + fName;
            }
            BooleanLogicTreeNodeJexl child = new BooleanLogicTreeNodeJexl(node.getType(), fName, node.getLowerBound(), node.getRangeLowerInclusive(),
                            node.getUpperBound(), node.getRangeUpperInclusive(), node.isNegated());
            return child;
        }
        
        BooleanLogicTreeNodeJexl returnNode = null;
        if (node.getType() == ParserTreeConstants.JJTANDNODE || node.getType() == ParserTreeConstants.JJTORNODE) {
            returnNode = new BooleanLogicTreeNodeJexl(node.getType(), node.isNegated());
            returnNode.setChildrenAllNegated(node.isChildrenAllNegated());
        } else if (node.getType() == ParserTreeConstants.JJTNOTNODE) {
            returnNode = new BooleanLogicTreeNodeJexl(node.getType());
        } else if (node.getType() == ParserTreeConstants.JJTJEXLSCRIPT) {
            if (log.isTraceEnabled()) {
                log.trace("ROOT/JexlScript node");
            }
            returnNode = new BooleanLogicTreeNodeJexl(node.getType());
            
        } else {
            log.error("Currently Unsupported Node type: " + node.getClass().getName() + " \t" + node.getType());
        }
        
        @SuppressWarnings("unchecked")
        Enumeration<DatawaveTreeNode> children = node.children();
        while (children.hasMoreElements()) {
            DatawaveTreeNode child = children.nextElement();
            if (returnNode != null) {
                returnNode.add(transformTreeNode(child));
            }
        }
        
        return returnNode;
    }
    
    // After tree conflicts have been resolved, we can collapse branches where
    // leaves have been pruned.
    public void collapseBranches(BooleanLogicTreeNodeJexl myroot) throws BooleanLogicFatalQueryException {
        
        // NOTE: doing a depth first enumeration didn't wory when I started
        // removing nodes halfway through. The following method does work,
        // it's essentially a reverse breadth first traversal.
        @SuppressWarnings("unchecked")
        List<BooleanLogicTreeNodeJexl> nodes = Collections.list(myroot.breadthFirstEnumeration());
        
        // walk backwards
        for (int i = nodes.size() - 1; i >= 0; i--) {
            BooleanLogicTreeNodeJexl node = nodes.get(i);
            if (log.isTraceEnabled()) {
                log.trace("collapseBranches, inspecting node: " + node.toString() + "  " + node.printNode());
            }
            
            if (node.getType() == ParserTreeConstants.JJTANDNODE || node.getType() == ParserTreeConstants.JJTORNODE) {
                if (node.getChildCount() == 0 && !node.isRangeNode()) {
                    node.removeFromParent();
                } else if (node.getChildCount() == 1) {
                    BooleanLogicTreeNodeJexl p = (BooleanLogicTreeNodeJexl) node.getParent();
                    BooleanLogicTreeNodeJexl c = (BooleanLogicTreeNodeJexl) node.getFirstChild();
                    node.removeFromParent();
                    p.add(c);
                    
                }
            } else if (node.getType() == ParserTreeConstants.JJTJEXLSCRIPT) {
                if (node.getChildCount() == 0) {
                    if (log.isTraceEnabled()) {
                        log.trace("collapseBranches, headNode has no children");
                    }
                    throw new BooleanLogicFatalQueryException(this.updatedQuery, "Head node has no children");
                }
            }
        }
        
    }
    
    public BooleanLogicTreeNodeJexl refactorTree(BooleanLogicTreeNodeJexl myroot) {
        @SuppressWarnings("unchecked")
        List<BooleanLogicTreeNodeJexl> nodes = Collections.list(myroot.breadthFirstEnumeration());
        
        // walk backwards
        for (int i = nodes.size() - 1; i >= 0; i--) {
            BooleanLogicTreeNodeJexl node = nodes.get(i);
            if (node.getType() == ParserTreeConstants.JJTANDNODE || node.getType() == ParserTreeConstants.JJTORNODE) {
                // 1. check to see if all children are negated
                // 2. check to see if we have to handle ranges.
                
                Enumeration<?> children = node.children();
                boolean allNegated = true;
                while (children.hasMoreElements()) {
                    BooleanLogicTreeNodeJexl child = (BooleanLogicTreeNodeJexl) children.nextElement();
                    if (!child.isNegated()) {
                        allNegated = false;
                    }
                }
                if (allNegated) {
                    node.setChildrenAllNegated(true);
                }
                
            }
        }
        
        return myroot;
    }
    
    // If all children are of type SEL, roll this up into an AND or OR node.
    private static boolean canRollUp(BooleanLogicTreeNodeJexl parent) {
        if (log.isTraceEnabled()) {
            log.trace("canRollUp: testing " + parent.getContents());
        }
        if (parent.getChildCount() < 1) {
            if (log.isTraceEnabled()) {
                log.trace("canRollUp: child count < 1, return false");
            }
            return false;
        }
        @SuppressWarnings("unchecked")
        Enumeration<BooleanLogicTreeNodeJexl> e = parent.children();
        BooleanLogicTreeNodeJexl child;
        while (e.hasMoreElements()) {
            child = e.nextElement();
            
            if (child.getType() != ParserTreeConstants.JJTEQNODE) {
                if (log.isTraceEnabled()) {
                    log.trace("canRollUp: child.getType -> " + ParserTreeConstants.jjtNodeName[child.getType()] + " int: " + child.getType() + "  return false");
                }
                return false;
            }
            
            if (child.isNegated()) {
                if (log.isTraceEnabled()) {
                    log.trace("canRollUp: child.isNegated, return false");
                }
                return false;
            }
            
            if (child.getFieldValue().toString().contains("*")) {
                if (log.isTraceEnabled()) {
                    log.trace("canRollUp: child has wildcard: " + child.getFieldValue());
                }
                return false;
            }
        }
        return true;
    }
    
    /**
     * Small utility function to print out the depth-first enumeration of the tree. Specify the root or sub root of the tree you wish to view.
     *
     * @param root
     *            The root node of the tree or sub-tree.
     */
    public static void showDepthFirstTraversal(BooleanLogicTreeNodeJexl root) {
        System.out.println("DepthFirstTraversal");
        Enumeration<?> e = root.depthFirstEnumeration();
        int i = -1;
        while (e.hasMoreElements()) {
            i += 1;
            BooleanLogicTreeNodeJexl n = (BooleanLogicTreeNodeJexl) e.nextElement();
            System.out.println(i + " : " + n);
        }
    }
    
    public static void showBreadthFirstTraversal(BooleanLogicTreeNodeJexl root) {
        System.out.println("BreadthFirstTraversal");
        log.debug("BooleanLogicIterator.showBreadthFirstTraversal()");
        Enumeration<?> e = root.breadthFirstEnumeration();
        int i = -1;
        while (e.hasMoreElements()) {
            i += 1;
            BooleanLogicTreeNodeJexl n = (BooleanLogicTreeNodeJexl) e.nextElement();
            System.out.println(i + " : " + n);
            log.debug(i + " : " + n);
        }
    }
    
    private void splitLeaves(BooleanLogicTreeNodeJexl node) {
        if (log.isTraceEnabled()) {
            log.trace("BoolLogic: splitLeaves() method, will split into positive and negated leaves");
        }
        
        positives.clear();
        negatives.clear();
        @SuppressWarnings("unchecked")
        Enumeration<BooleanLogicTreeNodeJexl> dfe = node.depthFirstEnumeration();
        BooleanLogicTreeNodeJexl elem;
        while (dfe.hasMoreElements()) {
            elem = dfe.nextElement();
            if (elem.isLeaf()) {
                // only split off negative == nodes. Negated ranges and regex nodes get evaluated as positives do
                if (elem.isNegated()) {
                    negatives.add(elem);
                } else {
                    positives.add(elem);
                }
            }
        }
    }
    
    private void reHeapPriorityQueue(BooleanLogicTreeNodeJexl node) {
        positives.clear();
        @SuppressWarnings("unchecked")
        Enumeration<BooleanLogicTreeNodeJexl> dfe = node.depthFirstEnumeration();
        BooleanLogicTreeNodeJexl elem;
        while (dfe.hasMoreElements()) {
            elem = dfe.nextElement();
            if (elem.isLeaf() && !elem.isNegated()) {
                positives.add(elem);
            }
        }
    }
    
    /* *************************************************************************
     * The iterator interface methods.
     */
    @Override
    public boolean hasTop() {
        if (log.isDebugEnabled()) {
            if (topKey == null) {
                log.debug("hasTop, current_tot: " + total_events + "  current_matched: " + matched_events);
            }
        }
        return (topKey != null);
    }
    
    @Override
    public Key getTopKey() {
        return topKey;
    }
    
    @Override
    public Value getTopValue() {
        return topValue;
    }
    
    private void setTopKey(Key key) {
        if (this.overallRange != null && key != null) {
            if (overallRange.getEndKey() != null) { // if null end key, that means range is to the end of the tablet.
                if (!this.overallRange.contains(key)) {
                    topKey = null;
                    return;
                } else {
                    try {
                        StringTuple matchingNodes = serializeMatchingQueryNodes();
                        ByteArrayOutputStream baos = new ByteArrayOutputStream();
                        matchingNodes.write(new DataOutputStream(baos));
                        topValue = new Value(baos.toByteArray());
                    } catch (IOException ex) {
                        log.error("Exception serializing matching nodes", ex);
                    }
                }
            }
        }
        topKey = key;
    }
    
    /**
     * Validate that a field is an unevaluated field, if it is, add it (in string form) to the provided set.
     *
     * The field name may be in column family form, so extract the fieldname from a column family from the field index and validate that it is an unevaluated
     * field before adding it to the set of matching unevaluated nodes.
     *
     * @param fieldName
     * @param fieldValue
     * @param matches
     */
    protected void captureUnevaluatedMatches(Text fieldName, Text fieldValue, Set<String> matches) {
        if (log.isDebugEnabled()) {
            log.debug("validateChuffNode(): f: " + fieldName + " v: " + fieldValue);
        }
        String fieldStr = fieldName.toString();
        int start = 0;
        int end = fieldStr.length();
        
        if (fieldStr.startsWith("fi\0")) {
            start = 3;
        }
        int pos = fieldStr.indexOf(0, start);
        if (pos > start) {
            end = pos;
        }
        fieldStr = fieldStr.substring(start, end);
        if (unevaluatedFields.contains(fieldStr)) {
            matches.add(fieldStr + ":" + fieldValue);
        }
    }
    
    /**
     * Serialize a list of matching unevaluated field names/values in order to pass them to the Evaluating iterator.
     *
     * @return a list of matching query nodes.
     */
    protected StringTuple serializeMatchingQueryNodes() {
        if (this.root == null) {
            return new StringTuple(0);
        }
        
        if (log.isTraceEnabled()) {
            log.trace("serializeMatchingQueryNodes() begin");
        }
        
        Enumeration<?> dfe = this.root.depthFirstEnumeration();
        Set<String> matches = new HashSet<>();
        while (dfe.hasMoreElements()) {
            BooleanLogicTreeNodeJexl node = (BooleanLogicTreeNodeJexl) dfe.nextElement();
            if (log.isTraceEnabled()) {
                log.trace("serializeMatchingQueryNodes(): Visiting node: " + node.toString());
            }
            if (node.isLeaf() && node.isValid() && node.getFieldName() != null) {
                captureUnevaluatedMatches(node.getFieldName(), node.getFieldValue(), matches);
            } else if (node.isRollUp() && node.isValid()) {
                // TODO: handle and's and or's differently.
                if (log.isDebugEnabled()) {
                    log.debug("serializeMatchingQueryNodes(): Found valid rollup: " + node + " with iterator " + node.getUserObject().getClass());
                }
                Object o = node.getUserObject();
                if (o instanceof OrIteratorJexl) {
                    OrIteratorJexl ori = (OrIteratorJexl) o;
                    captureUnevaluatedMatches(ori.getCurrentFieldName(), ori.getCurrentFieldValue(), matches);
                } else if (o instanceof IntersectingIteratorJexl) {
                    for (Object child : node.getRolledUpChildren()) {
                        BooleanLogicTreeNodeJexl b = (BooleanLogicTreeNodeJexl) child;
                        captureUnevaluatedMatches(b.getFieldName(), b.getFieldValue(), matches);
                    }
                }
            }
        }
        
        StringTuple buf = new StringTuple(matches.size());
        for (String match : matches) {
            buf.add(match);
        }
        
        if (log.isDebugEnabled()) {
            if (buf.getSize() > 0) {
                log.debug("serializeMatchingQueryNodes(): end: [" + buf.toString() + "]");
            } else {
                log.debug("serializeMatchingQueryNodes(): end: [empty]");
            }
        }
        
        return buf;
    }
    
    public static Multimap<String,String> deserializeMatchingQueryNodes(StringTuple serializedNodes) {
        return deserializeMatchingQueryNodes(serializedNodes, true);
    }
    
    /**
     * Deserialize a list of matching unevaluated field names/values into a multimap, case sensitiy should be identical to that of QueryEvaluator
     *
     * @param serializedNodes
     * @param isCaseSensitive
     * @return
     */
    public static Multimap<String,String> deserializeMatchingQueryNodes(StringTuple serializedNodes, boolean isCaseSensitive) {
        
        if (log.isDebugEnabled()) {
            if (serializedNodes.getSize() > 0) {
                log.debug("deserializeMatchingQueryNodes(): begin: [" + serializedNodes.toString() + "]");
            } else {
                log.debug("deserializeMatchingQueryNodes(): begin: [empty]");
            }
        }
        
        if (serializedNodes == null || serializedNodes.getSize() == 0) {
            return HashMultimap.create();
        }
        
        Multimap<String,String> nodes = HashMultimap.create();
        
        int sz = serializedNodes.getSize();
        for (int i = 0; i < sz; i++) {
            String p = serializedNodes.get(i);
            int pos = p.indexOf(':');
            if (isCaseSensitive) { // see QueryEvaluator constructor, DatawaveQueryAnalyzer
                nodes.put(p.substring(0, pos).toUpperCase(), p.substring(pos + 1).toLowerCase());
            } else {
                nodes.put(p.substring(0, pos), p.substring(pos + 1));
            }
        }
        
        return nodes;
    }
    
    private void resetNegatives() {
        for (BooleanLogicTreeNodeJexl neg : negatives) {
            neg.setTopKey(null);
            neg.setValid(true);
        }
    }
    
    public static String getEventKeyDatatypeUid(Key k) {
        if (k == null || k.getColumnFamily() == null) {
            return null;
        } else {
            return k.getColumnFamily().toString();
        }
    }
    
    public static String getEventKeyRowDatatypeUid(Key k) {
        if (k == null || k.getColumnFamily() == null) {
            return null;
        } else {
            StringBuilder b = new StringBuilder();
            return b.append(k.getRow()).append(NULL_BYTE_STRING).append(k.getColumnFamily()).toString();
        }
    }
    
    public static String getIndexKeyDatatypeUid(Key k) {
        try {
            int idx = 0;
            String sKey = k.getColumnQualifier().toString();
            idx = sKey.indexOf(NULL_BYTE_STRING);
            return sKey.substring(idx + 1);
        } catch (Exception e) {
            return null;
        }
    }
    
    public static String getIndexKeyRowDatatypeUid(Key k) {
        try {
            int idx = 0;
            String sKey = k.getColumnQualifier().toString();
            idx = sKey.indexOf(NULL_BYTE_STRING);
            StringBuilder b = new StringBuilder();
            return b.append(k.getRow()).append(NULL_BYTE_STRING).append(sKey.substring(idx + 1)).toString();
        } catch (IndexOutOfBoundsException ioobe) {
            return null;
        } catch (NullPointerException npe) {
            return null;
        }
    }
    
    /**
     * Analyze the sub-iterators' current topKeys to figure out if we can skip intermediate keys. Basically acts like the internals seeking of the
     * IntersectingIterator but includes logic to handle both AND's and OR's.
     *
     * All sub-iterators should return Event keys
     *
     * @return
     * @throws IOException
     */
    public Key findBestJumpKey() throws IOException {
        @SuppressWarnings("unchecked")
        List<BooleanLogicTreeNodeJexl> bfl = Collections.list(root.breadthFirstEnumeration());
        // walk the tree backwards
        BooleanLogicTreeNodeJexl node;
        for (int i = bfl.size() - 1; i >= 0; i--) {
            node = bfl.get(i);
            
            if (node.isNegated() || node.isChildrenAllNegated()) {
                continue;
            }
            
            if (node.isLeaf()) {
                node.setAdvanceKey(node.getTopKey()); // all underlying nodes should return event keys.
                if (log.isTraceEnabled()) {
                    log.trace("leaf: " + node.getTopKey() + " advanceKey: " + node.getAdvanceKey());
                }
            } else {
                if (ParserTreeConstants.JJTANDNODE == node.getType()) {
                    log.trace("Logical ANDnode: " + node);
                    node.setAdvanceKey(null);
                    // for AND we want the max
                    @SuppressWarnings("unchecked")
                    Enumeration<BooleanLogicTreeNodeJexl> children = node.children();
                    BooleanLogicTreeNodeJexl child;
                    while (children.hasMoreElements()) {
                        child = children.nextElement();
                        
                        // we don't care about negated nodes, they are essentially filters
                        if (child.isNegated() || child.isChildrenAllNegated()) {
                            continue;
                        }
                        
                        // if we see a child with a null key, we're done b/c we're an AND
                        if (null == child.getAdvanceKey()) {
                            node.setAdvanceKey(null);
                            break;
                        } else if (null == node.getAdvanceKey()) {
                            node.setAdvanceKey(child.getAdvanceKey());
                        } else if (node.getAdvanceKey().compareTo(child.getAdvanceKey()) < 0) {
                            node.setAdvanceKey(child.getAdvanceKey());
                        }
                        
                    } // end while
                    if (log.isTraceEnabled()) {
                        log.trace("findBestJumpKey, AND node max: " + node.getAdvanceKey());
                    }
                    if (null == node.getAdvanceKey()) {
                        node.setDone(true);
                    }
                    
                } else if (ParserTreeConstants.JJTORNODE == node.getType()) {
                    // for OR we want the min
                    node.setAdvanceKey(null);
                    @SuppressWarnings("unchecked")
                    Enumeration<BooleanLogicTreeNodeJexl> children = node.children();
                    BooleanLogicTreeNodeJexl child;
                    while (children.hasMoreElements()) {
                        child = children.nextElement();
                        
                        // we don't care about negated nodes, they are essentially filters
                        if (child.isNegated() || child.isChildrenAllNegated()) {
                            continue;
                        }
                        if (log.isTraceEnabled()) {
                            log.trace("OR Node, child advanceKey: " + child.getAdvanceKey());
                        }
                        // making the test separate for clarity
                        if (null == child.getAdvanceKey()) {
                            continue;
                        }
                        
                        if (null == node.getAdvanceKey()) {
                            node.setAdvanceKey(child.getAdvanceKey());
                        } else if (node.getAdvanceKey().compareTo(child.getAdvanceKey()) > 0) {
                            node.setAdvanceKey(child.getAdvanceKey());
                        }
                    }
                    if (log.isTraceEnabled()) {
                        log.trace("findBestJumpKey, OR node min: " + node.getAdvanceKey());
                    }
                    
                } else if (ParserTreeConstants.JJTJEXLSCRIPT == node.getType()) {
                    BooleanLogicTreeNodeJexl child = (BooleanLogicTreeNodeJexl) node.getFirstChild();
                    node.setAdvanceKey(child.getAdvanceKey());
                    if (log.isTraceEnabled()) {
                        log.trace("findBestJumpKey, HEAD node jumpKey: " + node.getAdvanceKey());
                    }
                    
                    return (null == node.getAdvanceKey()) ? null : new Key(node.getAdvanceKey());
                } else {
                    // problem, we encountered a node which we cannot process.
                }
            }
        }
        return null;
    }
    
    /*
     * Given the "farthest-minimum" key from findBestJumpKey(), attempt to jump all iterators to the corresponding index keys if they are not already beyond it.
     * 
     * The incoming jump key has been formatted into the structure of an event key ShardId | datatype\x00uid | <empty>
     * 
     * Why not seek? Consider an OrIterator with 10 sub-iterators. If one of them has a low event key and you call seek the lowest will move forward while the
     * remaining 9 will rewind to the seek start key only to end up on their current topKey after seek is finished. If they are already past the jump key, then
     * they should just stay where they are. Underlying jump implementations will check all pieces against the jumpKey and decide whether or not to move.
     */
    @Override
    public boolean jump(Key jumpKey) throws IOException {
        if (log.isTraceEnabled()) {
            log.trace("jump, All leaves need to advance to: " + jumpKey);
        }
        @SuppressWarnings("unchecked")
        Enumeration<BooleanLogicTreeNodeJexl> bfe = root.breadthFirstEnumeration();
        BooleanLogicTreeNodeJexl n;
        while (bfe.hasMoreElements()) {
            n = bfe.nextElement();
            n.setAdvanceKey(null);
        }
        
        // now advance all nodes to the advance key
        boolean jumped = false;
        for (BooleanLogicTreeNodeJexl leaf : positives) {
            // The child iterator will properly "jump" if needed
            if (leaf.getTopKey() != null && leaf.getTopKey().compareTo(jumpKey) < 0) {
                if (leaf.jump(jumpKey))
                    jumped = true;
            }
        }
        reHeapPriorityQueue(root);
        return jumped;
    }
    
    @Override
    public void next() throws IOException {
        if (log.isTraceEnabled()) {
            log.trace("next() method called");
        }
        boolean finished = false;
        if (positives.isEmpty()) {
            setTopKey(null);
            return;
        }
        
        Key previousJumpKey = null;
        while (!finished) {
            if (log.isTraceEnabled()) {
                this.total_events += 1;
            }
            Key jumpKey = this.findBestJumpKey();
            
            if (jumpKey == null) { // stop
                if (log.isTraceEnabled()) {
                    log.trace("next(), jump key is null, stopping");
                }
                setTopKey(null);
                return;
            }
            
            if (log.isTraceEnabled()) {
                log.trace("next(), jumpKey: " + jumpKey);
            }
            
            boolean sameAsCurrentTop = false;
            if (topKey != null) {
                // check that the uid's are not the sameAsCurrentTop NOTE: BoolLogic's topKey is an event key
                sameAsCurrentTop = getEventKeyRowDatatypeUid(jumpKey).equals(getEventKeyRowDatatypeUid(topKey));
                if (log.isTraceEnabled()) {
                    log.trace("jumpKeyRowUid: " + getEventKeyRowDatatypeUid(jumpKey) + "  topKeyRowUid: " + getEventKeyRowDatatypeUid(topKey));
                }
            }
            
            if (log.isTraceEnabled()) {
                log.trace("previousJumpKey: " + previousJumpKey);
                log.trace("current JumpKey: " + jumpKey);
            }
            
            if (!this.overallRange.contains(jumpKey)) {
                if (log.isTraceEnabled()) {
                    log.trace("jumpKey is outside of range, that means the next key is out of range, stopping");
                    log.trace("jumpKey: " + jumpKey + " overallRange.endKey: " + overallRange.getEndKey());
                }
                // stop
                setTopKey(null);
                return;
            }
            
            boolean sameAsPreviousJumpKey = false;
            if (previousJumpKey != null) {
                sameAsPreviousJumpKey = previousJumpKey.equals(jumpKey);
            }
            // -----------------------------------
            // OPTIMIZED block NOTE: jump method takes care of recreating priority queue for the positive leaves
            if (!sameAsCurrentTop && !sameAsPreviousJumpKey) {
                log.trace("next() optimized next block");
                previousJumpKey = jumpKey;
                jump(jumpKey); // attempt to jump everybody forward to this row and uid.
                
                // now test the tree state.
                if (testTreeState()) {
                    Key tempKey = root.getTopKey();
                    // it is potentially valid, now we need to seek all of the negatives
                    if (!negatives.isEmpty()) {
                        advanceNegatives(this.root.getTopKey());
                        if (!testTreeState()) {
                            continue;
                        }
                    }
                    
                    if (root.getTopKey().equals(tempKey)) {
                        // it's valid set nextKey and make sure it's not the sameAsCurrentTop as topKey.
                        if (log.isTraceEnabled()) {
                            if (this.root.hasTop()) {
                                log.trace("this.root.getTopKey()->" + this.root.getTopKey());
                            } else {
                                log.trace("next, this.root.getTopKey() is null");
                            }
                            
                            if (topKey != null) {
                                log.trace("topKey->" + topKey);
                                
                            } else {
                                log.trace("topKey is null");
                            }
                        }
                        if (compare(topKey, this.root.getTopKey()) != 0) {
                            setTopKey(this.root.getTopKey());
                            if (log.isTraceEnabled()) {
                                log.trace("next, (used jump) returning topKey: " + this.getTopKey());
                                this.matched_events += 1;
                            }
                            return;
                        }
                    }
                }
                
                // --------------------------------------
                // Regular next block
            } else {
                log.trace("next() regular next block");
                BooleanLogicTreeNodeJexl node;
                while (true) {
                    node = positives.poll();
                    if (!node.isDone() && node.hasTop()) {
                        break;
                    }
                    
                    if (positives.isEmpty()) {
                        if (log.isTraceEnabled()) {
                            log.trace("next, (non-jump block), no top key found, stopping");
                        }
                        setTopKey(null);
                        return;
                    }
                }
                
                if (log.isTraceEnabled()) {
                    if (topKey == null) {
                        log.trace("no jump, jumpKey: " + jumpKey + "  topKey: null");
                    } else {
                        log.trace("no jump, jumpKey: " + jumpKey + "  topKey: " + topKey);
                    }
                    log.trace("next, (no jump) min node: " + node);
                }
                node.next();
                resetNegatives();
                
                if (!node.hasTop()) {
                    // it may be part of an or, so it could be ok.
                    node.setValid(false);
                    if (testTreeState()) {
                        // it's valid set nextKey and make sure it's not the sameAsCurrentTop as topKey.
                        if (null != topKey && topKey.compareTo(this.root.getTopKey()) != 0) {
                            if (this.overallRange != null) {
                                if (this.overallRange.contains(root.getTopKey())) {
                                    setTopKey(this.root.getTopKey());
                                    if (log.isTraceEnabled()) {
                                        log.trace("next, (non jump block), returning topKey: " + this.getTopKey());
                                        this.matched_events += 1;
                                    }
                                    return;
                                } else {
                                    setTopKey(null);
                                    if (log.isTraceEnabled()) {
                                        log.trace("next, (non-jump block), no topKey, finished");
                                    }
                                    return;
                                }
                                
                            } else {
                                setTopKey(this.root.getTopKey());
                                if (log.isTraceEnabled()) {
                                    log.trace("next, (non jump block), returning topKey: " + this.getTopKey());
                                }
                                return;
                            }
                        }
                    }
                } else {
                    
                    if (overallRange.contains(node.getTopKey())) {
                        // the node had something so push it back into priority queue
                        positives.add(node);
                    }
                    
                    // now test the tree state.
                    if (testTreeState()) {
                        Key tempKey = root.getTopKey();
                        // it is potentially valid, now we need to seek all of the negatives
                        if (!negatives.isEmpty()) {
                            advanceNegatives(this.root.getTopKey());
                            if (!testTreeState()) {
                                continue;
                            }
                        }
                        
                        if (root.getTopKey().equals(tempKey)) {
                            // it's valid set nextKey and make sure it's not the sameAsCurrentTop as topKey.
                            if (log.isTraceEnabled()) {
                                if (this.root.hasTop()) {
                                    log.trace("this.root.getTopKey()->" + this.root.getTopKey());
                                } else {
                                    log.trace("next, this.root.getTopKey() is null");
                                }
                                
                                if (topKey != null) {
                                    log.trace("topKey->" + topKey);
                                    
                                } else {
                                    log.trace("topKey is null");
                                }
                            }
                            
                            if (compare(topKey, this.root.getTopKey()) != 0) {
                                if (this.overallRange != null) {
                                    if (overallRange.contains(this.root.getTopKey())) {
                                        setTopKey(this.root.getTopKey());
                                        if (log.isTraceEnabled()) {
                                            log.trace("next, (non jump block), returning topKey: " + this.getTopKey());
                                            this.matched_events += 1;
                                        }
                                        return;
                                    } else {
                                        setTopKey(null);
                                        if (log.isTraceEnabled()) {
                                            log.trace("next, (no jump), no topkey, finished.");
                                        }
                                        return;
                                    }
                                } else {
                                    setTopKey(this.root.getTopKey());
                                    if (log.isTraceEnabled()) {
                                        log.trace("next, (non jump block), returning topKey: " + this.getTopKey());
                                    }
                                    return;
                                }
                            }
                        }
                    }
                    
                }
                
                // is the priority queue empty?
                if (positives.isEmpty()) {
                    finished = true;
                    setTopKey(null);
                }
            }
        }
    }
    
    /*
     * create a range for the given row of the
     */
    private void advanceNegatives(Key k) throws IOException {
        if (log.isTraceEnabled()) {
            log.trace("advancingNegatives for Key: " + k);
        }
        Text rowID = k.getRow();
        Text colFam = k.getColumnFamily();
        
        for (BooleanLogicTreeNodeJexl neg : negatives) {
            Key startKey = new Key(rowID, neg.getFieldName(), new Text(neg.getFieldValue() + NULL_BYTE_STRING + colFam));
            Key endKey = new Key(rowID, neg.getFieldName(), new Text(neg.getFieldValue() + NULL_BYTE_STRING + colFam + ONE_BYTE_STRING));
            Range range = new Range(startKey, true, endKey, false);
            
            if (log.isTraceEnabled()) {
                log.trace("range: " + range);
            }
            neg.seek(range, EMPTY_COL_FAMS, false);
            
            if (neg.hasTop()) {
                neg.setValid(false);
            }
            if (log.isTraceEnabled()) {
                if (neg.hasTop()) {
                    log.trace("neg top key: " + neg.getTopKey());
                } else {
                    log.trace("neg has no top");
                }
            }
        }
    }
    
    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        if (!this.initialized) {
            initialize();
        } else {
            // We want to ensure that splitLeaves is *always* called
            splitLeaves(this.root);
        }
        
        this.overallRange = range;
        if (log.isTraceEnabled()) {
            log.trace("seek, overallRange: " + overallRange);
        }
        // Given some criteria, advance all iterators to that position.
        // NOTE: All of our iterators exist in the leaves.
        setTopKey(null);
        root.setTopKey(null);
        
        // We found out that scanner execution can be periodically interrupted
        // and this iterator re-seeked to a saved position. Luckily, we should
        // be able to detect this based on the incoming range start key and use
        // the jump method to get all sub iterators back into their positions.
        if (range.getStartKey() != null && range.getStartKey().getColumnFamily() != null
                        && range.getStartKey().getColumnFamily().toString().split(NULL_BYTE_STRING).length == 2) {
            // we have been restarted after an interrupt, will need to do something special
            if (log.isTraceEnabled()) {
                log.trace("seeked to a specific uid key, most likely due to scanner interruption, resyncronizing.  Range.getStartKey: " + range.getStartKey());
                log.trace("Get everybody to the beginning...");
            }
            for (BooleanLogicTreeNodeJexl node : positives) {
                node.setDone(false);
                node.seek(range, columnFamilies, inclusive);
                if (log.isTraceEnabled()) {
                    String tk = "empty";
                    if (node.hasTop()) {
                        tk = node.getTopKey().toString();
                    }
                    log.trace("seek, positive leaf: " + node.getContents() + " topKey: " + tk);
                }
            }
            
        } else {
            
            // don't take this out, if you jump rows on the tablet you could have
            // pulled nodes out of the positives priority queue. On a call to seek
            // it is usually jumping rows, so everything needs to become possibly
            // valid again.
            for (BooleanLogicTreeNodeJexl node : positives) {
                node.setDone(false);
                node.seek(range, columnFamilies, inclusive);
                if (log.isTraceEnabled()) {
                    String tk = "empty";
                    if (node.hasTop()) {
                        tk = node.getTopKey().toString();
                    }
                    log.trace("seek, positive leaf: " + node.getContents() + " topKey: " + tk);
                }
            }
        }
        
        // Now that all nodes have been seek'd recreate the priorityQueue to sort them properly.
        reHeapPriorityQueue(root);
        resetNegatives();
        
        if (log.isTraceEnabled()) {
            log.trace("seek, overallRange: " + overallRange);
        }
        
        // test Tree, if it's not valid, call next
        if (testTreeState() && overallRange.contains(root.getTopKey())) {
            if (!negatives.isEmpty()) {
                // now advance negatives
                advanceNegatives(this.root.getTopKey());
                if (!testTreeState()) {
                    next();
                }
            }
            
            if (log.isTraceEnabled()) {
                log.trace("seek, Comparing topKey : " + this.root.getTopKey() + " to range: " + overallRange);
            }
            
            if (this.root.isValid() && overallRange.contains(this.root.getTopKey())) {
                setTopKey(this.root.getTopKey());
                if (log.isTraceEnabled()) {
                    log.trace("seek, topKey : " + this.getTopKey());
                }
            } else {
                setTopKey(null);
                if (log.isTraceEnabled()) {
                    log.trace("seek, no topKey found");
                }
            }
        } else {
            // seek failed in the logic test, but there may be other possible
            // values which satisfy the logic tree. Make sure our iterators aren't
            // all null, and then call next.
            
            if (log.isTraceEnabled()) {
                log.trace("seek, testTreeState is false, HEAD(root) does not have top, calling next()");
            }
            // check nodes in positives to see if they're all null/outside range
            // or if nothing percolated up to root yet.
            Iterator<BooleanLogicTreeNodeJexl> iter = positives.iterator();
            while (iter.hasNext()) {
                BooleanLogicTreeNodeJexl node = iter.next();
                if (!node.hasTop() || !overallRange.contains(node.getTopKey())) {
                    log.debug("Removing node from positives: " + node);
                    iter.remove();
                }
            }
            
            // Next will fail fast if positives was made empty
            next();
        }
    }
    
    private int compare(Key k1, Key k2) {
        if (k1 != null && k2 != null) {
            return k1.compareTo(k2);
        } else if (k1 == null && k2 == null) {
            return 0;
        } else if (k1 == null) { // in this case, null is considered bigger b/c it's closer to the end of the table.
            return 1;
        } else {
            return -1;
        }
    }
    
    /* *************************************************************************
     * Inner classes
     */
    public static class BooleanLogicTreeNodeComparator implements Comparator<Object> {
        
        @Override
        public int compare(Object o1, Object o2) {
            BooleanLogicTreeNodeJexl n1 = (BooleanLogicTreeNodeJexl) o1;
            BooleanLogicTreeNodeJexl n2 = (BooleanLogicTreeNodeJexl) o2;
            
            Key k1 = n1.getTopKey();
            Key k2 = n2.getTopKey();
            if (log.isTraceEnabled()) {
                String t1 = "null";
                String t2 = "null";
                StringBuilder b = new StringBuilder();
                if (k1 != null) {
                    t1 = b.append(k1.getRow()).append(NULL_BYTE_STRING).append(k1.getColumnFamily()).toString();
                }
                b.delete(0, b.length());
                if (k2 != null) {
                    t2 = b.append(k2.getRow()).append(NULL_BYTE_STRING).append(k2.getColumnFamily()).toString();
                }
                if (log.isTraceEnabled()) {
                    log.trace("BooleanLogicTreeNodeComparator   \tt1: " + t1 + "  t2: " + t2);
                }
            }
            
            if (k1 != null && k2 != null) {
                return k1.compareTo(k2);
            } else if (k1 == null && k2 == null) {
                return 0;
            } else if (k1 == null) {
                return 1;
            } else {
                return -1;
            }
            
        }
    }
    
    @Override
    public IteratorOptions describeOptions() {
        Map<String,String> options = new HashMap<>();
        options.put(FIELD_INDEX_QUERY, "query expression");
        options.put(BASE_CACHE_DIR, "Base directory for caching sorted set for regex evaluations");
        options.put(QueryParameters.UNEVALUATED_FIELDS, "List of index-only fields");
        return new IteratorOptions(getClass().getSimpleName(), "evaluates event objects against an expression using the field index", options, null);
    }
    
    @Override
    public boolean validateOptions(Map<String,String> options) {
        if (!options.containsKey(FIELD_INDEX_QUERY)) {
            return false;
        }
        this.updatedQuery = options.get(FIELD_INDEX_QUERY);
        
        if (options.containsKey(QueryParameters.UNEVALUATED_FIELDS)) {
            String unevalFields = options.get(QueryParameters.UNEVALUATED_FIELDS);
            if (unevalFields != null && !unevalFields.trim().equals("")) {
                Collections.addAll(this.unevaluatedFields, StringUtils.split(unevalFields, GenericShardQueryConfiguration.PARAM_VALUE_SEP));
            }
        } else {
            // Warn if no unevaluatedFields were provided (doesn't *need* to fail)
            log.debug("No unevaluated fields were provided to the filters");
        }
        
        // if a base regex cache dir is set, then we need to initialize the FileSystem etc.
        if (options.containsKey(BASE_CACHE_DIR)) {
            this.baseRegexCacheDir = new Path(options.get(BASE_CACHE_DIR));
            Configuration conf = new Configuration();
            for (Map.Entry<String,String> entry : options.entrySet()) {
                conf.set(entry.getKey(), entry.getValue());
            }
            try {
                this.fs = FileSystem.get(conf);
            } catch (Exception e) {
                throw new IllegalArgumentException("Cannot create hadoop file system from options", e);
            }
        }
        
        return true;
    }
}
