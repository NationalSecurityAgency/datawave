package nsa.datawave.query.parser;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import nsa.datawave.core.iterators.EvaluatingIterator;
import nsa.datawave.core.iterators.GlobalIndexRangeSamplingIterator;
import nsa.datawave.core.iterators.GlobalIndexShortCircuitIterator;
import nsa.datawave.core.iterators.filter.GlobalIndexDateRangeFilter;
import nsa.datawave.core.iterators.uid.GlobalIndexUidMappingIterator;
import nsa.datawave.iterators.IteratorSettingHelper;
import nsa.datawave.query.config.GenericShardQueryConfiguration;
import nsa.datawave.query.iterators.IndexRegexFilter;
import nsa.datawave.query.iterators.IndexRegexIterator;
import nsa.datawave.query.parser.JavaRegexAnalyzer.JavaRegexParseException;
import nsa.datawave.query.rewrite.Constants;
import nsa.datawave.query.rewrite.exceptions.TooManyTermsException;
import nsa.datawave.query.tables.ScannerFactory;
import nsa.datawave.webservice.query.Query;
import nsa.datawave.webservice.query.exception.BadRequestQueryException;
import nsa.datawave.webservice.query.exception.DatawaveErrorCode;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.jexl2.parser.ASTStringLiteral;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.commons.jexl2.parser.ParserTreeConstants;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.math.LongRange;
import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

/**
 * This class is used to query the global indices to determine that set of ranges to use when querying the shard table.
 */
@Deprecated
public class RangeCalculator {
    
    private static final Logger log = Logger.getLogger(RangeCalculator.class);
    private static final String START_SHARD = "_0";
    
    /**
     * Object that is used to hold ranges found in the index. Subclasses may compute the final range set in various ways.
     */
    protected static class TermRange {
        
        private Set<Range> ranges = new TreeSet<>();
        private long cardinality = -1;
        private boolean completeRange = true;
        private List<String> aliases;
        
        public TermRange(DatawaveTreeNode node, Set<Range> ranges, long cardinality) {
            this(node, ranges, cardinality, null);
        }
        
        public TermRange(DatawaveTreeNode node, Set<Range> ranges, long cardinality, List<String> aliasList) {
            if (ranges != null) {
                this.ranges.addAll(ranges);
            }
            this.cardinality = cardinality;
            this.aliases = (aliasList == null) ? Collections.<String> emptyList() : aliasList;
        }
        
        public void addAll(Set<Range> r) {
            ranges.addAll(r);
        }
        
        public void add(Range r) {
            ranges.add(r);
        }
        
        public Set<Range> getRanges() {
            return ranges;
        }
        
        public long getCardinality() {
            return this.cardinality;
        }
        
        public void setCardinality(int card) {
            this.cardinality = card;
        }
        
        public boolean isCompleteRange() {
            return this.completeRange;
        }
        
        public void setCompleteRange(boolean val) {
            this.completeRange = val;
        }
        
        public List<String> getAliases() {
            return this.aliases;
        }
        
        @Override
        public String toString() {
            ToStringBuilder tsb = new ToStringBuilder(this);
            tsb.append("ranges", ranges);
            return tsb.toString();
        }
    }
    
    /**
     * Used to rank nodes in handleAND from the easiest to evaluate to the most complex/expensive to evaluate. This allows the AND evaluation to be terminated
     * early with the least expense.
     */
    public static class DatawaveTreeNodeComparator implements Comparator<DatawaveTreeNode> {
        
        private Map<Integer,Integer> nodeComplexityRank = new HashMap<>();
        
        public DatawaveTreeNodeComparator() {
            super();
            nodeComplexityRank.put(ParserTreeConstants.JJTEQNODE, 1);
            nodeComplexityRank.put(ParserTreeConstants.JJTANDNODE, 2);
            nodeComplexityRank.put(ParserTreeConstants.JJTORNODE, 3);
            nodeComplexityRank.put(ParserTreeConstants.JJTERNODE, 4);
        }
        
        @Override
        public int compare(DatawaveTreeNode node1, DatawaveTreeNode node2) {
            
            if (null == node1 && null == node2)
                return 0;
            
            if (null == node1 && null != node2)
                return -1;
            
            if (null != node1 && null == node2)
                return 1;
            
            int node1Complexity = 5;
            if (!node1.isRangeNode())
                node1Complexity = nodeComplexityRank.get(node1.getType());
            
            int node2Complexity = 5;
            if (!node2.isRangeNode())
                node2Complexity = nodeComplexityRank.get(node2.getType());
            
            return Integer.compare(node1Complexity, node2Complexity);
            
        }
    }
    
    /**
     * Thrown by queryGlobalIndexForRange when there is an range expansion issue for a node such as a required field expands to too many terms. We can often
     * recover from these, such as whene they occur in a non-required branch of a query (one side of an AND).
     */
    @SuppressWarnings("serial")
    public static class RangeExpansionException extends Exception {
        /** the node that caused the exception */
        private final DatawaveTreeNode problemNode;
        
        /** reason message */
        private final String reason;
        
        public RangeExpansionException(DatawaveTreeNode problemNode, String reason) {
            this.problemNode = problemNode;
            this.reason = reason;
        }
        
        @Override
        public String getMessage() {
            return super.getMessage() + " Reason: " + reason;
        }
        
        @Override
        public String toString() {
            return super.toString() + " Reason: " + reason;
        }
        
        public DatawaveTreeNode getProblemNode() {
            return this.problemNode;
        }
    }
    
    /**
     * Thrown by queryGlobalIndexForRange when there is a range expansion exception on a index-only (unevaluated) field.
     */
    @SuppressWarnings("serial")
    public static class FatalRangeExpansionException extends RangeExpansionException {
        public FatalRangeExpansionException(DatawaveTreeNode problemNode, String reason) {
            super(problemNode, reason);
        }
    }
    
    /** Interface for getting and returning scanner instances */
    private ScannerFactory scannerFactory;
    
    /** Accumulo auths */
    private Set<Authorizations> auths;
    
    /** Query start date */
    private Date begin;
    
    /** Query end date */
    private Date end;
    
    /** Holds begin and end date in long form, quantized to the day */
    private LongRange dateRange;
    
    /** Formats dates into shard YYYYMMDD format */
    private SimpleDateFormat shardDateFormatter;
    
    /** Forward global term->shard/doc index */
    private String shardIndexTableName;
    
    /** Reverse global term->shard/doc index */
    private String shardReverseIndexTableName;
    
    /** Number of threads used in batch scanner when performing range lookups */
    private int queryThreads = 8;
    
    /** events that exceed this threshold will return a single range of the entire day */
    private int eventPerDayThreshold = 4800;
    
    /** exceeding this threshold for shards ina day will return a single range of the entire day */
    private int shardsPerDayThreshold = 100;
    
    /** Total number of unique terms you will expand a range to; i.e. X <= 500 and X >=1 yields 500 potential terms. */
    private int rangeExpansionThreshold = 1000;
    
    /** Total number of query terms we will allow in the entire query */
    private int maxTermExpansionThreshold = 5000;
    
    /** The uid mapper class if any */
    private String uidMapperClass = null;
    
    /** The root of the re-written query tree */
    private DatawaveTreeNode queryNode;
    
    /** Total number of terms seen so far */
    private long allTermsCount = 0;
    
    /** A list of the index-only fields, because we can't drop those and recover. */
    private Set<String> unevaluatedFields;
    
    /** final set of ranges for lookup in the shard table */
    private Set<Range> result = null;
    
    private Query query = null;
    
    /**
     * @return set of ranges to use for the shard table
     */
    public Set<Range> getResult() {
        return result;
    }
    
    /**
     * Execute the range calculation.
     *
     * @param config
     *            The query configuration object
     * @param indexedFieldNames
     *            A list of indexed fields.
     * @param masterRoot
     *            The query root node
     * @throws ParseException
     *             if there is a problem parsing the query.s
     * @throws RangeExpansionException
     *             if a range expanded to too many individual terms.
     * @throws TooManyTermsException
     *             too many terms in the query.
     * @throws JavaRegexParseException
     */
    public void execute(GenericShardQueryConfiguration config, Set<String> indexedFieldNames, DatawaveTreeNode masterRoot, ScannerFactory scannerFactory)
                    throws ParseException, RangeExpansionException, TooManyTermsException, JavaRegexParseException {
        
        if (log.isDebugEnabled()) {
            log.debug("RangeCalculator.execute()");
        }
        
        this.scannerFactory = scannerFactory;
        this.auths = config.getAuthorizations();
        this.begin = config.getBeginDate();
        
        // need to push the "end" to the next day to get around lexigraphic sorting with the "shard_partitionid"
        // the end range that is calculated should essentially fall at the very beginning of the next shard_partitionid, but
        // will have no "_paritionid" part which would include that extra day.
        this.end = getEndDateForIndexLookup(config.getEndDate());
        
        // The begin and end dates from the query may be down to the second, for doing lookups in the index we
        // want to use the day.
        this.dateRange = new LongRange(DateUtils.truncate(config.getBeginDate(), Calendar.DAY_OF_MONTH).getTime(), this.end.getTime());
        
        this.shardDateFormatter = config.getShardDateFormatter();
        this.shardIndexTableName = config.getIndexTableName();
        this.shardReverseIndexTableName = config.getReverseIndexTableName();
        
        // unevaluatedFields are fields that are index-only and don't go through
        // JEXL evaluation. If we catch a RangeExpansionException on one of those
        // we can't recover and must throw the exception up.
        this.unevaluatedFields = (null == config.getUnevaluatedFields()) ? Collections.<String> emptySet() : new HashSet<>(config.getUnevaluatedFields());
        
        this.queryThreads = config.getNumQueryThreads();
        
        // Get the thresholds
        this.eventPerDayThreshold = config.getEventPerDayThreshold();
        this.shardsPerDayThreshold = config.getShardsPerDayThreshold();
        this.rangeExpansionThreshold = config.getRangeExpansionThreshold();
        this.maxTermExpansionThreshold = config.getMaxTermExpansionThreshold();
        this.allTermsCount = 0;
        
        // Get the uid mapper if any
        this.uidMapperClass = config.getUidMapperClass();
        
        this.query = config.getQuery();
        
        // Copy the query parse tree and post-process it.
        DatawaveQueryAnalyzer analyzer = new DatawaveQueryAnalyzer();
        DatawaveTreeNode root = analyzer.copyTree(masterRoot);
        try {
            root = analyzer.removeNonIndexedFields(root, indexedFieldNames);
            root = analyzer.removeNullValueNodes(root);
            root = analyzer.removeNegationViolations(root);
            root = analyzer.removeUnhandledRegexNodes(root);
            if (log.isDebugEnabled()) {
                log.debug("query for RangeCalc: " + root.getContents());
            }
        } catch (Exception ex) {
            log.error("Error removing non-indexed fields in RangeCalculator!", ex);
            this.result = null;
            return;
        }
        
        if (log.isDebugEnabled()) {
            log.debug("RangeCalc, processed query: " + root.getContents());
        }
        
        /*
         * Walk the parse tree backwards, NOTE, there will be parse tree modifications made where applicable for range/regex nodes.
         */
        @SuppressWarnings("unchecked")
        List<DatawaveTreeNode> backwards = Collections.<DatawaveTreeNode> list(root.breadthFirstEnumeration());
        for (int i = backwards.size() - 1; i >= 0; i--) {
            if (!backwards.get(i).isLeaf()) { // leaves are processed as a part of their parents.
                DatawaveTreeNode node = backwards.get(i);
                if (node.isChildrenAllNegated() || node.isNegated() || node.isFunctionNode()) {
                    // Can't do anything here.
                    node.setCardinality(0);
                    continue;
                }
                
                if (node.getType() == ParserTreeConstants.JJTANDNODE) {
                    node = handleAND(node, config.getDatatypeFilter());
                    // node had zero range results NOT because of a RangeExpansionException.
                    if (node.isRemoval()) {
                        node.removeFromParent();
                    }
                    
                } else if (node.getType() == ParserTreeConstants.JJTORNODE) {
                    node = handleOR(node, config.getDatatypeFilter());
                    
                } else if (node.getType() == ParserTreeConstants.JJTJEXLSCRIPT) {
                    root = handleHEAD(node, config.getDatatypeFilter());
                    
                } else {
                    log.error("Unknown Node in RangeCalculation: " + node.getContents());
                }
            }
        } // end for loop
        
        this.result = root.getRanges();
        
        try {
            this.queryNode = analyzer.collapseBranches(root);
            // in case we truncated an or, technically queryNode has already
            // been set to the root and it's all pass by reference,
            // but make it explicit.
            log.debug("Query before remove null values: " + root.getContents());
            this.queryNode = analyzer.removeNullValueNodes(root);
            log.debug("Query after range calculation: " + root.getContents());
        } catch (Exception ex) { // all nodes are null, no terms left.
            log.debug("No query terms left in Query, RangeCalc result should be empty set");
            this.result = Collections.emptySet();
            this.queryNode = root;
        }
    }
    
    public DatawaveTreeNode getQueryNode() {
        return this.queryNode;
    }
    
    /**
     * Handle the HEAD node in the query, which has only one child.
     *
     * @param node
     *            The node we're evaluating.
     * @param typeFilter
     *            The types we want to ignore
     * @return The modified node.
     * @throws RangeExpansionException
     * @throws TooManyTermsException
     * @throws JavaRegexParseException
     */
    private DatawaveTreeNode handleHEAD(DatawaveTreeNode node, Set<String> typeFilter) throws RangeExpansionException, TooManyTermsException,
                    JavaRegexParseException {
        if (log.isDebugEnabled()) {
            log.debug("Range calc, handleHEAD");
        }
        
        if (node.getChildCount() < 1) {
            log.warn("RangeCalcualtor query has no elements");
            // all children were removed, meaning we found nothing.
            node.setRanges(new HashSet<Range>());
            node.setCardinality(0);
            return node;
        }
        
        // there will be only one child.
        DatawaveTreeNode child = (DatawaveTreeNode) node.getFirstChild();
        
        if (child.hasRangeExpansionException()) {
            log.trace("handleHEAD, child is pending removal due to RangeExpansionException: " + child);
            throw child.getRangeExpansionException();
        }
        
        // NOTE, if the node is a range node but not a regex, it is handled
        // below in the else clause that handles the single term condition.
        // Regex's need to be converted first.
        if (child.getType() == ParserTreeConstants.JJTERNODE) {
            DatawaveTreeNode rangeNode = convertRegexToRange(child);
            String tbName = this.shardIndexTableName;
            if (rangeNode.isReverseIndex()) {
                tbName = this.shardReverseIndexTableName;
            }
            try {
                TermRange tr = queryGlobalIndexForRange(rangeNode, tbName, typeFilter, true /* required */, Long.MAX_VALUE);
                if (tr.getRanges() != null) {
                    // We want to propagate the ranges we found for the Wildcarded term to this node
                    node.setRanges(tr.getRanges());
                    node.setCardinality(tr.getCardinality());
                    
                    // Set it on the term as well as the head
                    child.setCardinality(tr.getCardinality());
                    child.setRanges(tr.getRanges());
                    
                    // We found ranges, so expand the wildcard into an OR of discrete terms
                    if (tr.getRanges().size() > 0) {
                        aliasNode(tr, child);
                    }
                }
            } catch (TableNotFoundException ex) {
                log.error("index table not found", ex);
                throw new RuntimeException(" index table not found", ex);
            }
        } else if (child.getType() == ParserTreeConstants.JJTANDNODE || child.getType() == ParserTreeConstants.JJTORNODE) {
            node.setRanges(child.getRanges());
            node.setCardinality(child.getCardinality());
        } else { // single term, need to query for it.
            try {
                
                TermRange tr;
                if (child.isRangeNode() || child.isUnboundedRange()) { // note: regex has already been handled.
                    DatawaveTreeNode rangeNode = child;
                    String tbName = this.shardIndexTableName;
                    if (log.isDebugEnabled()) {
                        log.debug("handleHEAD, contains range node type: " + child.getType() + "  node: " + child.printRangeNode());
                    }
                    tr = queryGlobalIndexForRange(rangeNode, tbName, typeFilter, true /* required */, Long.MAX_VALUE);
                    if (tr == null || tr.getRanges().isEmpty()) {
                        node.setCardinality(-1);
                        node.setRanges(new HashSet<Range>());
                        
                        // Set it on the term as well as the head
                        child.setCardinality(-1);
                        child.setRanges(new HashSet<Range>());
                    } else {
                        node.setCardinality(tr.getCardinality());
                        node.setRanges(tr.getRanges());
                        
                        // Set it on the term as well as the head
                        child.setCardinality(tr.getCardinality());
                        child.setRanges(tr.getRanges());
                        
                        aliasNode(tr, child);
                    }
                } else if (!child.isFunctionNode() && child.getType() != ParserTreeConstants.JJTFUNCTIONNODE) {
                    // lookup ranges in global index
                    tr = queryGlobalIndexForTerm(child, this.shardIndexTableName, typeFilter);
                    log.debug("handleHEAD, singleTerm, tr: " + tr);
                    if (tr == null || tr.getRanges().isEmpty()) {
                        node.setCardinality(-1);
                        node.setRanges(new HashSet<Range>());
                        
                        // Set it on the term as well as the head
                        child.setCardinality(-1);
                        child.setRanges(new HashSet<Range>());
                    } else {
                        node.setCardinality(tr.getCardinality());
                        node.setRanges(tr.getRanges());
                        
                        // Set it on the term as well as the head
                        child.setCardinality(tr.getCardinality());
                        child.setRanges(tr.getRanges());
                    }
                }
                
            } catch (TableNotFoundException e) {
                log.error("index table not found", e);
                throw new RuntimeException(" index table not found", e);
            }
        }
        if (log.isDebugEnabled()) {
            log.debug("RangeCalculator, HEAD node range cardinality: " + node.getCardinality());
        }
        return node;
    }
    
    /**
     * Process the children in this AND node and set the cardinality/ranges accordingly.
     *
     * CASE 1: If a node has zero matching children, then we can short circuit the evaluation since it will have zero results. Mark ourselves for removal but do
     * not remove any children.
     *
     * CASE 2: If we have a child node with a RangeExpansionException, mark that child for pending removal due to RangeExpansionException and do not include
     * them in the normal processing logic for cardinality/ranges. Remove this specially marked children at the end of the processing. If all children are
     * marked to be removed because of expansion exceptions, we need to mark ourself as needing removal due to expansion exception.
     *
     * @param node
     *            The node we're evaluating
     * @param typeFilter
     *            The types to ignore
     * @return The modified node.
     * @throws RangeExpansionException
     * @throws TooManyTermsException
     * @throws JavaRegexParseException
     */
    private DatawaveTreeNode handleAND(DatawaveTreeNode node, Set<String> typeFilter) throws RangeExpansionException, TooManyTermsException,
                    JavaRegexParseException {
        if (log.isDebugEnabled()) {
            log.debug("Range calc, handleAND");
        }
        
        // Push all children to a priority queue, so they are processed in
        // an efficient order: term, AND, OR, Regex, Range. One set of empty
        // results will terminate this early.
        PriorityQueue<DatawaveTreeNode> sortedChildren = new PriorityQueue<>(node.getChildCount(), new DatawaveTreeNodeComparator());
        
        @SuppressWarnings("unchecked")
        Enumeration<DatawaveTreeNode> childrenEnum = node.children();
        while (childrenEnum.hasMoreElements()) {
            DatawaveTreeNode child = childrenEnum.nextElement();
            if (child.isNegated() || child.isFunctionNode() || child.isChildrenAllNegated()) {
                continue; // skip negations & functions.
            }
            
            if (child.hasFatalRangeExpansionException()) {
                log.debug("handleAND, child has fatal range expansion exception " + child.getContents());
                node.setRangeExpansionException(child.getRangeExpansionException());
                child.removeFromParent();
                return node;
            }
            
            if (log.isDebugEnabled()) {
                log.debug("handleAND, adding node: " + child.getContents());
            }
            sortedChildren.add(child);
        }
        
        boolean handleRangeExpansionExceptionRemoval = false;
        Set<Range> currentRangeSet = null;
        long currentCardinality = Long.MAX_VALUE; // worst case for an AND
        
        while (!sortedChildren.isEmpty()) {
            DatawaveTreeNode child = sortedChildren.poll();
            
            if (log.isDebugEnabled()) {
                log.debug("handleAND, processing child: " + child.getContents());
            }
            
            if (child.hasRangeExpansionException()) {
                // If any of the children of the node we're evaluating has a
                // range expansion exception, we will ignore it for now and
                // handle it later.
                handleRangeExpansionExceptionRemoval = true;
                continue;
            }
            
            // handle ranges and regexes specially
            if (child.getType() == ParserTreeConstants.JJTERNODE || child.isRangeNode()) {
                
                DatawaveTreeNode rangeNode = child;
                // if regex, convert to a range.
                if (ParserTreeConstants.JJTERNODE == child.getType()) {
                    rangeNode = convertRegexToRange(child);
                }
                
                // set appropriate table name for range node.
                String tbName = this.shardIndexTableName;
                if (rangeNode.isReverseIndex()) {
                    tbName = this.shardReverseIndexTableName;
                }
                
                if (log.isDebugEnabled()) {
                    log.debug("handleAND, contains range node type: " + child.getType() + "  node: " + child.printRangeNode());
                }
                
                try {
                    // all ranges are not required for and and because we simply choose the best range.
                    // (note: unevaluated/index-only fields, are automatically flipped to required in this method)
                    TermRange tr = queryGlobalIndexForRange(rangeNode, tbName, typeFilter, false /* required */, currentCardinality);
                    if (log.isDebugEnabled()) {
                        log.debug("handleAND, term range: " + tr);
                    }
                    
                    // either we don't have an existing range, or this range is
                    // more selective than our other ranges, so use this range.
                    if (isMoreSelective(tr, currentRangeSet, currentCardinality)) {
                        
                        if (log.isDebugEnabled()) {
                            log.debug("handleAND, have a range node, we're using it, card: " + tr.getCardinality());
                        }
                        currentCardinality = tr.getCardinality();
                        currentRangeSet = tr.getRanges();
                        
                        aliasNode(tr, child);
                    } else {
                        // we aren't using the range, toss it.
                        child.removeFromParent();
                        if (log.isDebugEnabled()) {
                            log.debug("handleAND, have a range node, but we're not using it.");
                        }
                    }
                } catch (FatalRangeExpansionException ree) {
                    // fatal, can't find complete range for index-only term.
                    log.debug("handleAND caught " + ree.getClass().getSimpleName());
                    node.setRangeExpansionException(ree);
                    child.removeFromParent();
                    return node; // bail out now.
                } catch (RangeExpansionException ree) {
                    // non-fatal, can't find complete range for term
                    // which we can find later in an event.
                    log.debug("handleAND caught " + ree.getClass().getSimpleName());
                    child.setRangeExpansionException(ree);
                    handleRangeExpansionExceptionRemoval = true;
                    continue; // continuing processing children.
                } catch (TableNotFoundException ex) {
                    log.error("index table not found", ex);
                    throw new RuntimeException(" index table not found", ex);
                }
            } else if (child.getType() == ParserTreeConstants.JJTANDNODE || child.getType() == ParserTreeConstants.JJTORNODE) {
                if (log.isDebugEnabled()) {
                    log.debug("handleAND, child AND/OR: " + child.getContents());
                }
                // either we don't have an existing range, or this range is
                // more selective than our other ranges, so use this range.
                TermRange tr = new TermRange(child, null, child.getCardinality());
                if (isMoreSelective(tr, currentRangeSet, currentCardinality)) {
                    if (log.isDebugEnabled()) {
                        log.debug("handleAND, child AND/OR, we're using it, card: " + child.getCardinality());
                    }
                    currentCardinality = child.getCardinality();
                    currentRangeSet = child.getRanges();
                }
                
            } else { // single term
                if (log.isDebugEnabled()) {
                    log.debug("handleAND, child " + child.getContents());
                }
                // lookup ranges in global index
                try {
                    // lookup ranges in global index
                    TermRange tr = queryGlobalIndexForTerm(child, this.shardIndexTableName, typeFilter);
                    child.setRanges(tr.getRanges());
                    if (isMoreSelective(tr, currentRangeSet, currentCardinality)) {
                        currentCardinality = tr.getCardinality();
                        currentRangeSet = tr.getRanges();
                    }
                    
                } catch (TableNotFoundException e) {
                    log.error("index table not found", e);
                    throw new RuntimeException(" index table not found", e);
                }
            }
            
            // An AND'd term had zero results, don't bother looking up the rest
            // of the terms in the and clause.
            if (currentRangeSet != null && currentRangeSet.isEmpty()) {
                node.setRemoval(true); // flag for removal
                break;
            }
        } // end processing children.
        
        // If we had any nodes pending removal let's clean them up.
        // Since we don't remove regular nodes with zero matches
        // in this method, if we end up having no children left, it is due to
        // all children having expansion exceptions, so we need to mark ourself
        // as having a range expansion exception.
        if (handleRangeExpansionExceptionRemoval) {
            @SuppressWarnings("unchecked")
            List<DatawaveTreeNode> children = Collections.list(node.children());
            for (int i = children.size() - 1; i >= 0; i--) {
                DatawaveTreeNode child = children.get(i);
                if (child.hasRangeExpansionException()) {
                    node.remove(child);
                }
            }
            
            // all children removed, flag this node with a range expansion exception.
            if (node.isLeaf()) {
                log.trace("AND full of rangeExpansionException children, mark it as a RangeExpansion removal.");
                node.setRangeExpansionException(new RangeExpansionException(node, "AND node with all children removed due to range expansion exceptions"));
                return node;
            }
        }
        
        node.setCardinality(currentCardinality);
        node.setRanges(currentRangeSet);
        if (log.isDebugEnabled()) {
            log.debug("handleAND, returning with cardinality: " + node.getCardinality());
        }
        return node;
    }
    
    /**
     * Process the children of this OR node and union their ranges/cardinality. If any of our children are marked with RangeExpansionExceptions, we can stop and
     * mark ourself as a RangeExpansionException.
     *
     * @param node
     *            The node we're evaluating
     * @param typeFilter
     *            The list of types to filter
     * @return The modified node.
     * @throws RangeExpansionException
     * @throws TooManyTermsException
     * @throws JavaRegexParseException
     */
    private DatawaveTreeNode handleOR(DatawaveTreeNode node, Set<String> typeFilter) throws RangeExpansionException, TooManyTermsException,
                    JavaRegexParseException {
        if (log.isDebugEnabled()) {
            log.debug("Range calc, handleOR");
        }
        
        SortedSet<Range> treeSet = new TreeSet<>();
        long currentCardinality = 0;
        
        @SuppressWarnings("unchecked")
        List<DatawaveTreeNode> children = Collections.list(node.children());
        
        for (int i = children.size() - 1; i >= 0; i--) {
            DatawaveTreeNode child = children.get(i);
            
            if (log.isDebugEnabled()) {
                log.debug("handleOR, processing child: " + child.getContents());
            }
            
            if (child.isNegated() || child.isFunctionNode() || child.isChildrenAllNegated()) {
                // skip negations and functions
                continue;
            }
            
            if (child.hasRangeExpansionException()) {
                // If any of the children of this node have a range expansion
                // exception, flag that we have a range expansions exception
                // and exit early.
                if (log.isTraceEnabled()) {
                    log.trace("handleOR had child with RangeExpansionException: " + child);
                }
                node.setRangeExpansionException(child.getRangeExpansionException());
                return node;
            }
            
            // handle ranges and regexes specially.
            if (child.getType() == ParserTreeConstants.JJTERNODE || child.isRangeNode()) {
                
                DatawaveTreeNode rangeNode = child;
                
                if (ParserTreeConstants.JJTERNODE == child.getType()) {
                    rangeNode = convertRegexToRange(child);
                }
                
                // set appropriate table name for range node.
                String tbName = this.shardIndexTableName;
                if (rangeNode.isReverseIndex()) {
                    tbName = this.shardReverseIndexTableName;
                }
                
                if (log.isDebugEnabled()) {
                    log.debug("handleOR, contains range node type: " + child.getType() + "  node: " + child.printRangeNode());
                }
                
                try {
                    TermRange tr = queryGlobalIndexForRange(rangeNode, tbName, typeFilter, true /* required */, currentCardinality);
                    if (log.isDebugEnabled()) {
                        log.debug("handleOR, term range: " + tr);
                    }
                    
                    currentCardinality = safeIncrementLong(currentCardinality, tr.getCardinality());
                    treeSet.addAll(tr.getRanges());
                    aliasNode(tr, child);
                } catch (RangeExpansionException ree) {
                    log.debug("handleOR caught " + ree.getClass().getSimpleName());
                    // node is required, but we can't calculate a full range for
                    // it. Not much else we can do here, so flag the current node
                    // as needing removal.
                    node.setRangeExpansionException(ree);
                    return node;
                } catch (TableNotFoundException ex) {
                    log.error("index table not found", ex);
                    throw new RuntimeException(" index table not found", ex);
                }
            } else if (child.getType() == ParserTreeConstants.JJTANDNODE || child.getType() == ParserTreeConstants.JJTORNODE) {
                if (log.isDebugEnabled()) {
                    log.debug("handleOR, child AND/OR: " + child.getContents());
                }
                
                currentCardinality = safeIncrementLong(currentCardinality, child.getCardinality());
                if (child.getRanges() != null) {
                    treeSet.addAll(child.getRanges());
                }
                
            } else { // single term
                if (log.isDebugEnabled()) {
                    log.debug("handleOR, child " + child.getContents());
                }
                
                // lookup ranges in global index
                try {
                    TermRange tr = queryGlobalIndexForTerm(child, this.shardIndexTableName, typeFilter);
                    currentCardinality = safeIncrementLong(currentCardinality, tr.getCardinality());
                    child.setRanges(tr.getRanges());
                    treeSet.addAll(tr.getRanges());
                } catch (TableNotFoundException e) {
                    log.error("index table not found", e);
                    throw new RuntimeException(" index table not found", e);
                }
            }
        } // end processing children
        
        // trim the remaining range set.
        trimRangeSet(treeSet);
        
        // Take another pass through the children to remove empty nodes.
        for (int i = children.size() - 1; i >= 0; i--) {
            DatawaveTreeNode child = children.get(i);
            if (child.isNegated() || child.isFunctionNode() || child.isChildrenAllNegated()) {
                // skip negations and functions
                continue;
            }
            if (child.getRanges() == null || child.getRanges().isEmpty()) {
                log.debug("handleOR, removing node with empty ranges: " + child);
                child.removeFromParent();
            }
        }
        
        node.setCardinality(currentCardinality);
        node.setRanges(treeSet);
        if (log.isTraceEnabled()) {
            log.trace("handleOR, returning with cardinality: " + node.getCardinality());
        }
        return node;
    }
    
    /**
     * Eliminate overlapping ranges from the set of ranges.
     * <p/>
     * Ranges appear in the form:
     * <dl>
     * <dt>day:</dt>
     * <dd>20120101</dd>
     * <dt>shard/datatype:</dt>
     * <dd>20120101_0/datatype</dd>
     * <dt>shard/datatype/uid (event specific range):</dt>
     * <dd>20120101_0/datatype/foo.bar.baz</dd>
     * </dl>
     * <p/>
     * The ranges are provided to this method in sorted order so that day ranges appear before shard/datatype ranges for that day, which in turn appear before
     * shard/datatype/uid ranges for the same shard and datatype.
     * <p/>
     * As a result, we can iterate through the sorted ranges and eliminate those that overlap. If we have a day, we can remove all other ranges for shards that
     * fall within that day. If we have a range for a shard,datatype, we can remove all event specific ranges for that shard,datatype pair.
     * <p/>
     * Shards are stored in the row, datatypes and uid's are stored in the column family separated by a null
     *
     * @param ranges
     *            A set of ranges sorted in lexical order
     */
    private static final void trimRangeSet(SortedSet<Range> ranges) {
        Iterator<Range> rangeIterator = ranges.iterator();
        String rowPrefix = null;
        String colfPrefix = null;
        
        while (rangeIterator.hasNext()) {
            Range r = rangeIterator.next();
            String row = r.getStartKey().getRow().toString();
            String colf = r.getStartKey().getColumnFamily().toString();
            
            if (row.equals(rowPrefix) && colfPrefix != null && colf.startsWith(colfPrefix)) {
                if (log.isTraceEnabled()) {
                    log.trace("trimRangeSet() dropping by shard/datatype:" + r.toString());
                }
                // prefix match of existing shard/datatype range
                rangeIterator.remove();
            } else if (rowPrefix != null && row.startsWith(rowPrefix) && colf == null) {
                if (log.isTraceEnabled()) {
                    log.trace("trimRangeSet() dropping by day:" + r.toString());
                }
                // prefix match of existing row
                rangeIterator.remove();
            } else if (row.length() == 8) {
                if (log.isTraceEnabled()) {
                    log.trace("trimRangeSet() new day:" + r.toString());
                }
                // no match, set new day range
                rowPrefix = row;
                colfPrefix = null;
            } else if (!colf.contains(Constants.NULL_BYTE_STRING)) {
                if (log.isTraceEnabled()) {
                    log.trace("trimRangeSet() new shard/datatype:" + r.toString());
                }
                // no match, set new shard/datatype range.
                rowPrefix = row;
                colfPrefix = colf;
            } else if (log.isTraceEnabled()) {
                // keep range.
                log.trace("trimRangeSet() keeping:" + r.toString());
            }
        }
    }
    
    /**
     * Query the global index for the accumulo ranges that need to be scanned for a single term
     *
     * This method will exit under the following known conditions.:
     * <ul>
     * <li>Too many terms in query, throws TooManyTermsException</li>
     * <li>Various RuntimeExceptions</li>
     * <li>Exit with empty range due to no match in index</li>
     * <li>Exit with known good range</li>
     * <ul>
     *
     * @param node
     *            the range node to evaluate.
     * @param tableName
     *            name of table to query.
     * @param typeFilter
     *            filter on these content types
     * @return TermRange
     * @throws TableNotFoundException
     */
    protected TermRange queryGlobalIndexForTerm(DatawaveTreeNode node, String tableName, Set<String> typeFilter) throws TableNotFoundException,
                    TooManyTermsException {
        
        if (this.allTermsCount > this.maxTermExpansionThreshold) {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.TOO_MANY_TERMS);
            throw new TooManyTermsException(qe);
        }
        
        if (log.isDebugEnabled()) {
            log.debug("queryGlobalIndexForTerm: " + node.getContents());
        }
        
        String types = formatTypes(typeFilter);
        
        String startDay = this.shardDateFormatter.format(begin);
        String endDay = this.shardDateFormatter.format(end);
        
        Key startRange = new Key(new Text(node.getFieldValue()), new Text(node.getFieldName()), new Text(startDay + START_SHARD));
        Key endRange = new Key(new Text(node.getFieldValue()), new Text(node.getFieldName()), new Text(endDay));
        Range range = new Range(startRange, true, endRange, false);
        log.debug(" scanner range: " + range);
        
        // pull back the ranges in the shard table to search. Here, the
        // GlobalIndexShortCircuitIterator may roll up ranges to query an
        // entire shard or day if we exceed event or shard count thresholds.
        BatchScanner s = null;
        SortedSet<Range> rangeResults;
        long totalEventCount = 0;
        try {
            s = scannerFactory.newScanner(tableName, this.auths, this.query);
            s.setRanges(Collections.singleton(range));
            s.fetchColumnFamily(new Text(node.getFieldName()));
            
            configureGlobalIndexUidMapper(s, this.uidMapperClass);
            configureGlobalIndexDateRangeFilter(s, this.dateRange);
            configureGlobalIndexShortCircuitIterator(s, this.shardsPerDayThreshold, this.eventPerDayThreshold, types);
            
            // The GlobalIndexShortCircuitIterator should never return more than
            // one k/v pair for a given range, but we could handle it if it does.
            rangeResults = new TreeSet<>();
            
            for (Entry<Key,Value> entry : s) {
                String colq = entry.getKey().getColumnQualifier().toString();
                long eventCount = Long.parseLong(colq.substring(colq.lastIndexOf(Constants.NULL_BYTE_STRING) + 1));
                totalEventCount = safeIncrementLong(totalEventCount, eventCount);
                extractGlobalIndexShortCircuitRanges(entry.getKey(), entry.getValue(), rangeResults);
            }
        } finally {
            if (s != null)
                scannerFactory.close(s);
        }
        
        trimRangeSet(rangeResults);
        
        if (log.isDebugEnabled()) {
            log.debug("Found " + rangeResults.size() + " ranges and " + totalEventCount + " cardinality for node: " + node.getContents());
        }
        
        if (log.isTraceEnabled()) {
            log.trace("Ranges for " + node.getContents() + ": " + rangeResults.toString());
        }
        
        this.allTermsCount += 1;
        if (this.allTermsCount > this.maxTermExpansionThreshold) {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.TOO_MANY_TERMS);
            throw new TooManyTermsException(qe);
        }
        
        return new TermRange(node, rangeResults, totalEventCount);
    }
    
    /**
     * Query the global index for the accumulo ranges that need to be scanned for a range node.
     *
     * This method will exit under the following known conditions.:
     * <ul>
     * <li>Too many terms in query, throws TooManyTermsException</li>
     * <li>Incomplete range, returns null</li>
     * <li>Various RuntimeExceptions</li>
     * <li>Early exit due to term count exceeding maximum cardinality parameter for a non-required field, returns dummy range that is incomplete</lu>
     * <li>Exit due to term count exceeding range expansion, returns incomplete range</li>
     * <li>Exit due to term count exceeding range expansion for index-only field, throws RangeExpansionException</li>
     * <li>Exit with empty range due to no match in index</li>
     * <li>Exit with known good range</li>
     * <ul>
     *
     * @param node
     *            the range node to evaluate.
     * @param tableName
     *            name of table to query.
     * @param typeFilter
     *            filter on these content types
     * @param required
     *            whether this term is required by the query. In an AND, for a term in a field that is not index-only, any of the ranges may be dropped if they
     *            are problematic.
     * @param maxCardinality
     *            if a term is not required, we can abort the lookup as soon as the number of terms retrieved exceeds this number,
     * @return TermRange
     * @throws TableNotFoundException
     */
    protected TermRange queryGlobalIndexForRange(DatawaveTreeNode node, String tableName, Set<String> typeFilter, boolean required, long maxCardinality)
                    throws TableNotFoundException, RangeExpansionException, TooManyTermsException {
        if (this.allTermsCount > this.maxTermExpansionThreshold) {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.TOO_MANY_TERMS, MessageFormat.format("Threshold: {0}",
                            this.maxTermExpansionThreshold));
            throw new TooManyTermsException(qe);
        }
        
        // Validate that we have a complete range node.
        if (node.getType() != ParserTreeConstants.JJTLENODE || node.getUpperBound() == null || node.getLowerBound() == null || node.isUnboundedRange()) {
            
            if (!node.isUnboundedRange()) {
                if (log.isDebugEnabled()) {
                    log.debug("queryGlobalIndexForRange, missing piece on range, returning null: " + node);
                    log.debug("\t" + node.printRangeNode());
                    log.debug("\tnode type: " + node.getType());
                    log.debug("\tlower bound: " + node.getLowerBound());
                    log.debug("\tupper bound: " + node.getUpperBound());
                }
                return null;
            }
            // Unbounded ranges will be given an infinite start or end key accordingly.
        }
        
        String types = formatTypes(typeFilter);
        
        String startDay = this.shardDateFormatter.format(begin);
        String endDay = this.shardDateFormatter.format(end);
        
        // build the start and end range for the scanner
        // Key for global index is Row-> Normalized FieldValue, CF-> FieldName, CQ->shard_id\x00datatype
        Key startRange;
        if (null == node.getLowerBound() || node.getLowerBound().isEmpty()) { // unbounded range gets infinite start key
            startRange = null;
        } else if (node.getRangeLowerOp().equals(">=")) { // inclusive
            startRange = new Key(new Text(node.getLowerBound()), new Text(node.getFieldName()), new Text(startDay + START_SHARD));
        } else { // non-inclusive
            startRange = new Key(new Text(node.getLowerBound() + "\0"), new Text(node.getFieldName()), new Text(startDay + START_SHARD));
        }
        
        // When the end of a range is non-inclusive, we want to never include any terms for that
        // upper bound. Therefore, we create a key whose row is that upper bound, and set it to be non-inclusive
        // in the range.
        Key endRange;
        boolean endKeyInclusive;
        if (null == node.getUpperBound() || node.getUpperBound().isEmpty()) { // unbounded range gets infinite end key
            endRange = null;
            endKeyInclusive = false;
        } else if (node.getRangeUpperOp().equals("<=")) {
            endRange = new Key(new Text(node.getUpperBound()), new Text(node.getFieldName()), new Text(endDay));
            endKeyInclusive = true;
        } else {
            endRange = new Key(new Text(node.getUpperBound()));
            endKeyInclusive = false;
        }
        
        // Initially we are going to get unique terms and their aggregated count from the index.
        // This will enable us to short circuit if there are too many terms in the range without
        // having to pull all the data from the index.
        
        Map<String,Long> uniqTerms = new HashMap<>();
        int uniqTermCount = 0;
        Long totalEventCount = 0L;
        BatchScanner bs = this.scannerFactory.newScanner(tableName, this.auths, this.queryThreads, this.query);
        try {
            
            // Test that the range makes sense. Since we don't drop failed normalization terms from the query
            // it is possible to get 2 values which don't sort lexicographically. Your start key will be greater than
            // your end key which will throw an illegal argument exception when creating a range.
            // We will log that it happened, but continue gracefully.
            if (null != startRange && startRange.compareTo(endRange) <= 0) {
                
                Range range = new Range(startRange, true, endRange, endKeyInclusive);
                
                if (log.isDebugEnabled()) {
                    log.debug("queryGlobalIndexForRange, scanner range: " + range);
                }
                bs.setRanges(Collections.singleton(range));
                bs.fetchColumnFamily(new Text(node.getFieldName()));
                
                // set up the GlobalIndexDateRangeFilter
                IteratorSetting cfg = new IteratorSetting(IteratorSettingHelper.BASE_ITERATOR_PRIORITY + 21, GlobalIndexDateRangeFilter.class);
                try {
                    cfg.addOption(Constants.START_DATE, Long.toString(this.shardDateFormatter.parse(startDay).getTime()));
                    cfg.addOption(Constants.END_DATE, Long.toString(this.shardDateFormatter.parse(endDay).getTime()));
                } catch (java.text.ParseException e1) {
                    throw new RuntimeException("Unable to format day to yyyyMMdd: " + startDay + " or " + endDay);
                }
                bs.addScanIterator(cfg);
                
                // set up the GlobalIndexRangeSamplingIterator
                cfg = new IteratorSetting(IteratorSettingHelper.BASE_ITERATOR_PRIORITY + 22, GlobalIndexRangeSamplingIterator.class);
                cfg.addOption(GlobalIndexShortCircuitIterator.DATA_TYPES, types);
                bs.addScanIterator(cfg);
                
                if (node.regex()) {
                    IteratorSetting iteratorCfg = new IteratorSetting(IteratorSettingHelper.BASE_ITERATOR_PRIORITY + 28, "regexFilter",
                                    IndexRegexIterator.class);
                    iteratorCfg.addOption(IndexRegexFilter.REGEX_OPT, node.getFieldValue());
                    iteratorCfg.addOption(IndexRegexFilter.REVERSE_OPT, Boolean.toString(node.isReverseIndex()));
                    bs.addScanIterator(iteratorCfg);
                }
                
                // flag field as required if it is an unevaluated (index-only) field.
                boolean fatalExpansionException = false;
                if (this.unevaluatedFields.contains(node.getFieldName())) {
                    log.debug("Flagging unevaluated field as required");
                    required = true;
                    fatalExpansionException = true;
                }
                
                for (Entry<Key,Value> entry : bs) {
                    if (log.isDebugEnabled()) {
                        log.debug(entry.getKey());
                    }
                    String uniqueTerm = entry.getKey().getRow().toString();
                    long eventCount = Long.parseLong(new String(entry.getValue().get()));
                    
                    try {
                        totalEventCount = safeIncrementLong(totalEventCount, eventCount);
                    } catch (IllegalArgumentException ex) {
                        throw new IllegalArgumentException("Term " + uniqueTerm + " has eventCount less than zero: " + eventCount, ex);
                    }
                    
                    // short-circuit: the number of events returned has exceeded
                    // the current max cardinality, We can skip the rest of the
                    // lookups here in order to save time if this field is not
                    // required (such is typically the case with an AND). Terms
                    // appearing in OR or index only fields are required,
                    if (!required && (totalEventCount > maxCardinality)) {
                        TermRange tr = new TermRange(node, null, Long.MAX_VALUE);
                        tr.setCompleteRange(false);
                        if (log.isDebugEnabled()) {
                            log.debug("Aborting index lookup after exceeding" + " maxCardinality:[" + maxCardinality + "]" + " in non-required node: "
                                            + node.getContents() + " should fall through to a filter");
                        }
                        return tr;
                    }
                    
                    // obtaining the size of a map can be expensive, instead
                    // track the count of each unique item added.
                    if (uniqTerms.put(uniqueTerm, eventCount) == null) {
                        uniqTermCount++;
                    }
                    
                    // If this range term expands into too many terms, we stop,
                    // possibly throwing an exception
                    if (uniqTermCount > this.rangeExpansionThreshold) {
                        TermRange tr = new TermRange(node, null, totalEventCount);
                        tr.setCompleteRange(false);
                        
                        String m = "Unique term count, " + uniqTermCount + ", exceeds threshold, " + this.rangeExpansionThreshold
                                        + ", aborting global index scan.";
                        
                        if (fatalExpansionException) { // index-only field
                            throw new FatalRangeExpansionException(node, m + " offending node: " + node.getContents());
                        } else if (required) { // required field, but present in the event.
                            throw new RangeExpansionException(node, m + " offending node: " + node.getContents());
                        } else if (log.isDebugEnabled()) {
                            log.debug(m + " offending node: " + node.getContents() + " should fall through to a filter");
                        }
                        
                        return tr;
                    }
                }
            } else {
                log.warn("Start key was greater than end key for range: " + node.printRangeNode() + ", continuing...");
            }
            
        } finally {
            scannerFactory.close(bs);
        }
        
        if (log.isDebugEnabled())
            log.debug("Found " + uniqTermCount + " matching terms for range: " + uniqTerms.toString());
        
        List<String> uniqRows = new ArrayList<>(uniqTermCount);
        List<Range> ranges = new ArrayList<>();
        
        // Gather the set of all ranges for all terms
        // make sure we reverse the term values back if this was a reverse index search
        StringBuilder reverseTerm = new StringBuilder();
        for (String term : uniqTerms.keySet()) {
            // build the set of ranges for the next index table search
            Key startKey = new Key(new Text(term), new Text(node.getFieldName()), new Text(startDay + START_SHARD));
            Key endKey = new Key(new Text(term), new Text(node.getFieldName()), new Text(endDay));
            Range range = new Range(startKey, true, endKey, false);
            ranges.add(range);
            
            // reverse the term for the term set if we pulled from the reverse index above
            if (node.isReverseIndex()) {
                reverseTerm.setLength(0);
                term = reverseTerm.append(term).reverse().toString();
            }
            uniqRows.add(term);
        }
        
        SortedSet<Range> rangeResults = new TreeSet<>();
        
        // Now that we know what the terms are, pull back the ranges in the
        // shard table to search. Here, the GlobalIndexShortCircuitIterator
        // may roll up ranges to query an entire shard or day if we exceed
        // event or shard count thresholds.
        if (ranges.size() > 0) {
            
            bs = scannerFactory.newScanner(tableName, this.auths, this.queryThreads, this.query);
            try {
                log.debug("*** Looking up ranges for term in regex: " + ranges);
                bs.setRanges(ranges);
                bs.fetchColumnFamily(new Text(node.getFieldName()));
                
                configureGlobalIndexUidMapper(bs, this.uidMapperClass);
                configureGlobalIndexDateRangeFilter(bs, this.dateRange);
                configureGlobalIndexShortCircuitIterator(bs, this.shardsPerDayThreshold, this.eventPerDayThreshold, types);
                
                for (Entry<Key,Value> entry : bs) {
                    // The GlobalIndexShortCircuitIterator will return us a
                    // key and a serialized ArrayWritable<Range> in the Value.
                    extractGlobalIndexShortCircuitRanges(entry.getKey(), entry.getValue(), rangeResults);
                }
                
                // Reduce the ranges
                trimRangeSet(rangeResults);
                
            } finally {
                scannerFactory.close(bs);
            }
        }
        
        if (log.isDebugEnabled()) {
            log.debug("Found " + totalEventCount + " ranges for node: " + node.getContents());
        }
        if (log.isTraceEnabled()) {
            log.trace("Ranges for " + node.getContents() + ": " + rangeResults.toString());
        }
        
        this.allTermsCount += uniqTerms.size();
        if (this.allTermsCount > this.maxTermExpansionThreshold) {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.TOO_MANY_TERMS, MessageFormat.format("Threshold: {0}",
                            this.maxTermExpansionThreshold));
            throw new TooManyTermsException(qe);
        }
        
        return new TermRange(node, rangeResults, totalEventCount, uniqRows);
        
    }
    
    private static final DatawaveTreeNode convertRegexToRange(DatawaveTreeNode node) throws JavaRegexParseException {
        log.debug("convertRegexToRange node: " + node.getContents());
        if (node.getType() != ParserTreeConstants.JJTERNODE) {
            return null;
        }
        DatawaveTreeNode convert = new DatawaveTreeNode(ParserTreeConstants.JJTLENODE);
        convert.setFieldName(node.getFieldName());
        convert.setFieldValue(node.getFieldValue());
        convert.setRangeLowerOp(">=");
        convert.setRangeUpperOp("<=");
        
        String normalizedFieldValue = node.getFieldValue();
        JavaRegexAnalyzer regex = new JavaRegexAnalyzer(normalizedFieldValue);
        
        if (regex.hasWildCard()) {
            
            if (regex.isLeadingLiteral()) {
                // either middle or trailing wildcard, truncate teh field value at the wildcard location
                convert.setLowerBound(regex.getLeadingLiteral());
                // for upper bound, tack on the upper bound UTF character
                convert.setUpperBound(convert.getLowerBound() + Constants.MAX_UNICODE_STRING);
            } else if (regex.isTrailingLiteral()) {
                /*
                 * TODO for leading wildcards, we need to make sure the FIELDNAME is in the reverse index
                 */
                // Then we have a leading wildcard, reverse the term and use the global reverse index.
                StringBuilder buf = new StringBuilder(regex.getTrailingLiteral());
                normalizedFieldValue = buf.reverse().toString();
                log.debug("Leading wildcard, normalizedFieldValue: " + normalizedFieldValue);
                // set the upper and lower bounds
                convert.setLowerBound(normalizedFieldValue);
                convert.setUpperBound(convert.getLowerBound() + Constants.MAX_UNICODE_STRING);
                convert.setReverseIndex(true);
            } else { // must be ngram
                log.warn("Node does not contain a regex I support, treating like regular node: " + node);
                // either middle or trailing wildcard, truncate teh field value at the wildcard location
                convert.setLowerBound("");
                // for upper bound, tack on the upper bound UTF character
                convert.setUpperBound(convert.getLowerBound() + Constants.MAX_UNICODE_STRING);
            }
            
            convert.regex(true);
            
        } else {
            log.warn("Node does not contain a regex I support, treating like regular node: " + node);
            convert.setLowerBound(regex.getLeadingLiteral());
            convert.setUpperBound(convert.getLowerBound() + Constants.NULL_BYTE_STRING);
        }
        
        if (log.isDebugEnabled()) {
            log.debug("convertRegexToRange node result: " + convert.getContents());
        }
        
        return convert;
        
    }
    
    /**
     * Is the specified range complete and more selective than the current range? Used by handleAND to determine whether a node is more restrictive than its
     * siblings.
     *
     * @param range
     * @param currentRangeSet
     * @param currentCardinality
     * @return boolean of whether the passed in range is more selective than the current range.
     */
    private static final boolean isMoreSelective(TermRange range, Set<Range> currentRangeSet, long currentCardinality) {
        boolean isMoreSelective = false;
        
        if (log.isTraceEnabled()) {
            log.trace("isMoreSelective(): " + " termRange: " + range + ", currentRangeSet: " + currentRangeSet + ", currentCardinality: " + currentCardinality);
        }
        
        if (range == null) {
            // no range, can't use it.
            if (log.isDebugEnabled()) {
                log.debug("isMoreSelective(): no range to evaluate.");
            }
            isMoreSelective = false;
        } else if (!range.isCompleteRange()) {
            if (log.isDebugEnabled()) {
                log.debug("isMoreSelective(): incomplete range.");
            }
            // incomplete range, can't use it.
            isMoreSelective = false;
        } else if (currentRangeSet == null) {
            if (log.isDebugEnabled()) {
                log.debug("isMoreSelective(): no current range.");
            }
            // no current range, use the new range.
            isMoreSelective = true;
        } else if (currentCardinality > range.getCardinality()) {
            if (log.isDebugEnabled()) {
                log.debug("isMoreSelective(): smaller cardinality.");
            }
            // current range is broader than this one, use it.
            isMoreSelective = true;
        }
        
        return isMoreSelective;
    }
    
    /**
     * Given a date, truncate it to year, month, date and increment the day by one to determine the following day.
     *
     * @param endDate
     * @return
     */
    public static Date getEndDateForIndexLookup(Date endDate) {
        Date newDate = DateUtils.truncate(endDate, Calendar.DATE);
        return DateUtils.addDays(newDate, 1);
    }
    
    /**
     * Expand a single node into a series of children or'd together based on the Term ranges provided. This is where regex queries get expanded into their
     * matching strings, for example.
     *
     * @param tr
     * @param node
     */
    private static final void aliasNode(TermRange tr, DatawaveTreeNode node) {
        node.setType(ParserTreeConstants.JJTORNODE);
        node.setRangeNode(false);
        for (String fVal : tr.getAliases()) {
            node.add(new DatawaveTreeNode(ParserTreeConstants.JJTEQNODE, node.getFieldName(), fVal, false, ASTStringLiteral.class));
        }
        node.setCardinality(tr.getCardinality());
        node.setRanges(tr.getRanges());
        
        // set this to null to avoid confusion from debug output.
        node.setFieldName(null);
        node.setFieldValue(null);
    }
    
    /**
     * Safely increment a long value by a second long value so as to avoid overflows.
     *
     * @param input
     *            the number to increment.
     * @param increment
     *            the amount to increment
     * @return the sum of the number and amount to increment, unless that number would exceed the size of a long, in which case Long.MAX_VALUE is returned.
     * @throws IllegalArgumentException
     *             if the increment
     */
    private static final long safeIncrementLong(long input, long increment) {
        if (increment < 0) {
            log.warn("GlobalIndexShortCircuitIterator returned a cardinaliaty less than zero for one of the terms in the query");
            return input;
        } else if (increment == 0) {
            return input;
        }
        
        input += increment;
        if (input < increment) { // overflow
            return Long.MAX_VALUE;
        }
        
        return input;
    }
    
    /**
     * Format typeFilter strings into a string that can be passed as an option to the GlobalIndexShortCircuitIterator
     *
     * @param typeFilter
     * @return
     */
    private static final String formatTypes(Set<String> typeFilter) {
        StringBuilder types = new StringBuilder();
        if (null != typeFilter) {
            String sep = "";
            for (String type : typeFilter) {
                types.append(sep).append(type);
                sep = ",";
            }
        }
        return types.toString();
    }
    
    /**
     * Extract ranges from a value returned by the GlobalIndexShortCircuitIterator into a set of ranges.
     *
     * @param v
     *            The value to extract from.
     * @param rangeResults
     *            A set to hold the extracted ranges.
     * @throws RuntimeException
     *             if the ArrayWritable in the value can not be de-serialized.
     */
    private static final void extractGlobalIndexShortCircuitRanges(Key k, Value v, Set<Range> rangeResults) {
        // The GlobalIndexShortCircuitIterator will return a serialized
        // ArrayWritable<Range> in the Value.
        ArrayWritable ranges = new ArrayWritable(Range.class);
        try {
            ranges.readFields(new DataInputStream(new ByteArrayInputStream(v.get())));
        } catch (IOException e) {
            throw new RuntimeException("Error deserializing ArrayWritable", e);
        }
        
        // Add the ranges to the range results
        for (Writable w : ranges.get()) {
            log.debug("*** For " + k + " found range: " + w);
            rangeResults.add((Range) w);
        }
    }
    
    private static final void configureGlobalIndexUidMapper(ScannerBase bs, String uidMapperClass) {
        // Setup the GlobalIndexDateRangeFilter
        if (uidMapperClass != null) {
            IteratorSetting cfg = new IteratorSetting(IteratorSettingHelper.BASE_ITERATOR_PRIORITY + 21, GlobalIndexUidMappingIterator.class);
            cfg.addOption(GlobalIndexUidMappingIterator.UID_MAPPER, uidMapperClass);
            bs.addScanIterator(cfg);
        }
    }
    
    private static final void configureGlobalIndexDateRangeFilter(ScannerBase bs, LongRange dateRange) {
        // Setup the GlobalIndexDateRangeFilter
        IteratorSetting cfg = new IteratorSetting(IteratorSettingHelper.BASE_ITERATOR_PRIORITY + 22, GlobalIndexDateRangeFilter.class);
        cfg.addOption(Constants.START_DATE, Long.toString(dateRange.getMinimumLong()));
        cfg.addOption(Constants.END_DATE, Long.toString(dateRange.getMaximumLong()));
        bs.addScanIterator(cfg);
    }
    
    private static final void configureGlobalIndexShortCircuitIterator(ScannerBase bs, int shardsPerDayThreshold, int eventPerDayThreshold, String types) {
        IteratorSetting cfg = new IteratorSetting(IteratorSettingHelper.BASE_ITERATOR_PRIORITY + 23, GlobalIndexShortCircuitIterator.class);
        cfg.addOption(GlobalIndexShortCircuitIterator.SHARDS_PER_DAY, Integer.toString(shardsPerDayThreshold));
        cfg.addOption(GlobalIndexShortCircuitIterator.EVENTS_PER_DAY, Integer.toString(eventPerDayThreshold));
        cfg.addOption(GlobalIndexShortCircuitIterator.DATA_TYPES, types);
        bs.addScanIterator(cfg);
    }
}
