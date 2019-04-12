package datawave.query.jexl.visitors;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import datawave.core.iterators.SourcePool;
import datawave.core.iterators.ThreadLocalPooledSource;
import datawave.core.iterators.filesystem.FileSystemCache;
import datawave.core.iterators.querylock.QueryLock;
import datawave.query.Constants;
import datawave.query.attributes.ValueTuple;
import datawave.query.composite.CompositeMetadata;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.iterator.NestedIterator;
import datawave.query.jexl.functions.TermFrequencyAggregator;
import datawave.query.predicate.ChainableEventDataQueryFilter;
import datawave.query.predicate.EventDataQueryExpressionFilter;
import datawave.util.UniversalSet;
import datawave.query.iterator.SourceFactory;
import datawave.query.iterator.SourceManager;
import datawave.query.iterator.builder.AbstractIteratorBuilder;
import datawave.query.iterator.builder.AndIteratorBuilder;
import datawave.query.iterator.builder.IndexFilterIteratorBuilder;
import datawave.query.iterator.builder.IndexIteratorBuilder;
import datawave.query.iterator.builder.IndexListIteratorBuilder;
import datawave.query.iterator.builder.IndexRangeIteratorBuilder;
import datawave.query.iterator.builder.IndexRegexIteratorBuilder;
import datawave.query.iterator.builder.IteratorBuilder;
import datawave.query.iterator.builder.IvaratorBuilder;
import datawave.query.iterator.builder.NegationBuilder;
import datawave.query.iterator.builder.OrIteratorBuilder;
import datawave.query.iterator.builder.TermFrequencyIndexBuilder;
import datawave.query.iterator.profile.QuerySpanCollector;
import datawave.query.jexl.ArithmeticJexlEngines;
import datawave.query.jexl.DatawaveJexlContext;
import datawave.query.jexl.DatawaveJexlEngine;
import datawave.query.jexl.DefaultArithmetic;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlASTHelper.IdentifierOpLiteral;
import datawave.query.jexl.LiteralRange;
import datawave.query.jexl.LiteralRange.NodeOperand;
import datawave.query.jexl.functions.FieldIndexAggregator;
import datawave.query.jexl.functions.IdentityAggregator;
import datawave.query.jexl.nodes.ExceededOrThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.ExceededValueThresholdMarkerJexlNode;
import datawave.query.parser.JavaRegexAnalyzer;
import datawave.query.parser.JavaRegexAnalyzer.JavaRegexParseException;
import datawave.query.predicate.EventDataQueryFilter;
import datawave.query.predicate.Filter;
import datawave.query.predicate.TimeFilter;
import datawave.query.util.IteratorToSortedKeyValueIterator;
import datawave.query.util.TypeMetadata;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.jexl2.JexlArithmetic;
import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.Script;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTAssignment;
import org.apache.commons.jexl2.parser.ASTDelayedPredicate;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTEvaluationOnly;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTIdentifier;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ASTMethodNode;
import org.apache.commons.jexl2.parser.ASTNENode;
import org.apache.commons.jexl2.parser.ASTNRNode;
import org.apache.commons.jexl2.parser.ASTNotNode;
import org.apache.commons.jexl2.parser.ASTNumberLiteral;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTReferenceExpression;
import org.apache.commons.jexl2.parser.ASTSizeMethod;
import org.apache.commons.jexl2.parser.ASTStringLiteral;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParserTreeConstants;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.MessageFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.apache.commons.jexl2.parser.JexlNodes.children;

/**
 * A visitor that builds a tree of iterators. The main points are at ASTAndNodes and ASTOrNodes, where the code will build AndIterators and OrIterators,
 * respectively. This will automatically roll up binary representations of subtrees into a generic n-ary tree because there isn't a true mapping between JEXL
 * AST trees and iterator trees. A JEXL tree can have subtrees rooted at an ASTNotNode whereas an iterator tree cannot.
 * 
 */
public class IteratorBuildingVisitor extends BaseVisitor {
    private static final Logger log = Logger.getLogger(IteratorBuildingVisitor.class);
    
    public static final String NULL_DELIMETER = "\u0000";
    
    @SuppressWarnings("rawtypes")
    protected NestedIterator root;
    protected SourceManager source;
    protected SortedKeyValueIterator<Key,Value> limitedSource = null;
    protected Map<Entry<String,String>,Entry<Key,Value>> limitedMap = null;
    protected Collection<String> includeReferences = UniversalSet.instance();
    protected Collection<String> excludeReferences = Collections.emptyList();
    protected Predicate<Key> datatypeFilter = Predicates.<Key> alwaysTrue();
    protected TimeFilter timeFilter;
    
    protected FileSystemCache hdfsFileSystem;
    protected String hdfsFileCompressionCodec;
    protected QueryLock queryLock;
    protected List<String> ivaratorCacheDirURIs;
    protected String queryId;
    protected String scanId;
    protected String ivaratorCacheSubDirPrefix = "";
    protected long ivaratorCacheScanPersistThreshold = 100000L;
    protected long ivaratorCacheScanTimeout = 1000L * 60 * 60;
    protected int ivaratorCacheBufferSize = 10000;
    protected int maxRangeSplit = 11;
    protected int ivaratorMaxOpenFiles = 100;
    protected SourcePool ivaratorSources = null;
    protected SortedKeyValueIterator<Key,Value> ivaratorSource = null;
    protected int ivaratorCount = 0;
    
    protected TypeMetadata typeMetadata;
    protected EventDataQueryFilter attrFilter;
    protected Set<String> fieldsToAggregate = Collections.emptySet();
    protected Set<String> termFrequencyFields = Collections.emptySet();
    protected boolean allowTermFrequencyLookup = true;
    protected Set<String> indexOnlyFields = Collections.emptySet();
    protected FieldIndexAggregator fiAggregator = new IdentityAggregator(null);
    
    protected CompositeMetadata compositeMetadata;
    protected int compositeSeekThreshold = 10;
    
    protected Range rangeLimiter;
    
    // should the UIDs be sorted. If so, then ivarators will be used. Otherwise it is determined that
    // each leaf of the tree can return unsorted UIDs (i.e. no intersections are required). In this
    // case the keys will be modified to include enough context to restart at the correct place.
    protected boolean sortedUIDs = true;
    
    protected boolean limitLookup;
    
    protected Class<? extends IteratorBuilder> iteratorBuilderClass = IndexIteratorBuilder.class;
    
    private Collection<String> unindexedFields = Lists.newArrayList();
    
    protected boolean disableFiEval = false;
    
    protected boolean collectTimingDetails = false;
    
    protected QuerySpanCollector querySpanCollector = null;
    
    protected boolean limitOverride = false;
    // this is final. It will be set by the SatisfactionVisitor and cannot be
    // changed here.
    // prior code that changed its value from true to false will now log a
    // warning, so that the
    // SatisfactionVisitor can be changed to accomodate the conditions that
    // caused it.
    protected boolean isQueryFullySatisfied;
    
    /**
     * Keep track of the iterator environment since we are deep copying
     */
    protected IteratorEnvironment env;
    
    protected Set<JexlNode> delayedEqNodes = Sets.newHashSet();
    
    public boolean isQueryFullySatisfied() {
        if (limitLookup) {
            return false;
        } else
            return isQueryFullySatisfied;
    }
    
    @SuppressWarnings("unchecked")
    public <T> NestedIterator<T> root() {
        return root;
    }
    
    @Override
    public Object visit(ASTJexlScript node, Object data) {
        if (limitLookup) {
            limitedSource = source.deepCopy(env);
            limitedMap = Maps.newHashMap();
        }
        Object obj = super.visit(node, data);
        if (root == null && delayedEqNodes.size() == 1) {
            JexlNode eqNode = delayedEqNodes.iterator().next();
            obj = visit((ASTEQNode) eqNode, data);
        }
        return obj;
    }
    
    @Override
    public Object visit(ASTAndNode and, Object data) {
        
        if (ExceededOrThresholdMarkerJexlNode.instanceOf(and)) {
            if (!limitLookup) {
                
                JexlNode source = ExceededOrThresholdMarkerJexlNode.getExceededOrThresholdSource(and);
                // if the parent is our ExceededOrThreshold marker, then use an
                // Ivarator to get the job done
                if (source instanceof ASTAssignment) {
                    try {
                        ivarateList(source, data);
                    } catch (IOException ioe) {
                        throw new DatawaveFatalQueryException(ioe);
                    }
                } else {
                    QueryException qe = new QueryException(DatawaveErrorCode.UNEXPECTED_SOURCE_NODE, MessageFormat.format("{0}",
                                    "Limited ExceededOrThresholdMarkerJexlNode"));
                    throw new DatawaveFatalQueryException(qe);
                    
                }
            } else {
                QueryException qe = new QueryException(DatawaveErrorCode.UNEXPECTED_SOURCE_NODE, MessageFormat.format("{0}",
                                "Limited ExceededOrThresholdMarkerJexlNode"));
                throw new DatawaveFatalQueryException(qe);
            }
            // we should not reach this case. This is an unallowed case.
            
        } else if (null != data && data instanceof IndexRangeIteratorBuilder) {
            // index checking has already been done, otherwise we would not have
            // an "ExceededValueThresholdMarker"
            // hence the "IndexAgnostic" method can be used here
            Map<LiteralRange<?>,List<JexlNode>> ranges = JexlASTHelper.getBoundedRangesIndexAgnostic(and, null, true);
            if (ranges.size() != 1) {
                QueryException qe = new QueryException(DatawaveErrorCode.MULTIPLE_RANGES_IN_EXPRESSION);
                throw new DatawaveFatalQueryException(qe);
            }
            ((IndexRangeIteratorBuilder) data).setRange(ranges.keySet().iterator().next());
        } else if (ExceededValueThresholdMarkerJexlNode.instanceOf(and)) {
            // if the parent is our ExceededValueThreshold marker, then use an
            // Ivarator to get the job done unless we don't have to
            JexlNode source = ExceededValueThresholdMarkerJexlNode.getExceededValueThresholdSource(and);
            
            String identifier = null;
            LiteralRange<?> range = null;
            boolean negatedLocal = false;
            if (source instanceof ASTAndNode) {
                range = buildLiteralRange(source, null);
                identifier = range.getFieldName();
            } else {
                if (source instanceof ASTNRNode || source instanceof ASTNotNode)
                    negatedLocal = true;
                range = buildLiteralRange(source);
                identifier = JexlASTHelper.getIdentifier(source);
            }
            boolean negatedOverall = negatedLocal;
            if (data instanceof AbstractIteratorBuilder) {
                AbstractIteratorBuilder oib = (AbstractIteratorBuilder) data;
                if (oib.isInANot()) {
                    negatedOverall = !negatedOverall;
                }
            }
            
            // if we are not limiting the lookup, or not allowing term frequency lookup,
            // or the field is index only but not in the term frequencies, then we must ivarate
            if (!limitLookup || !allowTermFrequencyLookup || (indexOnlyFields.contains(identifier) && !termFrequencyFields.contains(identifier))) {
                if (source instanceof ASTAndNode) {
                    try {
                        if (JexlASTHelper.getFunctionNodes(source).isEmpty()) {
                            ivarateRange(source, data);
                        } else {
                            ivarateFilter(source, data);
                        }
                    } catch (IOException ioe) {
                        throw new DatawaveFatalQueryException("Unable to ivarate", ioe);
                    }
                } else if (source instanceof ASTERNode || source instanceof ASTNRNode) {
                    try {
                        ivarateRegex(source, data);
                    } catch (IOException ioe) {
                        throw new DatawaveFatalQueryException("Unable to ivarate", ioe);
                    }
                } else {
                    QueryException qe = new QueryException(DatawaveErrorCode.UNEXPECTED_SOURCE_NODE, MessageFormat.format("{0}",
                                    "ExceededValueThresholdMarkerJexlNode"));
                    throw new DatawaveFatalQueryException(qe);
                }
            } else {
                
                NestedIterator<Key> nested = null;
                if (termFrequencyFields.contains(identifier)) {
                    nested = buildExceededFromTermFrequency(identifier, source, range, data);
                } else {
                    /**
                     * This is okay since 1) We are doc specific 2) We are not index only or tf 3) Therefore, we must evaluate against the document for this
                     * expression 4) Return a stubbed range in case we have a disjunction that breaks the current doc.
                     */
                    if (!limitOverride && !negatedOverall)
                        nested = createExceededCheck(identifier, range);
                }
                
                if (null != nested && null != data && data instanceof AbstractIteratorBuilder) {
                    
                    AbstractIteratorBuilder iterators = (AbstractIteratorBuilder) data;
                    if (negatedLocal) {
                        iterators.addExclude(nested);
                    } else {
                        iterators.addInclude(nested);
                    }
                } else {
                    if (isQueryFullySatisfied == true) {
                        log.warn("Determined that isQueryFullySatisfied should be false, but it was not preset to false in the SatisfactionVisitor");
                    }
                    return nested;
                    
                }
                
            }
        } else if (null != data && data instanceof AndIteratorBuilder) {
            and.childrenAccept(this, data);
        } else {
            // Create an AndIterator and recursively add the children
            AbstractIteratorBuilder andItr = new AndIteratorBuilder();
            andItr.negateAsNeeded(data);
            and.childrenAccept(this, andItr);
            
            // If there is no parent
            if (data == null) {
                // Make this AndIterator the root node
                if (!andItr.includes().isEmpty()) {
                    root = andItr.build();
                }
            } else {
                // Otherwise, add this AndIterator to its parent
                AbstractIteratorBuilder parent = (AbstractIteratorBuilder) data;
                if (!andItr.includes().isEmpty()) {
                    parent.addInclude(andItr.build());
                }
            }
            
            if (log.isTraceEnabled()) {
                log.trace("ASTAndNode visit: pretty formatting of:\nparent.includes:" + formatIncludesOrExcludes(andItr.includes()) + "\nparent.excludes:"
                                + formatIncludesOrExcludes(andItr.excludes()));
            }
            
        }
        
        return null;
    }
    
    private LiteralRange<?> buildLiteralRange(JexlNode source) {
        if (source instanceof ASTERNode)
            return buildLiteralRange(((ASTERNode) source));
        else if (source instanceof ASTNRNode)
            return buildLiteralRange(((ASTNRNode) source));
        else
            return null;
        
    }
    
    public static LiteralRange<?> buildLiteralRange(ASTERNode node) {
        JavaRegexAnalyzer analyzer;
        try {
            analyzer = new JavaRegexAnalyzer(String.valueOf(JexlASTHelper.getLiteralValue(node)));
            
            LiteralRange<String> range = new LiteralRange<>(JexlASTHelper.getIdentifier(node), NodeOperand.AND);
            if (!analyzer.isLeadingLiteral()) {
                // if the range is a leading wildcard we have to seek over the whole range since it's forward indexed only
                range.updateLower(Constants.NULL_BYTE_STRING, true);
                range.updateUpper(Constants.MAX_UNICODE_STRING, true);
            } else {
                range.updateLower(analyzer.getLeadingLiteral(), true);
                if (analyzer.hasWildCard()) {
                    range.updateUpper(analyzer.getLeadingLiteral() + Constants.MAX_UNICODE_STRING, true);
                } else {
                    range.updateUpper(analyzer.getLeadingLiteral(), true);
                }
            }
            return range;
        } catch (JavaRegexParseException | NoSuchElementException e) {
            throw new DatawaveFatalQueryException(e);
        }
    }
    
    LiteralRange<?> buildLiteralRange(ASTNRNode node) {
        JavaRegexAnalyzer analyzer;
        try {
            analyzer = new JavaRegexAnalyzer(String.valueOf(JexlASTHelper.getLiteralValue(node)));
            
            LiteralRange<String> range = new LiteralRange<>(JexlASTHelper.getIdentifier(node), NodeOperand.AND);
            range.updateLower(analyzer.getLeadingOrTrailingLiteral(), true);
            range.updateUpper(analyzer.getLeadingOrTrailingLiteral() + Constants.MAX_UNICODE_STRING, true);
            
            return range;
        } catch (JavaRegexParseException | NoSuchElementException e) {
            throw new DatawaveFatalQueryException(e);
        }
    }
    
    /**
     *
     * @param data
     */
    private NestedIterator<Key> buildExceededFromTermFrequency(String identifier, JexlNode node, LiteralRange<?> range, Object data) {
        if (limitLookup) {
            
            TermFrequencyIndexBuilder builder = new TermFrequencyIndexBuilder();
            builder.setSource(source.deepCopy(env));
            builder.setTypeMetadata(typeMetadata);
            builder.setFieldsToAggregate(fieldsToAggregate);
            builder.setTimeFilter(timeFilter);
            builder.setAttrFilter(attrFilter);
            builder.setDatatypeFilter(datatypeFilter);
            builder.setTermFrequencyAggregator(getTermFrequencyAggregator(node, attrFilter, attrFilter != null ? attrFilter.getMaxNextCount() : -1));
            
            Range fiRange = getFiRangeForTF(range);
            
            builder.setRange(fiRange);
            builder.setField(identifier);
            
            return builder.build();
        } else {
            QueryException qe = new QueryException(DatawaveErrorCode.UNEXPECTED_SOURCE_NODE, MessageFormat.format("{0}", "buildExceededFromTermFrequency"));
            throw new DatawaveFatalQueryException(qe);
        }
        
    }
    
    /**
     * Range should be built from the start key of the rangeLimiter only, as the end key likely doesn't parse properly so don't rely on it. rangeLimiter must be
     * non-null (limitLookup == true)
     *
     * @param range
     *            non-null literal range to generate an FI range from
     * @return non-null FI based range for the literal range provided
     */
    protected Range getFiRangeForTF(LiteralRange<?> range) {
        Key startKey = rangeLimiter.getStartKey();
        
        StringBuilder strBuilder = new StringBuilder("fi");
        strBuilder.append(NULL_DELIMETER).append(range.getFieldName());
        Text cf = new Text(strBuilder.toString());
        
        strBuilder = new StringBuilder(range.getLower().toString());
        
        strBuilder.append(NULL_DELIMETER).append(startKey.getColumnFamily());
        Text cq = new Text(strBuilder.toString());
        
        Key seekBeginKey = new Key(startKey.getRow(), cf, cq, startKey.getTimestamp());
        
        strBuilder = new StringBuilder(range.getUpper().toString());
        
        strBuilder.append(NULL_DELIMETER).append(startKey.getColumnFamily());
        cq = new Text(strBuilder.toString());
        
        Key seekEndKey = new Key(startKey.getRow(), cf, cq, startKey.getTimestamp());
        
        return new Range(seekBeginKey, true, seekEndKey, true);
    }
    
    @Override
    public Object visit(ASTOrNode or, Object data) {
        if (null != data && data instanceof OrIteratorBuilder) {
            or.childrenAccept(this, data);
        } else {
            // Create an OrIterator and recursively add the children
            AbstractIteratorBuilder orItr = new OrIteratorBuilder();
            orItr.setSortedUIDs(sortedUIDs);
            orItr.negateAsNeeded(data);
            or.childrenAccept(this, orItr);
            
            // If there is no parent
            if (data == null) {
                // Make this OrIterator the root node
                if (!orItr.includes().isEmpty()) {
                    root = orItr.build();
                }
            } else {
                // Otherwise, add this OrIterator to its parent
                AbstractIteratorBuilder parent = (AbstractIteratorBuilder) data;
                if (!orItr.includes().isEmpty()) {
                    parent.addInclude(orItr.build());
                }
                if (log.isTraceEnabled()) {
                    log.trace("ASTOrNode visit: pretty formatting of:\nparent.includes:" + formatIncludesOrExcludes(orItr.includes()) + "\nparent.excludes:"
                                    + formatIncludesOrExcludes(orItr.excludes()));
                }
            }
        }
        
        return null;
    }
    
    @Override
    public Object visit(ASTNotNode not, Object data) {
        // We have no parent
        if (root == null && data == null) {
            // We don't support querying only on a negation
            throw new IllegalStateException("Root node cannot be a negation!");
        }
        
        NegationBuilder stub = new NegationBuilder();
        stub.negateAsNeeded(data);
        
        // Add all of the children to this negation
        not.childrenAccept(this, stub);
        
        // Then add the children to the parent's children
        AbstractIteratorBuilder parent = (AbstractIteratorBuilder) data;
        /*
         * because we're in a negation, the includes become excludes to the parent. conversely, the excludes in the child tree become includes to the parent.
         */
        parent.excludes().addAll(stub.includes());
        parent.includes().addAll(stub.excludes());
        
        if (log.isTraceEnabled()) {
            log.trace("pretty formatting of:\nparent.includes:" + formatIncludesOrExcludes(parent.includes()) + "\nparent.excludes:"
                            + formatIncludesOrExcludes(parent.excludes()));
        }
        return null;
    }
    
    private String formatIncludesOrExcludes(List<NestedIterator> in) {
        String builder = in.toString();
        builder = builder.replaceAll("OrIterator:", "\n\tOrIterator:");
        builder = builder.replaceAll("Includes:", "\n\t\tIncludes:");
        builder = builder.replaceAll("Excludes:", "\n\t\tExcludes:");
        builder = builder.replaceAll("Bridge:", "\n\t\t\tBridge:");
        return builder.toString();
    }
    
    @Override
    public Object visit(ASTNENode node, Object data) {
        // We have no parent already defined
        if (data == null) {
            // We don't support querying only on a negation
            throw new IllegalStateException("Root node cannot be a negation");
        }
        IndexIteratorBuilder builder = null;
        try {
            builder = iteratorBuilderClass.asSubclass(IndexIteratorBuilder.class).newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        builder.setSource(source.deepCopy(env));
        builder.setTypeMetadata(typeMetadata);
        builder.setFieldsToAggregate(fieldsToAggregate);
        builder.setTimeFilter(timeFilter);
        builder.setDatatypeFilter(datatypeFilter);
        builder.setKeyTransform(fiAggregator);
        node.childrenAccept(this, builder);
        
        // A EQNode may be of the form FIELD == null. The evaluation can
        // handle this, so we should just not build an IndexIterator for it.
        if (null == builder.getValue()) {
            
            // SatisfactionVisitor should have already initialized this to false
            if (isQueryFullySatisfied == true) {
                log.warn("Determined that isQueryFullySatisfied should be false, but it was not preset to false in the SatisfactionVisitor");
            }
            return null;
        }
        
        AbstractIteratorBuilder iterators = (AbstractIteratorBuilder) data;
        // Add the negated IndexIteratorBuilder to the parent as an *exclude*
        if (!iterators.hasSeen(builder.getField(), builder.getValue()) && includeReferences.contains(builder.getField())
                        && !excludeReferences.contains(builder.getField())) {
            iterators.addExclude(builder.build());
        } else {
            // SatisfactionVisitor should have already initialized this to false
            if (isQueryFullySatisfied == true) {
                log.warn("Determined that isQueryFullySatisfied should be false, but it was not preset to false in the SatisfactionVisitor");
            }
        }
        
        return null;
    }
    
    @Override
    public Object visit(ASTMethodNode node, Object data) {
        // if i find a method node then i can't build an index for the identifier it is called on
        return null;
    }
    
    @Override
    public Object visit(ASTSizeMethod node, Object data) {
        // if i find a size method node then i can't build an index for the identifier it is called on
        return null;
    }
    
    @Override
    public Object visit(ASTEQNode node, Object data) {
        IndexIteratorBuilder builder = null;
        try {
            builder = iteratorBuilderClass.asSubclass(IndexIteratorBuilder.class).newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        
        /**
         * If we have an unindexed type enforced, we've been configured to assert whether the field is indexed.
         */
        if (isUnindexed(node)) {
            if (isQueryFullySatisfied == true) {
                log.warn("Determined that isQueryFullySatisfied should be false, but it was not preset to false in the SatisfactionVisitor");
            }
            return null;
        }
        // boolean to tell us if we've overridden our subtree due to
        // a negation or
        boolean isNegation = false;
        if (data instanceof AbstractIteratorBuilder) {
            AbstractIteratorBuilder oib = (AbstractIteratorBuilder) data;
            isNegation = oib.isInANot();
        }
        builder.setSource(getSourceIterator(node, isNegation));
        builder.setTimeFilter(getTimeFilter(node));
        builder.setTypeMetadata(typeMetadata);
        builder.setFieldsToAggregate(fieldsToAggregate);
        builder.setDatatypeFilter(datatypeFilter);
        builder.setKeyTransform(fiAggregator);
        builder.forceDocumentBuild(!limitLookup && this.isQueryFullySatisfied);
        node.childrenAccept(this, builder);
        
        // A EQNode may be of the form FIELD == null. The evaluation can
        // handle this, so we should just not build an IndexIterator for it.
        if (null == builder.getValue()) {
            return null;
        }
        
        // We have no parent already defined
        if (data == null) {
            // Make this EQNode the root
            if (!includeReferences.contains(builder.getField()) && excludeReferences.contains(builder.getField())) {
                throw new IllegalStateException(builder.getField() + " is a blacklisted reference.");
            } else if (builder.getField() != null) {
                root = builder.build();
                
                if (log.isTraceEnabled()) {
                    log.trace("Build IndexIterator: " + root);
                }
            }
        } else {
            AbstractIteratorBuilder iterators = (AbstractIteratorBuilder) data;
            // Add this IndexIterator to the parent
            final boolean isNew = !iterators.hasSeen(builder.getField(), builder.getValue());
            final boolean inclusionReference = includeReferences.contains(builder.getField());
            final boolean notExcluded = !excludeReferences.contains(builder.getField());
            
            if (isNew && inclusionReference && notExcluded) {
                iterators.addInclude(builder.build());
            } else {
                if (isQueryFullySatisfied == true) {
                    log.warn("Determined that isQueryFullySatisfied should be false, but it was not preset to false in the SatisfactionVisitor");
                }
            }
        }
        
        return null;
    }
    
    protected TimeFilter getTimeFilter(ASTEQNode node) {
        final String identifier = JexlASTHelper.getIdentifier(node);
        if (limitLookup && !limitOverride && !fieldsToAggregate.contains(identifier)) {
            return TimeFilter.alwaysTrue();
        }
        
        return timeFilter;
        
    }
    
    protected SortedKeyValueIterator<Key,Value> getSourceIterator(final ASTEQNode node, boolean negation) {
        
        SortedKeyValueIterator<Key,Value> kvIter = null;
        String identifier = JexlASTHelper.getIdentifier(node);
        try {
            if (limitLookup && !negation) {
                
                if (!disableFiEval && fieldsToAggregate.contains(identifier)) {
                    kvIter = source.deepCopy(env);
                    seekIndexOnlyDocument(kvIter, node);
                } else if (disableFiEval && fieldsToAggregate.contains(identifier)) {
                    kvIter = createIndexOnlyKey(node);
                } else if (limitOverride) {
                    kvIter = createIndexOnlyKey(node);
                } else {
                    kvIter = new IteratorToSortedKeyValueIterator(getNodeEntry(node).iterator());
                }
                
            } else {
                kvIter = source.deepCopy(env);
                seekIndexOnlyDocument(kvIter, node);
            }
            
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        
        return kvIter;
    }
    
    protected SortedKeyValueIterator<Key,Value> createIndexOnlyKey(ASTEQNode node) throws IOException {
        Key newStartKey = getKey(node);
        
        IdentifierOpLiteral op = JexlASTHelper.getIdentifierOpLiteral(node);
        if (null == op || null == op.getLiteralValue()) {
            // deep copy since this is likely a null literal
            return source.deepCopy(env);
        }
        
        String fn = op.deconstructIdentifier();
        String literal = String.valueOf(op.getLiteralValue());
        
        if (log.isTraceEnabled()) {
            log.trace("createIndexOnlyKey for " + fn + " " + literal + " " + newStartKey);
        }
        List<Entry<Key,Value>> kv = Lists.newArrayList();
        if (null != limitedMap.get(Maps.immutableEntry(fn, literal))) {
            kv.add(limitedMap.get(Maps.immutableEntry(fn, literal)));
        } else {
            
            SortedKeyValueIterator<Key,Value> mySource = limitedSource;
            // if source size > 0, we are free to use up to that number for this query
            if (source.getSourceSize() > 0)
                mySource = source.deepCopy(env);
            
            mySource.seek(new Range(newStartKey, true, newStartKey.followingKey(PartialKey.ROW_COLFAM_COLQUAL), false), Collections.emptyList(), false);
            
            if (mySource.hasTop()) {
                kv.add(Maps.immutableEntry(mySource.getTopKey(), Constants.NULL_VALUE));
                
            }
        }
        
        return new IteratorToSortedKeyValueIterator(kv.iterator());
    }
    
    /**
     * @param kvIter
     * @param node
     * @throws IOException
     */
    protected void seekIndexOnlyDocument(SortedKeyValueIterator<Key,Value> kvIter, ASTEQNode node) throws IOException {
        if (null != rangeLimiter && limitLookup) {
            
            Key newStartKey = getKey(node);
            
            kvIter.seek(new Range(newStartKey, true, newStartKey.followingKey(PartialKey.ROW_COLFAM_COLQUAL), false), Collections.emptyList(), false);
            
        }
    }
    
    /**
     * @param node
     * @return
     */
    protected Collection<Entry<Key,Value>> getNodeEntry(ASTEQNode node) {
        Key key = getKey(node);
        return Collections.singleton(Maps.immutableEntry(key, Constants.NULL_VALUE));
        
    }
    
    /**
     * @param identifier
     * @param range
     * @return
     */
    protected Collection<Entry<Key,Value>> getExceededEntry(String identifier, LiteralRange<?> range) {
        
        Key key = getIvaratorKey(identifier, range);
        return Collections.singleton(Maps.immutableEntry(key, Constants.NULL_VALUE));
        
    }
    
    protected Key getIvaratorKey(String identifier, LiteralRange<?> range) {
        
        Key startKey = rangeLimiter.getStartKey();
        
        Object objValue = range.getLower();
        String value = null == objValue ? "null" : objValue.toString();
        
        StringBuilder builder = new StringBuilder("fi");
        builder.append(NULL_DELIMETER).append(identifier);
        Text cf = new Text(builder.toString());
        
        builder = new StringBuilder(value);
        
        builder.append(NULL_DELIMETER).append(startKey.getColumnFamily());
        Text cq = new Text(builder.toString());
        
        return new Key(startKey.getRow(), cf, cq, startKey.getTimestamp());
    }
    
    protected Key getKey(JexlNode node) {
        Key startKey = rangeLimiter.getStartKey();
        String identifier = JexlASTHelper.getIdentifier(node);
        Object objValue = JexlASTHelper.getLiteralValue(node);
        String value = null == objValue ? "null" : objValue.toString();
        
        StringBuilder builder = new StringBuilder("fi");
        builder.append(NULL_DELIMETER).append(identifier);
        Text cf = new Text(builder.toString());
        
        builder = new StringBuilder(value);
        
        builder.append(NULL_DELIMETER).append(startKey.getColumnFamily());
        Text cq = new Text(builder.toString());
        
        return new Key(startKey.getRow(), cf, cq, startKey.getTimestamp());
    }
    
    @Override
    public Object visit(ASTReference node, Object o) {
        // Recurse only if not delayed or evaluation only
        if (!ASTDelayedPredicate.instanceOf(node) && !ASTEvaluationOnly.instanceOf(node)) {
            super.visit(node, o);
        } else if (ASTDelayedPredicate.instanceOf(node)) {
            JexlNode subNode = ASTDelayedPredicate.getQueryPropertySource(node, ASTDelayedPredicate.class);
            if (subNode instanceof ASTEQNode) {
                delayedEqNodes.add(subNode);
            }
            if (isQueryFullySatisfied == true) {
                log.warn("Determined that isQueryFullySatisfied should be false, but it was not preset to false in the SatisfactionVisitor");
            }
            log.warn("Will not process ASTDelayedPredicate.");
        }
        
        return null;
    }
    
    /**
     * This method should only be used when we know it is not a term frequency or index only in the limited case, as we will subsequently evaluate this
     * expression during final evaluation
     * 
     * @param identifier
     * @param range
     * @return
     */
    protected NestedIterator<Key> createExceededCheck(String identifier, LiteralRange<?> range) {
        IndexIteratorBuilder builder = null;
        try {
            builder = iteratorBuilderClass.asSubclass(IndexIteratorBuilder.class).newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        
        IteratorToSortedKeyValueIterator kvIter = new IteratorToSortedKeyValueIterator(getExceededEntry(identifier, range).iterator());
        builder.setSource(kvIter);
        builder.setValue(null != range.getLower() ? range.getLower().toString() : "null");
        builder.setField(identifier);
        builder.setTimeFilter(TimeFilter.alwaysTrue());
        builder.setTypeMetadata(typeMetadata);
        builder.setFieldsToAggregate(fieldsToAggregate);
        builder.setDatatypeFilter(datatypeFilter);
        builder.setKeyTransform(fiAggregator);
        
        return builder.build();
    }
    
    protected Object visitDelayedIndexOnly(ASTEQNode node, Object data) {
        IndexIteratorBuilder builder = null;
        try {
            builder = iteratorBuilderClass.asSubclass(IndexIteratorBuilder.class).newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        
        /**
         * If we have an unindexed type enforced, we've been configured to assert whether the field is indexed.
         */
        if (isUnindexed(node)) {
            if (isQueryFullySatisfied == true) {
                log.warn("Determined that isQueryFullySatisfied should be false, but it was not preset to false in the SatisfactionVisitor");
            }
            return null;
        }
        
        // boolean to tell us if we've overridden our subtree due to
        // a negation or
        boolean isNegation = (null != data && data instanceof AbstractIteratorBuilder && ((AbstractIteratorBuilder) data).isInANot());
        builder.setSource(getSourceIterator(node, isNegation));
        
        builder.setTimeFilter(getTimeFilter(node));
        builder.setTypeMetadata(typeMetadata);
        builder.setFieldsToAggregate(fieldsToAggregate);
        builder.setDatatypeFilter(datatypeFilter);
        builder.setKeyTransform(fiAggregator);
        
        node.childrenAccept(this, builder);
        
        // A EQNode may be of the form FIELD == null. The evaluation can
        // handle this, so we should just not build an IndexIterator for it.
        if (null == builder.getValue()) {
            if (isQueryFullySatisfied == true) {
                log.warn("Determined that isQueryFullySatisfied should be false, but it was not preset to false in the SatisfactionVisitor");
            }
            return null;
        }
        
        // We have no parent already defined
        if (data == null) {
            // Make this EQNode the root
            if (!includeReferences.contains(builder.getField()) && excludeReferences.contains(builder.getField())) {
                throw new IllegalStateException(builder.getField() + " is a blacklisted reference.");
            } else {
                root = builder.build();
                
                if (log.isTraceEnabled()) {
                    log.trace("Build IndexIterator: " + root);
                }
            }
        } else {
            AbstractIteratorBuilder iterators = (AbstractIteratorBuilder) data;
            // Add this IndexIterator to the parent
            final boolean isNew = !iterators.hasSeen(builder.getField(), builder.getValue());
            final boolean inclusionReference = includeReferences.contains(builder.getField());
            final boolean notExcluded = !excludeReferences.contains(builder.getField());
            if (isNew && inclusionReference && notExcluded) {
                iterators.addInclude(builder.build());
            } else {
                if (isQueryFullySatisfied == true) {
                    log.warn("Determined that isQueryFullySatisfied should be false, but it was not preset to false in the SatisfactionVisitor");
                }
            }
        }
        
        return null;
    }
    
    @Override
    public Object visit(ASTReferenceExpression node, Object o) {
        // Recurse
        node.jjtGetChild(0).jjtAccept(this, o);
        
        return null;
    }
    
    @Override
    public Object visit(ASTIdentifier node, Object o) {
        // Set the literal in the IndexIterator
        if (o instanceof IndexIteratorBuilder) {
            IndexIteratorBuilder builder = (IndexIteratorBuilder) o;
            builder.setField(JexlASTHelper.deconstructIdentifier(node.image));
        }
        
        if (isUnindexed(node)) {
            if (isQueryFullySatisfied == true) {
                log.warn("Determined that isQueryFullySatisfied should be false, but it was not preset to false in the SatisfactionVisitor");
            }
        }
        
        return null;
    }
    
    @Override
    public Object visit(ASTStringLiteral node, Object o) {
        // Set the literal in the IndexIterator
        AbstractIteratorBuilder builder = (AbstractIteratorBuilder) o;
        builder.setValue(node.image);
        
        return null;
    }
    
    @Override
    public Object visit(ASTNumberLiteral node, Object o) {
        // Set the literal in the IndexIterator
        AbstractIteratorBuilder builder = (AbstractIteratorBuilder) o;
        builder.setValue(node.image);
        
        return null;
    }
    
    private boolean isUsable(Path path) throws IOException {
        try {
            if (!hdfsFileSystem.getFileSystem(path.toUri()).mkdirs(path)) {
                throw new IOException("Unable to mkdirs: fs.mkdirs(" + path + ")->false");
            }
        } catch (MalformedURLException e) {
            throw new IOException("Unable to load hadoop configuration", e);
        } catch (Exception e) {
            log.warn("Unable to access " + path, e);
            return false;
        }
        return true;
    }
    
    /**
     * Create a cache directory path for a specified regex node. If alternatives have been specified, then random alternatives will be attempted until one is
     * found that can be written to.
     * 
     * @return A path
     */
    private URI getTemporaryCacheDir() throws IOException {
        // first lets increment the count for a unique subdirectory
        String subdirectory = ivaratorCacheSubDirPrefix + "term" + Integer.toString(++ivaratorCount);
        
        if (ivaratorCacheDirURIs != null && !ivaratorCacheDirURIs.isEmpty()) {
            for (int i = 0; i < ivaratorCacheDirURIs.size(); i++) {
                String hdfsCacheDirURI = ivaratorCacheDirURIs.get(i);
                Path path = new Path(hdfsCacheDirURI, queryId);
                if (scanId == null) {
                    log.warn("Running query iterator for " + queryId + " without a scan id.  This could cause ivarator directory conflicts.");
                } else {
                    path = new Path(path, scanId);
                }
                path = new Path(path, subdirectory);
                if (isUsable(path)) {
                    return path.toUri();
                }
            }
        }
        throw new IOException("Unable to find a usable hdfs cache dir out of " + ivaratorCacheDirURIs);
    }
    
    /**
     * Build the iterator stack using the regex ivarator (field index caching regex iterator)
     * 
     * @param source
     * @param data
     */
    public void ivarateRegex(JexlNode source, Object data) throws IOException {
        IndexRegexIteratorBuilder builder = new IndexRegexIteratorBuilder();
        if (source instanceof ASTERNode || source instanceof ASTNRNode) {
            builder.setNegated(source instanceof ASTNRNode);
            builder.setField(JexlASTHelper.getIdentifier(source));
            builder.setValue(String.valueOf(JexlASTHelper.getLiteralValue(source)));
        } else {
            QueryException qe = new QueryException(DatawaveErrorCode.UNEXPECTED_SOURCE_NODE,
                            MessageFormat.format("{0}", "ExceededValueThresholdMarkerJexlNode"));
            throw new DatawaveFatalQueryException(qe);
        }
        builder.negateAsNeeded(data);
        builder.forceDocumentBuild(!limitLookup && this.isQueryFullySatisfied);
        ivarate(builder, source, data);
    }
    
    /**
     * Build the iterator stack using the regex ivarator (field index caching regex iterator)
     * 
     * @param source
     * @param data
     */
    public void ivarateList(JexlNode source, Object data) throws IOException {
        IvaratorBuilder builder = null;
        
        try {
            ExceededOrThresholdMarkerJexlNode.ExceededOrParams params = ExceededOrThresholdMarkerJexlNode.getParameters(source);
            
            String fieldName = params.getField();
            
            if (params.getRanges() != null && !params.getRanges().isEmpty()) {
                IndexRangeIteratorBuilder rangeIterBuilder = new IndexRangeIteratorBuilder();
                builder = rangeIterBuilder;
                
                SortedSet<Range> ranges = params.getSortedAccumuloRanges();
                rangeIterBuilder.setSubRanges(params.getSortedAccumuloRanges());
                
                LiteralRange<?> fullRange = new LiteralRange<>(String.valueOf(ranges.first().getStartKey().getRow()), ranges.first().isStartKeyInclusive(),
                                String.valueOf(ranges.last().getEndKey().getRow()), ranges.last().isEndKeyInclusive(), fieldName, NodeOperand.AND);
                
                rangeIterBuilder.setRange(fullRange);
            } else {
                IndexListIteratorBuilder listIterBuilder = new IndexListIteratorBuilder();
                builder = listIterBuilder;
                
                if (params.getValues() != null && !params.getValues().isEmpty()) {
                    listIterBuilder.setValues(new TreeSet<>(params.getValues()));
                }
                if (params.getFstURI() != null) {
                    listIterBuilder.setFstURI(new URI(params.getFstURI()));
                    listIterBuilder.setFstHdfsFileSystem(hdfsFileSystem.getFileSystem(listIterBuilder.getFstURI()));
                }
                
                // If this is actually negated, then this will be added to excludes. Do not negate in the ivarator
                listIterBuilder.setNegated(false);
            }
            
            builder.setField(fieldName);
        } catch (IOException | URISyntaxException | NullPointerException e) {
            QueryException qe = new QueryException(DatawaveErrorCode.UNPARSEABLE_EXCEEDED_OR_PARAMS, MessageFormat.format("Class: {0}",
                            ExceededOrThresholdMarkerJexlNode.class.getSimpleName()));
            throw new DatawaveFatalQueryException(qe);
        }
        
        builder.negateAsNeeded(data);
        builder.forceDocumentBuild(!limitLookup && this.isQueryFullySatisfied);
        
        ivarate(builder, source, data);
    }
    
    /**
     * Build the iterator stack using the regex ivarator (field index caching regex iterator)
     * 
     * @param source
     * @param data
     * @return
     */
    public LiteralRange<?> buildLiteralRange(JexlNode source, Object data) {
        // index checking has already been done, otherwise we would not have an
        // "ExceededValueThresholdMarker"
        // hence the "IndexAgnostic" method can be used here
        if (source instanceof ASTAndNode) {
            Map<LiteralRange<?>,List<JexlNode>> ranges = JexlASTHelper.getBoundedRangesIndexAgnostic((ASTAndNode) source, null, true);
            if (ranges.size() != 1) {
                QueryException qe = new QueryException(DatawaveErrorCode.MULTIPLE_RANGES_IN_EXPRESSION);
                throw new DatawaveFatalQueryException(qe);
            }
            return ranges.keySet().iterator().next();
        } else {
            QueryException qe = new QueryException(DatawaveErrorCode.UNEXPECTED_SOURCE_NODE,
                            MessageFormat.format("{0}", "ExceededValueThresholdMarkerJexlNode"));
            throw new DatawaveFatalQueryException(qe);
        }
    }
    
    /**
     * Build the iterator stack using the regex ivarator (field index caching regex iterator)
     * 
     * @param source
     * @param data
     */
    public void ivarateRange(JexlNode source, Object data) throws IOException {
        IndexRangeIteratorBuilder builder = new IndexRangeIteratorBuilder();
        builder.negateAsNeeded(data);
        // index checking has already been done, otherwise we would not have an
        // "ExceededValueThresholdMarker"
        // hence the "IndexAgnostic" method can be used here
        if (source instanceof ASTAndNode) {
            Map<LiteralRange<?>,List<JexlNode>> ranges = JexlASTHelper.getBoundedRangesIndexAgnostic((ASTAndNode) source, null, true);
            if (ranges.size() != 1) {
                QueryException qe = new QueryException(DatawaveErrorCode.MULTIPLE_RANGES_IN_EXPRESSION);
                throw new DatawaveFatalQueryException(qe);
            }
            builder.setRange(ranges.keySet().iterator().next());
        } else {
            QueryException qe = new QueryException(DatawaveErrorCode.UNEXPECTED_SOURCE_NODE,
                            MessageFormat.format("{0}", "ExceededValueThresholdMarkerJexlNode"));
            throw new DatawaveFatalQueryException(qe);
        }
        builder.forceDocumentBuild(!limitLookup && this.isQueryFullySatisfied);
        ivarate(builder, source, data);
    }
    
    /**
     * Build the iterator stack using the filter ivarator (field index caching filter iterator)
     *
     * @param source
     * @param data
     */
    public void ivarateFilter(JexlNode source, Object data) throws IOException {
        IndexFilterIteratorBuilder builder = new IndexFilterIteratorBuilder();
        builder.negateAsNeeded(data);
        // index checking has already been done, otherwise we would not have an
        // "ExceededValueThresholdMarker"
        // hence the "IndexAgnostic" method can be used here
        if (source instanceof ASTAndNode) {
            Map<LiteralRange<?>,List<JexlNode>> ranges = JexlASTHelper.getBoundedRangesIndexAgnostic((ASTAndNode) source, null, true);
            if (ranges.size() != 1) {
                QueryException qe = new QueryException(DatawaveErrorCode.MULTIPLE_RANGES_IN_EXPRESSION);
                throw new DatawaveFatalQueryException(qe);
            }
            List<ASTFunctionNode> functions = JexlASTHelper.getFunctionNodes(source);
            builder.setRangeAndFunction(ranges.keySet().iterator().next(), new FunctionFilter(functions));
        } else {
            QueryException qe = new QueryException(DatawaveErrorCode.UNEXPECTED_SOURCE_NODE, MessageFormat.format("{0}", "ASTFunctionNode"));
            throw new DatawaveFatalQueryException(qe);
        }
        ivarate(builder, source, data);
    }
    
    protected TermFrequencyAggregator getTermFrequencyAggregator(JexlNode node, EventDataQueryFilter attrFilter, int maxNextCount) {
        ChainableEventDataQueryFilter wrapped = createWrappedTermFrequencyFilter(node, attrFilter);
        
        return buildTermFrequencyAggregator(wrapped, maxNextCount);
    }
    
    protected TermFrequencyAggregator buildTermFrequencyAggregator(ChainableEventDataQueryFilter filter, int maxNextCount) {
        return new TermFrequencyAggregator(indexOnlyFields, filter, maxNextCount);
    }
    
    protected ChainableEventDataQueryFilter createWrappedTermFrequencyFilter(JexlNode node, EventDataQueryFilter existing) {
        // combine index only and term frequency to create non-event fields
        final Set<String> nonEventFields = new HashSet<>(indexOnlyFields.size() + termFrequencyFields.size());
        nonEventFields.addAll(indexOnlyFields);
        nonEventFields.addAll(termFrequencyFields);
        
        EventDataQueryFilter expressionFilter = new EventDataQueryExpressionFilter(node, typeMetadata, nonEventFields);
        
        ChainableEventDataQueryFilter chainableFilter = new ChainableEventDataQueryFilter();
        if (existing != null) {
            chainableFilter.addFilter(existing);
        }
        
        chainableFilter.addFilter(expressionFilter);
        
        return chainableFilter;
    }
    
    public static class FunctionFilter implements Filter {
        private Script script;
        
        public FunctionFilter(List<ASTFunctionNode> nodes) {
            ASTJexlScript script = new ASTJexlScript(ParserTreeConstants.JJTJEXLSCRIPT);
            if (nodes.size() > 1) {
                ASTAndNode andNode = new ASTAndNode(ParserTreeConstants.JJTANDNODE);
                children(script, andNode);
                children(andNode, nodes.toArray(new JexlNode[nodes.size()]));
            } else {
                children(script, nodes.get(0));
            }
            
            String query = JexlStringBuildingVisitor.buildQuery(script);
            JexlArithmetic arithmetic = new DefaultArithmetic();
            
            // Get a JexlEngine initialized with the correct JexlArithmetic for
            // this Document
            DatawaveJexlEngine engine = ArithmeticJexlEngines.getEngine(arithmetic);
            
            // Evaluate the JexlContext against the Script
            this.script = engine.createScript(query);
        }
        
        @Override
        public boolean keep(Key k) {
            // fieldname is after fi\0
            String fieldName = k.getColumnFamily().toString().substring(3);
            
            // fieldvalue is first portion of cq
            String fieldValue = k.getColumnQualifier().toString();
            // pull off datatype and uid
            int index = fieldValue.lastIndexOf('\0');
            index = fieldValue.lastIndexOf('\0', index - 1);
            fieldValue = fieldValue.substring(0, index);
            
            // create a jexl context with this valud
            JexlContext context = new DatawaveJexlContext();
            context.set(fieldName, new ValueTuple(fieldName, fieldValue, fieldValue, null));
            
            boolean matched = false;
            
            Object o = script.execute(context);
            
            // Jexl might return us a null depending on the AST
            if (o != null && Boolean.class.isAssignableFrom(o.getClass())) {
                matched = (Boolean) o;
            } else if (o != null && Collection.class.isAssignableFrom(o.getClass())) {
                // if the function returns a collection of matches, return
                // true/false
                // based on the number of matches
                Collection<?> matches = (Collection<?>) o;
                matched = (!matches.isEmpty());
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("Unable to process non-Boolean result from JEXL evaluation '" + o + "' for function query");
                }
            }
            return matched;
        }
    }
    
    /**
     * Set up a builder for an ivarator
     * 
     * @param builder
     * @param node
     * @param data
     */
    public void ivarate(IvaratorBuilder builder, JexlNode node, Object data) throws IOException {
        builder.setSource(ivaratorSource);
        builder.setTimeFilter(timeFilter);
        builder.setTypeMetadata(typeMetadata);
        builder.setCompositeMetadata(compositeMetadata);
        builder.setCompositeSeekThreshold(compositeSeekThreshold);
        builder.setFieldsToAggregate(fieldsToAggregate);
        builder.setDatatypeFilter(datatypeFilter);
        builder.setKeyTransform(fiAggregator);
        URI path = getTemporaryCacheDir();
        builder.setHdfsFileSystem(hdfsFileSystem.getFileSystem(path));
        builder.setHdfsFileCompressionCodec(hdfsFileCompressionCodec);
        builder.setQueryLock(queryLock);
        builder.setIvaratorCacheDirURI(path.toString());
        builder.setIvaratorCacheBufferSize(ivaratorCacheBufferSize);
        builder.setIvaratorCacheScanPersistThreshold(ivaratorCacheScanPersistThreshold);
        builder.setIvaratorCacheScanTimeout(ivaratorCacheScanTimeout);
        builder.setMaxRangeSplit(maxRangeSplit);
        builder.setIvaratorMaxOpenFiles(ivaratorMaxOpenFiles);
        builder.setCollectTimingDetails(collectTimingDetails);
        builder.setQuerySpanCollector(querySpanCollector);
        builder.setSortedUIDs(sortedUIDs);
        
        // We have no parent already defined
        if (data == null) {
            // Make this EQNode the root
            if (!includeReferences.contains(builder.getField()) && excludeReferences.contains(builder.getField())) {
                throw new IllegalStateException(builder.getField() + " is a blacklisted reference.");
            } else {
                root = builder.build();
                
                if (log.isTraceEnabled()) {
                    log.trace("Build IndexIterator: " + root);
                }
            }
        } else {
            AbstractIteratorBuilder iterators = (AbstractIteratorBuilder) data;
            // Add this IndexIterator to the parent
            if (!iterators.hasSeen(builder.getField(), builder.getValue()) && includeReferences.contains(builder.getField())
                            && !excludeReferences.contains(builder.getField())) {
                iterators.addInclude(builder.build());
            } else {
                if (isQueryFullySatisfied == true) {
                    log.warn("Determined that isQueryFullySatisfied should be false, but it was not preset to false by the SatisfactionVisitor");
                }
            }
        }
    }
    
    public IteratorBuildingVisitor setRange(Range documentRange) {
        this.rangeLimiter = documentRange;
        return this;
    }
    
    /**
     * @param documentRange
     * @return
     */
    public IteratorBuildingVisitor limit(Range documentRange) {
        return setRange(documentRange).setLimitLookup(true);
    }
    
    /**
     * Limits the number of source counts.
     * 
     * @param sourceCount
     * @return
     */
    public IteratorBuildingVisitor limit(long sourceCount) {
        source.setInitialSize(sourceCount);
        return this;
    }
    
    /**
     * @param limitLookup
     */
    public IteratorBuildingVisitor setLimitLookup(boolean limitLookup) {
        if (rangeLimiter != null) {
            this.limitLookup = limitLookup;
        }
        return this;
    }
    
    public IteratorBuildingVisitor setIteratorBuilder(Class<? extends IteratorBuilder> clazz) {
        this.iteratorBuilderClass = clazz;
        return this;
    }
    
    public IteratorBuildingVisitor setFieldsToAggregate(Set<String> fieldsToAggregate) {
        this.fieldsToAggregate = (fieldsToAggregate == null ? Collections.emptySet() : fieldsToAggregate);
        return this;
    }
    
    protected boolean isUnindexed(ASTEQNode node) {
        final String fieldName = JexlASTHelper.getIdentifier(node);
        return unindexedFields.contains(fieldName);
    }
    
    protected boolean isUnindexed(ASTIdentifier node) {
        final String fieldName = JexlASTHelper.deconstructIdentifier(node.image);
        return unindexedFields.contains(fieldName);
    }
    
    public IteratorBuildingVisitor setUnindexedFields(Collection<String> unindexedField) {
        this.unindexedFields.addAll(unindexedField);
        return this;
    }
    
    public IteratorBuildingVisitor disableIndexOnly(boolean disableFiEval) {
        this.disableFiEval = disableFiEval;
        return this;
    }
    
    public IteratorBuildingVisitor setCollectTimingDetails(boolean collectTimingDetails) {
        this.collectTimingDetails = collectTimingDetails;
        return this;
    }
    
    public IteratorBuildingVisitor setQuerySpanCollector(QuerySpanCollector querySpanCollector) {
        this.querySpanCollector = querySpanCollector;
        return this;
    }
    
    public IteratorBuildingVisitor limitOverride(boolean limitOverride) {
        this.limitOverride = limitOverride;
        return this;
    }
    
    public IteratorBuildingVisitor setSource(SourceFactory sourceFactory, IteratorEnvironment env) {
        SortedKeyValueIterator<Key,Value> skvi = sourceFactory.getSourceDeepCopy();
        this.source = new SourceManager(skvi);
        this.env = env;
        Map<String,String> options = Maps.newHashMap();
        try {
            this.source.init(skvi, options, env);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return this;
    }
    
    public IteratorBuildingVisitor setTimeFilter(TimeFilter timeFilter) {
        this.timeFilter = timeFilter;
        return this;
    }
    
    public IteratorBuildingVisitor setTypeMetadata(TypeMetadata typeMetadata) {
        this.typeMetadata = typeMetadata;
        return this;
    }
    
    public IteratorBuildingVisitor setIsQueryFullySatisfied(boolean isQueryFullySatisfied) {
        this.isQueryFullySatisfied = isQueryFullySatisfied;
        return this;
    }
    
    public IteratorBuildingVisitor setAttrFilter(EventDataQueryFilter attrFilter) {
        this.attrFilter = attrFilter;
        return this;
    }
    
    public IteratorBuildingVisitor setDatatypeFilter(Predicate<Key> datatypeFilter) {
        this.datatypeFilter = datatypeFilter;
        return this;
    }
    
    public IteratorBuildingVisitor setFiAggregator(FieldIndexAggregator fiAggregator) {
        this.fiAggregator = fiAggregator;
        return this;
    }
    
    public IteratorBuildingVisitor setCompositeMetadata(CompositeMetadata compositeMetadata) {
        this.compositeMetadata = compositeMetadata;
        return this;
    }
    
    public IteratorBuildingVisitor setCompositeSeekThreshold(int compositeSeekThreshold) {
        this.compositeSeekThreshold = compositeSeekThreshold;
        return this;
    }
    
    public IteratorBuildingVisitor setHdfsFileSystem(FileSystemCache hdfsFileSystem) {
        this.hdfsFileSystem = hdfsFileSystem;
        return this;
    }
    
    public IteratorBuildingVisitor setQueryLock(QueryLock queryLock) {
        this.queryLock = queryLock;
        return this;
    }
    
    public IteratorBuildingVisitor setIvaratorCacheDirURIAlternatives(List<String> ivaratorCacheDirURIAlternatives) {
        this.ivaratorCacheDirURIs = ivaratorCacheDirURIAlternatives;
        return this;
    }
    
    public IteratorBuildingVisitor setQueryId(String queryId) {
        this.queryId = queryId;
        return this;
    }
    
    public IteratorBuildingVisitor setScanId(String scanId) {
        this.scanId = scanId;
        return this;
    }
    
    public IteratorBuildingVisitor setIvaratorCacheSubDirPrefix(String ivaratorCacheSubDirPrefix) {
        this.ivaratorCacheSubDirPrefix = (ivaratorCacheSubDirPrefix == null ? "" : ivaratorCacheSubDirPrefix);
        return this;
    }
    
    public IteratorBuildingVisitor setHdfsFileCompressionCodec(String hdfsFileCompressionCodec) {
        this.hdfsFileCompressionCodec = hdfsFileCompressionCodec;
        return this;
    }
    
    public IteratorBuildingVisitor setIvaratorCacheBufferSize(int ivaratorCacheBufferSize) {
        this.ivaratorCacheBufferSize = ivaratorCacheBufferSize;
        return this;
    }
    
    public IteratorBuildingVisitor setIvaratorCacheScanPersistThreshold(long ivaratorCacheScanPersistThreshold) {
        this.ivaratorCacheScanPersistThreshold = ivaratorCacheScanPersistThreshold;
        return this;
    }
    
    public IteratorBuildingVisitor setIvaratorCacheScanTimeout(long ivaratorCacheScanTimeout) {
        this.ivaratorCacheScanTimeout = ivaratorCacheScanTimeout;
        return this;
    }
    
    public IteratorBuildingVisitor setMaxRangeSplit(int maxRangeSplit) {
        this.maxRangeSplit = maxRangeSplit;
        return this;
    }
    
    public IteratorBuildingVisitor setIvaratorMaxOpenFiles(int ivaratorMaxOpenFiles) {
        this.ivaratorMaxOpenFiles = ivaratorMaxOpenFiles;
        return this;
    }
    
    public IteratorBuildingVisitor setIvaratorSources(SourceFactory sourceFactory, int maxIvaratorSources) {
        this.ivaratorSources = new SourcePool(sourceFactory, maxIvaratorSources);
        this.ivaratorSource = new ThreadLocalPooledSource<>(ivaratorSources);
        return this;
    }
    
    public IteratorBuildingVisitor setIncludes(Collection<String> includes) {
        this.includeReferences = includes;
        return this;
    }
    
    public IteratorBuildingVisitor setExcludes(Collection<String> excludes) {
        this.excludeReferences = excludes;
        return this;
    }
    
    public IteratorBuildingVisitor setTermFrequencyFields(Set<String> termFrequencyFields) {
        this.termFrequencyFields = (termFrequencyFields == null ? Collections.emptySet() : termFrequencyFields);
        return this;
    }
    
    public IteratorBuildingVisitor setAllowTermFrequencyLookup(boolean allowTermFrequencyLookup) {
        this.allowTermFrequencyLookup = allowTermFrequencyLookup;
        return this;
    }
    
    public IteratorBuildingVisitor setIndexOnlyFields(Set<String> indexOnlyFields) {
        this.indexOnlyFields = (indexOnlyFields == null ? Collections.emptySet() : indexOnlyFields);
        return this;
    }
    
    public IteratorBuildingVisitor setSortedUIDs(boolean sortedUIDs) {
        this.sortedUIDs = sortedUIDs;
        return this;
    }
    
}
