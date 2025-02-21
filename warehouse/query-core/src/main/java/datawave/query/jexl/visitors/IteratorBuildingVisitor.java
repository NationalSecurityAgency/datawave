package datawave.query.jexl.visitors;

import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.DELAYED;
import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.DROPPED;
import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.EVALUATION_ONLY;
import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.EXCEEDED_OR;
import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.EXCEEDED_VALUE;
import static org.apache.commons.jexl3.parser.JexlNodes.setChildren;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.jexl3.JexlArithmetic;
import org.apache.commons.jexl3.JexlScript;
import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTERNode;
import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.ASTIdentifier;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ASTMethodNode;
import org.apache.commons.jexl3.parser.ASTNENode;
import org.apache.commons.jexl3.parser.ASTNRNode;
import org.apache.commons.jexl3.parser.ASTNotNode;
import org.apache.commons.jexl3.parser.ASTNumberLiteral;
import org.apache.commons.jexl3.parser.ASTOrNode;
import org.apache.commons.jexl3.parser.ASTReferenceExpression;
import org.apache.commons.jexl3.parser.ASTStringLiteral;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.ParserTreeConstants;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.lucene.util.fst.FST;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import datawave.core.iterators.DatawaveFieldIndexListIteratorJexl;
import datawave.core.iterators.filesystem.FileSystemCache;
import datawave.core.iterators.querylock.QueryLock;
import datawave.data.type.NoOpType;
import datawave.query.Constants;
import datawave.query.attributes.AttributeFactory;
import datawave.query.attributes.PreNormalizedAttributeFactory;
import datawave.query.attributes.ValueTuple;
import datawave.query.composite.CompositeMetadata;
import datawave.query.data.parsers.FieldIndexKey;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.iterator.EventFieldIterator;
import datawave.query.iterator.NestedIterator;
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
import datawave.query.iterator.ivarator.IvaratorCacheDir;
import datawave.query.iterator.ivarator.IvaratorCacheDirConfig;
import datawave.query.iterator.logic.OrIterator;
import datawave.query.iterator.logic.RangeFilterIterator;
import datawave.query.iterator.logic.RegexFilterIterator;
import datawave.query.iterator.profile.QuerySpanCollector;
import datawave.query.jexl.ArithmeticJexlEngines;
import datawave.query.jexl.DatawaveJexlContext;
import datawave.query.jexl.DatawaveJexlEngine;
import datawave.query.jexl.DefaultArithmetic;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlASTHelper.IdentifierOpLiteral;
import datawave.query.jexl.LiteralRange;
import datawave.query.jexl.LiteralRange.NodeOperand;
import datawave.query.jexl.functions.EventFieldAggregator;
import datawave.query.jexl.functions.FieldIndexAggregator;
import datawave.query.jexl.functions.IdentityAggregator;
import datawave.query.jexl.functions.JexlFunctionArgumentDescriptorFactory;
import datawave.query.jexl.functions.TermFrequencyAggregator;
import datawave.query.jexl.nodes.ExceededOr;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.jexl.visitors.EventDataQueryExpressionVisitor.ExpressionFilter;
import datawave.query.parser.JavaRegexAnalyzer;
import datawave.query.parser.JavaRegexAnalyzer.JavaRegexParseException;
import datawave.query.predicate.ChainableEventDataQueryFilter;
import datawave.query.predicate.EventDataQueryFilter;
import datawave.query.predicate.Filter;
import datawave.query.predicate.TermFrequencyDataFilter;
import datawave.query.predicate.TimeFilter;
import datawave.query.util.IteratorToSortedKeyValueIterator;
import datawave.query.util.TypeMetadata;
import datawave.query.util.sortedset.FileSortedSet;
import datawave.util.UniversalSet;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;

/**
 * A visitor that builds a tree of iterators. The main points are at ASTAndNodes and ASTOrNodes, where the code will build AndIterators and OrIterators,
 * respectively. This will automatically roll up binary representations of subtrees into a generic n-ary tree because there isn't a true mapping between JEXL
 * AST trees and iterator trees. A JEXL tree can have subtrees rooted at an ASTNotNode whereas an iterator tree cannot.
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
    protected Predicate<Key> datatypeFilter;
    protected TimeFilter timeFilter;

    protected FileSystemCache hdfsFileSystem;
    protected String hdfsFileCompressionCodec;
    protected QueryLock queryLock;
    protected List<IvaratorCacheDirConfig> ivaratorCacheDirConfigs;
    protected String queryId;
    protected String scanId;
    protected String ivaratorCacheSubDirPrefix = "";
    protected long ivaratorCacheScanPersistThreshold = 100000L;
    protected long ivaratorCacheScanTimeout = 1000L * 60 * 60;
    protected int ivaratorCacheBufferSize = 10000;
    protected int maxRangeSplit = 11;
    protected int ivaratorMaxOpenFiles = 100;
    protected long maxIvaratorResults = -1;
    protected int ivaratorNumRetries = 2;
    protected FileSortedSet.PersistOptions ivaratorPersistOptions = new FileSortedSet.PersistOptions();
    protected SortedKeyValueIterator<Key,Value> unsortedIvaratorSource = null;
    protected int ivaratorCount = 0;
    protected GenericObjectPool<SortedKeyValueIterator<Key,Value>> ivaratorSourcePool = null;

    protected TypeMetadata typeMetadata;
    protected EventDataQueryFilter attrFilter;
    protected EventDataQueryFilter fiAttrFilter;
    protected EventDataQueryFilter eventAttrFilter;
    protected Set<String> fieldsToAggregate = Collections.emptySet();
    protected Set<String> termFrequencyFields = Collections.emptySet();
    protected boolean allowTermFrequencyLookup = true;
    protected Set<String> indexOnlyFields = Collections.emptySet();
    protected FieldIndexAggregator fiAggregator;

    protected CompositeMetadata compositeMetadata;
    protected int compositeSeekThreshold = 10;

    // disabled by default
    protected int fiNextSeek = -1;
    protected int eventNextSeek = -1;
    protected int tfNextSeek = -1;

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

    protected boolean useRegexFilter = false;

    /**
     * Keep track of the iterator environment since we are deep copying
     */
    protected IteratorEnvironment env;

    protected Set<JexlNode> delayedEqNodes = Sets.newHashSet();

    protected Map<String,Object> exceededOrEvaluationCache;
    private int deepCopiesCalled = 0;

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
            limitedSource = deepCopySource();
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
        QueryPropertyMarker.Instance instance = QueryPropertyMarker.findInstance(and);
        if (instance.isAnyTypeOf(DELAYED, EVALUATION_ONLY, DROPPED)) {
            if (instance.isType(DELAYED)) {
                JexlNode subNode = instance.getSource();
                if (subNode instanceof ASTEQNode) {
                    delayedEqNodes.add(subNode);
                }
                checkForSatisfactionError();
                log.trace("Will not process ASTDelayedPredicate.");
            }
            return null;
        } else if (instance.isType(EXCEEDED_OR)) {
            JexlNode source = instance.getSource();
            // if the parent is our ExceededOrThreshold marker, then use an
            // Ivarator to get the job done
            if (source instanceof ASTAndNode) {
                try {
                    ivarateList(and, source, data);
                } catch (IOException ioe) {
                    throw new DatawaveFatalQueryException(ioe);
                }
            } else {
                QueryException qe = new QueryException(DatawaveErrorCode.UNEXPECTED_SOURCE_NODE,
                                MessageFormat.format("{0}", "Limited ExceededOrThresholdMarkerJexlNode"));
                throw new DatawaveFatalQueryException(qe);
            }
        } else if (data instanceof IndexRangeIteratorBuilder) {
            // index checking has already been done, otherwise we would not have
            // an "ExceededValueThresholdMarker"
            // hence the "IndexAgnostic" method can be used here
            LiteralRange range = JexlASTHelper.findRange().recursively().getRange(and);
            if (range == null) {
                QueryException qe = new QueryException(DatawaveErrorCode.MULTIPLE_RANGES_IN_EXPRESSION);
                throw new DatawaveFatalQueryException(qe);
            }
            ((IndexRangeIteratorBuilder) data).setRange(range);
        } else if (instance.isType(EXCEEDED_VALUE)) {
            // if the parent is our ExceededValueThreshold marker, then use an
            // Ivarator to get the job done unless we don't have to
            JexlNode source = instance.getSource();

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
            // TODO: this logic is wrong, it will allow an ivarator against a TF field
            if (!limitLookup || !allowTermFrequencyLookup || (indexOnlyFields.contains(identifier) && !termFrequencyFields.contains(identifier))) {
                if (source instanceof ASTAndNode) {
                    try {
                        List<ASTFunctionNode> functionNodes = JexlASTHelper.getFunctionNodes(source).stream()
                                        .filter(node -> JexlFunctionArgumentDescriptorFactory.F.getArgumentDescriptor(node).allowIvaratorFiltering())
                                        .collect(Collectors.toList());
                        if (functionNodes.isEmpty()) {
                            if (useRegexFilter) {
                                contextRequiredRange(and, source, data);
                            } else {
                                ivarateRange(and, source, data);
                            }
                        } else {
                            ivarateFilter(and, source, data, functionNodes);
                        }
                    } catch (IOException ioe) {
                        throw new DatawaveFatalQueryException("Unable to ivarate", ioe);
                    }
                } else if (source instanceof ASTERNode || source instanceof ASTNRNode) {
                    if (source instanceof ASTERNode && useRegexFilter) {
                        // build context iterator for regex filter
                        contextRequiredRegex(and, source, data);
                    } else {
                        try {
                            ivarateRegex(and, source, data);
                        } catch (IOException ioe) {
                            throw new DatawaveFatalQueryException("Unable to ivarate", ioe);
                        }
                    }
                } else {
                    QueryException qe = new QueryException(DatawaveErrorCode.UNEXPECTED_SOURCE_NODE,
                                    MessageFormat.format("{0}", "ExceededValueThresholdMarkerJexlNode"));
                    throw new DatawaveFatalQueryException(qe);
                }
            } else {

                NestedIterator<Key> nested = null;
                if (termFrequencyFields.contains(identifier)) {
                    nested = buildExceededFromTermFrequency(identifier, and, source, range, data);
                } else {
                    /**
                     * This is okay since 1) We are doc specific 2) We are not index only or tf 3) Therefore, we must evaluate against the document for this
                     * expression 4) Return a stubbed range in case we have a disjunction that breaks the current doc.
                     */
                    if (!limitOverride && !negatedOverall)
                        nested = createExceededCheck(identifier, range, and);
                }

                if (null != nested && data instanceof AbstractIteratorBuilder) {

                    AbstractIteratorBuilder iterators = (AbstractIteratorBuilder) data;
                    if (negatedLocal) {
                        iterators.addExclude(nested);
                    } else {
                        iterators.addInclude(nested);
                    }
                } else {
                    checkForSatisfactionError();
                    // if there is no parent
                    if (root == null && data == null) {
                        // make this nested the root node
                        root = nested;
                    }
                    return nested;

                }

            }
        } else if (data instanceof AndIteratorBuilder) {
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
                    // TODO: if the query is a single marker node an AndIterator is still built with a single source.
                    // This isn't functionally incorrect, it could be better.
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
                range.updateLower(Constants.NULL_BYTE_STRING, true, node);
                range.updateUpper(Constants.MAX_UNICODE_STRING, true, node);
            } else {
                range.updateLower(analyzer.getLeadingLiteral(), true, node);
                if (analyzer.hasWildCard()) {
                    range.updateUpper(analyzer.getLeadingLiteral() + Constants.MAX_UNICODE_STRING, true, node);
                } else {
                    range.updateUpper(analyzer.getLeadingLiteral(), true, node);
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
            range.updateLower(analyzer.getLeadingOrTrailingLiteral(), true, node);
            range.updateUpper(analyzer.getLeadingOrTrailingLiteral() + Constants.MAX_UNICODE_STRING, true, node);

            return range;
        } catch (JavaRegexParseException | NoSuchElementException e) {
            throw new DatawaveFatalQueryException(e);
        }
    }

    private NestedIterator<Key> buildExceededFromTermFrequency(String identifier, JexlNode rootNode, JexlNode sourceNode, LiteralRange<?> range, Object data) {
        if (limitLookup) {
            ChainableEventDataQueryFilter wrapped = createWrappedTermFrequencyFilter(sourceNode, attrFilter);
            NestedIterator<Key> eventFieldIterator = new EventFieldIterator(rangeLimiter, deepCopySource(), identifier, new AttributeFactory(this.typeMetadata),
                            getEventFieldAggregator(identifier, wrapped));
            TermFrequencyIndexBuilder builder = new TermFrequencyIndexBuilder();
            builder.setSource(deepCopySource());
            builder.setTypeMetadata(typeMetadata);
            builder.setFieldsToAggregate(fieldsToAggregate);
            builder.setTimeFilter(timeFilter);
            // this code path is only executed in the context of a document range. This optimization scans
            // the TF directly instead of the FI.
            builder.setAttrFilter(eventAttrFilter);
            builder.setEnv(env);
            builder.setTermFrequencyAggregator(getTermFrequencyAggregator(identifier, sourceNode, attrFilter, tfNextSeek));
            builder.setNode(rootNode);
            Range fiRange = getFiRangeForTF(range);

            builder.setRange(fiRange);
            builder.setField(identifier);

            NestedIterator<Key> tfIterator = builder.build();
            return new OrIterator<>(Arrays.asList(tfIterator, eventFieldIterator));
        } else {
            QueryException qe = new QueryException(DatawaveErrorCode.UNEXPECTED_SOURCE_NODE, MessageFormat.format("{0}", "buildExceededFromTermFrequency"));
            throw new DatawaveFatalQueryException(qe);
        }

    }

    protected EventFieldAggregator getEventFieldAggregator(String field, ChainableEventDataQueryFilter filter) {
        return new EventFieldAggregator(field, filter, eventNextSeek, typeMetadata, NoOpType.class.getName());
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
        if (data instanceof OrIteratorBuilder) {
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
        builder = builder.replace("OrIterator:", "\n\tOrIterator:");
        builder = builder.replace("Includes:", "\n\t\tIncludes:");
        builder = builder.replace("Excludes:", "\n\t\tExcludes:");
        builder = builder.replace("Bridge:", "\n\t\t\tBridge:");
        return builder;
    }

    @Override
    public Object visit(ASTNENode node, Object data) {
        // We have no parent already defined
        if (data == null) {
            // We don't support querying only on a negation
            throw new IllegalStateException("Root node cannot be a negation");
        }

        IndexIteratorBuilder builder = getIteratorBuilder();
        node.childrenAccept(this, builder);

        // verify that the field exists and is indexed
        if (builder.getField() == null || isUnindexed(builder.getField())) {
            checkForSatisfactionError();
            return null;
        }

        // the evaluation can handle a term in the form 'FIELD == null', however no IndexIterator should be built
        if (builder.getValue() == null) {
            if (this.indexOnlyFields.contains(builder.getField())) {
                QueryException qe = new QueryException(DatawaveErrorCode.INDEX_ONLY_FIELDS_RETRIEVAL_ERROR,
                                MessageFormat.format("{0} {1} {2}", "Unable to compare index only field", builder.getField(), "against null"));
                throw new DatawaveFatalQueryException(qe);
            }

            // FIELD != null should have set satisfied to false
            checkForSatisfactionError();
            return null;
        }

        AbstractIteratorBuilder iterators = (AbstractIteratorBuilder) data;
        // Add the negated IndexIteratorBuilder to the parent as an *exclude*
        if (!iterators.hasSeen(builder.getField(), builder.getValue()) && includeReferences.contains(builder.getField())
                        && !excludeReferences.contains(builder.getField())) {

            // do not perform a deep copy of the source if this iterator has not been seen yet
            builder.setQueryId(queryId);
            builder.setSource(deepCopySource());
            builder.setTypeMetadata(typeMetadata);
            builder.setTimeFilter(timeFilter);
            builder.setDatatypeFilter(getDatatypeFilter());
            builder.setKeyTransform(getFiAggregator());
            builder.setEnv(env);
            builder.setNode(node);

            iterators.addExclude(builder.build());
        } else {
            // SatisfactionVisitor should have already initialized this to false
            checkForSatisfactionError();
        }

        return null;
    }

    @Override
    public Object visit(ASTMethodNode node, Object data) {
        // if i find a method node then i can't build an index for the identifier it is called on
        return null;
    }

    @Override
    public Object visit(ASTEQNode node, Object data) {
        // visit children first to populate the field and value
        IndexIteratorBuilder builder = getIteratorBuilder();
        node.childrenAccept(this, builder);

        // verify that field exists and is indexed
        if (builder.getField() == null || isUnindexed(builder.getField())) {
            checkForSatisfactionError();
            return null;
        }

        // the evaluation can handle a term in the form 'FIELD == null', however no IndexIterator should be built
        if (builder.getValue() == null) {
            if (indexOnlyFields.contains(builder.getField())) {
                QueryException qe = new QueryException(DatawaveErrorCode.INDEX_ONLY_FIELDS_RETRIEVAL_ERROR,
                                MessageFormat.format("{0} {1} {2}", "Unable to compare index only field", builder.getField(), "against null"));
                throw new DatawaveFatalQueryException(qe);
            }
            return null;
        }

        // check to see if there is a mismatch between included and exclude references.
        // note: this is a lift and shift of old code and probably doesn't work as intended..
        if (!includeReferences.contains(builder.getField()) && excludeReferences.contains(builder.getField())) {
            throw new IllegalStateException(builder.getField() + " is a disallowlisted reference.");
        }

        // We have no parent already defined
        if (data == null) {
            // Make this EQNode the root
            // load the builder just in time and make it the root of the query (query is a single EQ node)
            loadBuilder(builder, data, node);
            root = builder.build();

            if (log.isTraceEnabled()) {
                log.trace("Build IndexIterator: " + root);
            }
        } else {
            AbstractIteratorBuilder iterators = (AbstractIteratorBuilder) data;
            // Add this IndexIterator to the parent
            final boolean isNew = !iterators.hasSeen(builder.getField(), builder.getValue());
            final boolean inclusionReference = includeReferences.contains(builder.getField());
            final boolean notExcluded = !excludeReferences.contains(builder.getField());

            if (isNew && inclusionReference && notExcluded) {
                loadBuilder(builder, data, node);
                iterators.addInclude(builder.build());
            } else {
                checkForSatisfactionError();
            }
        }

        return null;
    }

    /**
     * Load an {@link IndexIteratorBuilder} with all requisite components.
     * <p>
     * Note: at this point the method call to {@link #getSourceIterator(ASTEQNode, boolean)} WILL deep copy the source
     *
     * @param builder
     *            an IndexIteratorBuilder
     * @param data
     *            an existing AbstractIteratorBuilder, or null if this term is the root of the query
     * @param node
     *            the equality term
     */
    protected void loadBuilder(IndexIteratorBuilder builder, Object data, ASTEQNode node) {
        // boolean to tell us if we've overridden our subtree due to
        // a negation or
        boolean isNegation = false;
        if (data instanceof AbstractIteratorBuilder) {
            AbstractIteratorBuilder oib = (AbstractIteratorBuilder) data;
            isNegation = oib.isInANot();
        }
        builder.setQueryId(queryId);
        builder.setSource(getSourceIterator(node, isNegation));
        builder.setTimeFilter(getTimeFilter(node));
        builder.setTypeMetadata(typeMetadata);
        builder.setDatatypeFilter(getDatatypeFilter());
        builder.setKeyTransform(getFiAggregator());
        builder.setEnv(env);
        builder.buildDocument(shouldBuildDocument(builder.getField()));
        builder.setNode(node);
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
                    kvIter = deepCopySource();
                    seekIndexOnlyDocument(kvIter, node);
                } else if (disableFiEval && fieldsToAggregate.contains(identifier)) {
                    kvIter = createIndexOnlyKey(node);
                } else if (limitOverride) {
                    kvIter = createIndexOnlyKey(node);
                } else {
                    kvIter = new IteratorToSortedKeyValueIterator(getNodeEntry(node).iterator());
                }

            } else {
                kvIter = deepCopySource();
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
            return deepCopySource();
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
                mySource = deepCopySource();

            mySource.seek(new Range(newStartKey, true, newStartKey.followingKey(PartialKey.ROW_COLFAM_COLQUAL), false), Collections.emptyList(), false);

            if (mySource.hasTop()) {
                kv.add(Maps.immutableEntry(mySource.getTopKey(), Constants.NULL_VALUE));

            }
        }

        return new IteratorToSortedKeyValueIterator(kv.iterator());
    }

    /**
     * @param kvIter
     *            the key value iterator
     * @param node
     *            the node
     * @throws IOException
     *             for issues with read/write
     */
    protected void seekIndexOnlyDocument(SortedKeyValueIterator<Key,Value> kvIter, ASTEQNode node) throws IOException {
        if (null != rangeLimiter && limitLookup) {
            Key newStartKey = getKey(node);
            kvIter.seek(new Range(newStartKey, true, newStartKey.followingKey(PartialKey.ROW_COLFAM_COLQUAL), false), Collections.emptyList(), false);
        }
    }

    /**
     * @param node
     *            a node
     * @return a collection of entries
     */
    protected Collection<Entry<Key,Value>> getNodeEntry(ASTEQNode node) {
        Key key = getKey(node);
        return Collections.singleton(Maps.immutableEntry(key, Constants.NULL_VALUE));
    }

    /**
     * @param identifier
     *            the identifier
     * @param range
     *            a range
     * @return a collection of entries
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

    /**
     * This method should only be used when we know it is not a term frequency or index only in the limited case, as we will subsequently evaluate this
     * expression during final evaluation
     *
     * @param identifier
     *            an identifier
     * @param range
     *            the range
     * @param rootNode
     *            the root node
     * @return a key iterator
     */
    protected NestedIterator<Key> createExceededCheck(String identifier, LiteralRange<?> range, JexlNode rootNode) {
        IndexIteratorBuilder builder = getIteratorBuilder();

        IteratorToSortedKeyValueIterator kvIter = new IteratorToSortedKeyValueIterator(getExceededEntry(identifier, range).iterator());
        builder.setQueryId(queryId);
        builder.setSource(kvIter);
        builder.setValue(null != range.getLower() ? range.getLower().toString() : "null");
        builder.setField(identifier);
        builder.setTimeFilter(TimeFilter.alwaysTrue());
        builder.setTypeMetadata(typeMetadata);
        builder.setDatatypeFilter(getDatatypeFilter());
        builder.setKeyTransform(getFiAggregator());
        builder.setEnv(env);
        builder.setNode(rootNode);

        return builder.build();
    }

    protected Object visitDelayedIndexOnly(ASTEQNode node, Object data) {
        IndexIteratorBuilder builder = getIteratorBuilder();

        /**
         * If we have an unindexed type enforced, we've been configured to assert whether the field is indexed.
         */
        if (isUnindexed(node)) {
            checkForSatisfactionError();
            return null;
        }

        // boolean to tell us if we've overridden our subtree due to
        // a negation or
        boolean isNegation = (data instanceof AbstractIteratorBuilder && ((AbstractIteratorBuilder) data).isInANot());
        builder.setSource(getSourceIterator(node, isNegation));

        builder.setQueryId(queryId);
        builder.setTimeFilter(getTimeFilter(node));
        builder.setTypeMetadata(typeMetadata);
        builder.setDatatypeFilter(getDatatypeFilter());
        builder.setKeyTransform(getFiAggregator());
        builder.setEnv(env);

        node.childrenAccept(this, builder);

        // A EQNode may be of the form FIELD == null. The evaluation can
        // handle this, so we should just not build an IndexIterator for it.
        if (null == builder.getValue()) {
            checkForSatisfactionError();
            return null;
        }

        // We have no parent already defined
        if (data == null) {
            // Make this EQNode the root
            if (!includeReferences.contains(builder.getField()) && excludeReferences.contains(builder.getField())) {
                throw new IllegalStateException(builder.getField() + " is a disallowlisted reference.");
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
                checkForSatisfactionError();
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
            builder.setField(JexlASTHelper.deconstructIdentifier(node.getName()));
        }

        if (isUnindexed(node)) {
            checkForSatisfactionError();
        }

        return null;
    }

    @Override
    public Object visit(ASTStringLiteral node, Object o) {
        // Set the literal in the IndexIterator
        AbstractIteratorBuilder builder = (AbstractIteratorBuilder) o;
        if (builder != null) {
            builder.setValue(node.getLiteral());
        }

        return null;
    }

    @Override
    public Object visit(ASTNumberLiteral node, Object o) {
        // Set the literal in the IndexIterator
        AbstractIteratorBuilder builder = (AbstractIteratorBuilder) o;
        if (builder != null) {
            builder.setValue(String.valueOf(node.getLiteral()));
        }

        return null;
    }

    /**
     * Build a list of potential hdfs directories based on each ivarator cache dir configs.
     *
     * @return A path
     * @throws IOException
     *             for issues with read/write
     */
    private List<IvaratorCacheDir> getIvaratorCacheDirs() throws IOException {
        List<IvaratorCacheDir> pathAndFs = new ArrayList<>();

        // first lets increment the count for a unique subdirectory
        String subdirectory = ivaratorCacheSubDirPrefix + "term" + Integer.toString(++ivaratorCount);

        if (ivaratorCacheDirConfigs != null && !ivaratorCacheDirConfigs.isEmpty()) {
            for (IvaratorCacheDirConfig config : ivaratorCacheDirConfigs) {

                // first, make sure the cache configuration is valid
                if (config.isValid()) {
                    Path path = new Path(config.getBasePathURI(), queryId);
                    if (scanId == null) {
                        log.warn("Running query iterator for " + queryId + " without a scan id.  This could cause ivarator directory conflicts.");
                    } else {
                        path = new Path(path, scanId);
                    }
                    path = new Path(path, subdirectory);
                    URI uri = path.toUri();
                    pathAndFs.add(new IvaratorCacheDir(config, hdfsFileSystem.getFileSystem(uri), uri.toString()));
                }
            }
        }

        if (pathAndFs.isEmpty())
            throw new IOException("Unable to find a usable hdfs cache dir out of " + ivaratorCacheDirConfigs);

        return pathAndFs;
    }

    /**
     * Build the iterator stack using the regex ivarator (field index caching regex iterator)
     *
     * @param rootNode
     *            the node that was processed to generated this builder
     * @param sourceNode
     *            the source node derived from the root
     * @param data
     *            the node data
     * @throws IOException
     *             for issues with read/write
     */
    public void ivarateRegex(JexlNode rootNode, JexlNode sourceNode, Object data) throws IOException {
        IndexRegexIteratorBuilder builder = new IndexRegexIteratorBuilder();
        if (sourceNode instanceof ASTERNode || sourceNode instanceof ASTNRNode) {
            builder.setNegated(sourceNode instanceof ASTNRNode);
            builder.setField(JexlASTHelper.getIdentifier(sourceNode));
            builder.setValue(String.valueOf(JexlASTHelper.getLiteralValue(sourceNode)));
        } else {
            QueryException qe = new QueryException(DatawaveErrorCode.UNEXPECTED_SOURCE_NODE,
                            MessageFormat.format("{0}", "ExceededValueThresholdMarkerJexlNode"));
            throw new DatawaveFatalQueryException(qe);
        }
        builder.negateAsNeeded(data);
        builder.buildDocument(shouldBuildDocument(builder.getField()));
        ivarate(builder, rootNode, sourceNode, data);
    }

    /**
     * Build the iterator stack using the regex ivarator (field index caching regex iterator)
     *
     * @param rootNode
     *            the node that was processed to generated this builder
     * @param sourceNode
     *            the source node derived from the root
     * @param data
     *            the node data
     * @throws IOException
     *             for issues with read/write
     */
    public void ivarateList(JexlNode rootNode, JexlNode sourceNode, Object data) throws IOException {
        IvaratorBuilder builder = null;

        try {
            ExceededOr exceededOr = new ExceededOr(sourceNode);
            if (exceededOr.getParams().getRanges() != null && !exceededOr.getParams().getRanges().isEmpty()) {
                IndexRangeIteratorBuilder rangeIterBuilder = new IndexRangeIteratorBuilder();
                builder = rangeIterBuilder;

                SortedSet<Range> ranges = exceededOr.getParams().getSortedAccumuloRanges();
                rangeIterBuilder.setSubRanges(exceededOr.getParams().getSortedAccumuloRanges());
                // cache these ranges for use during Jexl Evaluation
                if (exceededOrEvaluationCache != null)
                    exceededOrEvaluationCache.put(exceededOr.getId(), ranges);
                LiteralRange<?> fullRange = new LiteralRange<>(String.valueOf(ranges.first().getStartKey().getRow()), ranges.first().isStartKeyInclusive(),
                                String.valueOf(ranges.last().getEndKey().getRow()), ranges.last().isEndKeyInclusive(),
                                JexlASTHelper.deconstructIdentifier(exceededOr.getField()), NodeOperand.AND);
                rangeIterBuilder.setRange(fullRange);
            } else {
                IndexListIteratorBuilder listIterBuilder = new IndexListIteratorBuilder();
                builder = listIterBuilder;

                if (exceededOr.getParams().getValues() != null && !exceededOr.getParams().getValues().isEmpty()) {
                    Set<String> values = new TreeSet<>(exceededOr.getParams().getValues());
                    listIterBuilder.setValues(values);

                    // cache these values for use during Jexl Evaluation
                    if (exceededOrEvaluationCache != null)
                        exceededOrEvaluationCache.put(exceededOr.getId(), values);
                } else if (exceededOr.getParams().getFstURI() != null) {
                    URI fstUri = new URI(exceededOr.getParams().getFstURI());
                    FST fst;
                    // only recompute this if not already set since this is potentially expensive
                    if (exceededOrEvaluationCache.containsKey(exceededOr.getId())) {
                        fst = (FST) exceededOrEvaluationCache.get(exceededOr.getId());
                    } else {
                        fst = DatawaveFieldIndexListIteratorJexl.FSTManager.get(new Path(fstUri), hdfsFileCompressionCodec,
                                        hdfsFileSystem.getFileSystem(fstUri));
                    }
                    listIterBuilder.setFst(fst);

                    // cache this fst for use during JexlEvaluation.
                    if (exceededOrEvaluationCache != null)
                        exceededOrEvaluationCache.put(exceededOr.getId(), fst);
                }

                // If this is actually negated, then this will be added to excludes. Do not negate in the ivarator
                listIterBuilder.setNegated(false);
            }

            builder.setField(JexlASTHelper.deconstructIdentifier(exceededOr.getField()));
        } catch (IOException | URISyntaxException | NullPointerException e) {
            QueryException qe = new QueryException(DatawaveErrorCode.UNPARSEABLE_EXCEEDED_OR_PARAMS, e, MessageFormat.format("Marker Type: {0}", EXCEEDED_OR));
            throw new DatawaveFatalQueryException(qe);
        }

        builder.negateAsNeeded(data);
        builder.buildDocument(shouldBuildDocument(builder.getField()));

        ivarate(builder, rootNode, sourceNode, data);
    }

    /**
     * Build the iterator stack using the regex ivarator (field index caching regex iterator)
     *
     * @param source
     *            the jexl node
     * @param data
     *            the node data
     * @return a range
     */
    public LiteralRange<?> buildLiteralRange(JexlNode source, Object data) {
        // index checking has already been done, otherwise we would not have an
        // "ExceededValueThresholdMarker"
        // hence the "IndexAgnostic" method can be used here
        if (source instanceof ASTAndNode) {
            LiteralRange range = JexlASTHelper.findRange().recursively().getRange(source);
            if (range == null) {
                QueryException qe = new QueryException(DatawaveErrorCode.MULTIPLE_RANGES_IN_EXPRESSION);
                throw new DatawaveFatalQueryException(qe);
            }
            return range;
        } else {
            QueryException qe = new QueryException(DatawaveErrorCode.UNEXPECTED_SOURCE_NODE,
                            MessageFormat.format("{0}", "ExceededValueThresholdMarkerJexlNode"));
            throw new DatawaveFatalQueryException(qe);
        }
    }

    /**
     * Build the iterator stack using the regex ivarator (field index caching regex iterator)
     *
     * @param rootNode
     *            the node that was processed to generated this builder
     * @param sourceNode
     *            the source node derived from the root
     * @param data
     *            the node data
     * @throws IOException
     *             for issues with read/write
     */
    public void ivarateRange(JexlNode rootNode, JexlNode sourceNode, Object data) throws IOException {
        IndexRangeIteratorBuilder builder = new IndexRangeIteratorBuilder();
        builder.negateAsNeeded(data);
        // index checking has already been done, otherwise we would not have an
        // "ExceededValueThresholdMarker"
        // hence the "IndexAgnostic" method can be used here
        if (sourceNode instanceof ASTAndNode) {
            LiteralRange range = JexlASTHelper.findRange().recursively().getRange(sourceNode);
            if (range == null) {
                QueryException qe = new QueryException(DatawaveErrorCode.MULTIPLE_RANGES_IN_EXPRESSION);
                throw new DatawaveFatalQueryException(qe);
            }
            builder.setRange(range);
        } else {
            QueryException qe = new QueryException(DatawaveErrorCode.UNEXPECTED_SOURCE_NODE,
                            MessageFormat.format("{0}", "ExceededValueThresholdMarkerJexlNode"));
            throw new DatawaveFatalQueryException(qe);
        }

        builder.buildDocument(shouldBuildDocument(builder.getField()));
        ivarate(builder, rootNode, sourceNode, data);
    }

    /**
     * Build the iterator stack using the filter ivarator (field index caching filter iterator)
     *
     * @param rootNode
     *            the node that was processed to generated this builder
     * @param sourceNode
     *            the source node derived from the root
     * @param functionNodes
     *            list of function nodes
     * @param data
     *            the node data
     * @throws IOException
     *             for issues with read/write
     */
    public void ivarateFilter(JexlNode rootNode, JexlNode sourceNode, Object data, List<ASTFunctionNode> functionNodes) throws IOException {
        IndexFilterIteratorBuilder builder = new IndexFilterIteratorBuilder();
        builder.negateAsNeeded(data);
        // index checking has already been done, otherwise we would not have an
        // "ExceededValueThresholdMarker"
        // hence the "IndexAgnostic" method can be used here
        if (sourceNode instanceof ASTAndNode) {
            LiteralRange range = JexlASTHelper.findRange().recursively().getRange(sourceNode);
            if (range == null) {
                QueryException qe = new QueryException(DatawaveErrorCode.MULTIPLE_RANGES_IN_EXPRESSION);
                throw new DatawaveFatalQueryException(qe);
            }
            builder.setRangeAndFunction(range, new FunctionFilter(functionNodes));
        } else {
            QueryException qe = new QueryException(DatawaveErrorCode.UNEXPECTED_SOURCE_NODE, MessageFormat.format("{0}", "ASTFunctionNode"));
            throw new DatawaveFatalQueryException(qe);
        }
        ivarate(builder, rootNode, sourceNode, data);
    }

    /**
     * Create a context-required regex filter. This allows a regex term to defeat documents on the field index without aggregating every single uid that
     * matches.
     *
     * @param and
     *            the marker node
     * @param source
     *            the source of the marker
     * @param data
     *            an iterator builder
     */
    private void contextRequiredRegex(ASTAndNode and, JexlNode source, Object data) {

        SortedKeyValueIterator<Key,Value> sourceCopy = deepCopySource();
        String field = JexlASTHelper.getIdentifier(source);
        String literal = String.valueOf(JexlASTHelper.getLiteralValue(source));

        RegexFilterIterator include = new RegexFilterIterator();
        include.withSource(sourceCopy);
        include.withField(field);
        include.withPattern(literal);
        include.withTimeFilter(timeFilter.getKeyTimeFilter());
        include.withAggregation(isQueryFullySatisfied);
        if (isQueryFullySatisfied) {
            // only aggregate attributes if the query is fully satisfied via the field index
            include.withAttributeFactory(new PreNormalizedAttributeFactory(typeMetadata));
        }

        AbstractIteratorBuilder iterators = (AbstractIteratorBuilder) data;
        iterators.addInclude(include);
    }

    /**
     * Create a context-required range filter. This allows a range term to defeat documents on the field index without aggregating every single uid that
     * matches.
     *
     * @param and
     *            the marker node
     * @param source
     *            the source of the marker
     * @param data
     *            an iterator builder
     */
    private void contextRequiredRange(ASTAndNode and, JexlNode source, Object data) {

        SortedKeyValueIterator<Key,Value> sourceCopy = deepCopySource();
        LiteralRange<?> range = JexlASTHelper.findRange().getRange(source);

        RangeFilterIterator include = new RangeFilterIterator();
        include.withSource(sourceCopy);
        include.withField(range.getFieldName());
        include.withTimeFilter(timeFilter.getKeyTimeFilter());
        include.withLowerBound(range.getLower().toString());
        include.withUpperBound(range.getUpper().toString());
        include.withLowerInclusive(range.isLowerInclusive());
        include.withUpperInclusive(range.isUpperInclusive());
        include.withAggregation(isQueryFullySatisfied);
        if (isQueryFullySatisfied) {
            // only aggregate attributes if the query is fully satisfied via the field index
            include.withAttributeFactory(new PreNormalizedAttributeFactory(typeMetadata));
        }

        AbstractIteratorBuilder iterators = (AbstractIteratorBuilder) data;
        iterators.addInclude(include);
    }

    protected TermFrequencyAggregator getTermFrequencyAggregator(String identifier, JexlNode node, EventDataQueryFilter attrFilter, int maxNextCount) {
        ChainableEventDataQueryFilter wrapped = createWrappedTermFrequencyFilter(node, attrFilter);

        return buildTermFrequencyAggregator(identifier, wrapped, maxNextCount);
    }

    protected TermFrequencyAggregator buildTermFrequencyAggregator(String identifier, ChainableEventDataQueryFilter filter, int maxNextCount) {
        // the only fields the aggregator should keep tokens of should be the TF field IF it is index only. If it is not index only then the value will come
        // from the event and there is no reason to aggregate all tokens we encounter. Since the TF keys are sorted first by value then by field, the aggregator
        // will pick up all fields and values in between the beginning and end of the range if it exists, so specifically for the negation case its critical
        // this list only match the target field
        Set<String> toAggregate = indexOnlyFields.contains(identifier) ? Collections.singleton(identifier) : Collections.emptySet();

        return new TermFrequencyAggregator(toAggregate, filter, maxNextCount);
    }

    protected ChainableEventDataQueryFilter createWrappedTermFrequencyFilter(JexlNode node, EventDataQueryFilter existing) {
        ChainableEventDataQueryFilter chainableFilter = new ChainableEventDataQueryFilter();
        if (existing != null) {
            chainableFilter.addFilter(existing);
        }

        AttributeFactory attributeFactory = new AttributeFactory(typeMetadata);
        Map<String,ExpressionFilter> expressionFilters = EventDataQueryExpressionVisitor.getExpressionFilters(node, attributeFactory);

        chainableFilter.addFilter(new TermFrequencyDataFilter(expressionFilters));

        return chainableFilter;
    }

    public static class FunctionFilter implements Filter {

        private final JexlScript script;
        private final DatawaveJexlContext context = new DatawaveJexlContext();
        private final FieldIndexKey parser = new FieldIndexKey();

        public FunctionFilter(List<ASTFunctionNode> nodes) {
            ASTJexlScript script = new ASTJexlScript(ParserTreeConstants.JJTJEXLSCRIPT);
            if (nodes.size() > 1) {
                ASTAndNode andNode = new ASTAndNode(ParserTreeConstants.JJTANDNODE);
                setChildren(script, andNode);
                setChildren(andNode, nodes.toArray(new JexlNode[0]));
            } else {
                setChildren(script, nodes.get(0));
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

            parser.parse(k);

            final String field = parser.getField();
            final String value = parser.getValue();

            context.clear();
            context.set(field, new ValueTuple(field, value, value, null));

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
     *            the ivarator builder
     * @param rootNode
     *            the node that was processed to generated this builder
     * @param sourceNode
     *            the source node derived from the root
     * @param data
     *            the node data
     * @throws IOException
     *             for issues with read/write
     */
    public void ivarate(IvaratorBuilder builder, JexlNode rootNode, JexlNode sourceNode, Object data) throws IOException {
        builder.setQueryId(queryId);
        builder.setSource(unsortedIvaratorSource);
        builder.setTimeFilter(timeFilter);
        builder.setTypeMetadata(typeMetadata);
        builder.setCompositeMetadata(compositeMetadata);
        builder.setCompositeSeekThreshold(compositeSeekThreshold);
        builder.setDatatypeFilter(getDatatypeFilter());
        builder.setKeyTransform(getFiAggregator());
        builder.setIvaratorCacheDirs(getIvaratorCacheDirs());
        builder.setHdfsFileCompressionCodec(hdfsFileCompressionCodec);
        builder.setQueryLock(queryLock);
        builder.setIvaratorCacheBufferSize(ivaratorCacheBufferSize);
        builder.setIvaratorCacheScanPersistThreshold(ivaratorCacheScanPersistThreshold);
        builder.setIvaratorCacheScanTimeout(ivaratorCacheScanTimeout);
        builder.setMaxRangeSplit(maxRangeSplit);
        builder.setIvaratorMaxOpenFiles(ivaratorMaxOpenFiles);
        builder.setMaxIvaratorResults(maxIvaratorResults);
        builder.setIvaratorNumRetries(ivaratorNumRetries);
        builder.setIvaratorPersistOptions(ivaratorPersistOptions);
        builder.setCollectTimingDetails(collectTimingDetails);
        builder.setQuerySpanCollector(querySpanCollector);
        builder.setSortedUIDs(sortedUIDs);
        builder.setIvaratorSourcePool(ivaratorSourcePool);
        builder.setEnv(env);
        builder.setNode(rootNode);

        // We have no parent already defined
        if (data == null) {
            // Make this EQNode the root
            if (!includeReferences.contains(builder.getField()) && excludeReferences.contains(builder.getField())) {
                throw new IllegalStateException(builder.getField() + " is a disallowlisted reference.");
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
                checkForSatisfactionError();
            }
        }
    }

    /**
     * A document should be aggregated if this is a shard range query and the field index fully satisfies the query, OR if the field is in the list of fields to
     * aggregate (typically index only or non-event fields).
     *
     * @return true if documents need to be built
     */
    protected boolean shouldBuildDocument(String field) {
        if (field == null) {
            throw new IllegalStateException("Must specify a field in order to determine if documents are built");
        }

        // historically this check always ran first
        if (!limitLookup && this.isQueryFullySatisfied) {
            return true;
        }

        // historically this check was a fail-safe
        return fieldsToAggregate != null && fieldsToAggregate.contains(field);
    }

    protected void checkForSatisfactionError() {
        if (isQueryFullySatisfied) {
            log.warn("Determined that isQueryFullySatisfied should be false, but it was not preset to false in the SatisfactionVisitor");
        }
    }

    public IndexIteratorBuilder getIteratorBuilder() {
        try {
            return iteratorBuilderClass.asSubclass(IndexIteratorBuilder.class).getDeclaredConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    protected SortedKeyValueIterator<Key,Value> deepCopySource() {
        deepCopiesCalled++;
        return source.deepCopy(env);
    }

    public int getDeepCopiesCalled() {
        return deepCopiesCalled;
    }

    /**
     * Get the DatatypeFilter
     *
     * @return a DatatypeFilter
     */
    public Predicate<Key> getDatatypeFilter() {
        if (datatypeFilter == null) {
            datatypeFilter = Predicates.alwaysTrue();
        }
        return datatypeFilter;
    }

    /**
     * Get the FieldIndexAggregator
     *
     * @return a FieldIndexAggregator
     */
    public FieldIndexAggregator getFiAggregator() {
        if (fiAggregator == null) {
            fiAggregator = new IdentityAggregator(null);
        }
        return fiAggregator;
    }

    public IteratorBuildingVisitor setRange(Range documentRange) {
        this.rangeLimiter = documentRange;
        return this;
    }

    /**
     * @param documentRange
     *            the document range
     * @return an iterator visitor
     */
    public IteratorBuildingVisitor limit(Range documentRange) {
        return setRange(documentRange).setLimitLookup(true);
    }

    /**
     * Limits the number of source counts.
     *
     * @param sourceCount
     *            the source count
     * @return an iterator visitor
     */
    public IteratorBuildingVisitor limit(long sourceCount) {
        source.setInitialSize(sourceCount);
        return this;
    }

    /**
     * @param limitLookup
     *            the limit lookup to set
     * @return the iterator visitor
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
        final String fieldName = JexlASTHelper.deconstructIdentifier(node.getName());
        return unindexedFields.contains(fieldName);
    }

    protected boolean isUnindexed(String fieldName) {
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

    public IteratorBuildingVisitor setSource(SourceFactory<Key,Value> sourceFactory, IteratorEnvironment env) {
        SortedKeyValueIterator<Key,Value> skvi = sourceFactory.getSourceDeepCopy("IBV");
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

    public IteratorBuildingVisitor setFiAttrFilter(EventDataQueryFilter fiAttrFilter) {
        this.fiAttrFilter = fiAttrFilter;
        return this;
    }

    public IteratorBuildingVisitor setEventAttrFilter(EventDataQueryFilter eventAttrFilter) {
        this.eventAttrFilter = eventAttrFilter;
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

    /**
     * Builder-style method of setting the 'next' seek threshold
     *
     * @param fiNextSeek
     *            next calls before a seek is issued
     * @return the IteratorBuildingVisitor
     */
    public IteratorBuildingVisitor setFiNextSeek(int fiNextSeek) {
        this.fiNextSeek = fiNextSeek;
        return this;
    }

    /**
     * Builder-style method of setting the 'next' seek threshold
     *
     * @param eventNextSeek
     *            next calls before a seek is issued
     * @return the IteratorBuildingVisitor
     */
    public IteratorBuildingVisitor setEventNextSeek(int eventNextSeek) {
        this.eventNextSeek = eventNextSeek;
        return this;
    }

    /**
     * Builder-style method of setting the 'next' seek threshold
     *
     * @param tfNextSeek
     *            next calls before a seek is issued
     * @return the IteratorBuildingVisitor
     */
    public IteratorBuildingVisitor setTfNextSeek(int tfNextSeek) {
        this.tfNextSeek = tfNextSeek;
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

    public IteratorBuildingVisitor setIvaratorCacheDirConfigs(List<IvaratorCacheDirConfig> ivaratorCacheDirConfigs) {
        this.ivaratorCacheDirConfigs = ivaratorCacheDirConfigs;
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

    public IteratorBuildingVisitor setMaxIvaratorResults(long maxIvaratorResults) {
        this.maxIvaratorResults = maxIvaratorResults;
        return this;
    }

    public IteratorBuildingVisitor setIvaratorNumRetries(int ivaratorNumRetries) {
        this.ivaratorNumRetries = ivaratorNumRetries;
        return this;
    }

    public IteratorBuildingVisitor setIvaratorPersistOptions(FileSortedSet.PersistOptions persistOptions) {
        this.ivaratorPersistOptions = persistOptions;
        return this;
    }

    public IteratorBuildingVisitor setUnsortedIvaratorSource(SortedKeyValueIterator<Key,Value> unsortedIvaratorSource) {
        this.unsortedIvaratorSource = unsortedIvaratorSource;
        return this;
    }

    public IteratorBuildingVisitor setIvaratorSourcePool(GenericObjectPool<SortedKeyValueIterator<Key,Value>> ivaratorSourcePool) {
        this.ivaratorSourcePool = ivaratorSourcePool;
        return this;
    }

    public IteratorBuildingVisitor setIncludes(Collection<String> includes) {
        this.includeReferences = Sets.newHashSet(includes);
        this.includeReferences.add(Constants.ANY_FIELD);
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

    public IteratorBuildingVisitor setExceededOrEvaluationCache(Map<String,Object> exceededOrEvaluationCache) {
        this.exceededOrEvaluationCache = exceededOrEvaluationCache;
        return this;
    }

    public IteratorBuildingVisitor setUseRegexFilter(boolean useRegexFilter) {
        this.useRegexFilter = useRegexFilter;
        return this;
    }

    public void resetRoot() {
        this.root = null;
    }
}
