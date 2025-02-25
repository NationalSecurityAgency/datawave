package datawave.query.iterator;

import static com.google.common.base.Preconditions.checkNotNull;
import static datawave.query.iterator.profile.QuerySpan.Stage.DocumentProjection;
import static org.apache.commons.pool.impl.GenericObjectPool.WHEN_EXHAUSTED_BLOCK;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.YieldCallback;
import org.apache.accumulo.core.iterators.YieldingKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.system.IterationInterruptedException;
import org.apache.accumulo.tserver.tablet.TabletClosedException;
import org.apache.commons.collections4.iterators.EmptyIterator;
import org.apache.commons.jexl3.JexlArithmetic;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.collect.UnmodifiableIterator;

import datawave.core.iterators.DatawaveFieldIndexListIteratorJexl;
import datawave.core.iterators.filesystem.FileSystemCache;
import datawave.data.type.Type;
import datawave.data.type.util.NumericalEncoder;
import datawave.ingest.data.config.ingest.CompositeIngest;
import datawave.marking.MarkingFunctionsFactory;
import datawave.query.Constants;
import datawave.query.attributes.AttributeKeepFilter;
import datawave.query.attributes.Document;
import datawave.query.attributes.ExcerptFields;
import datawave.query.attributes.ValueTuple;
import datawave.query.composite.CompositeMetadata;
import datawave.query.exceptions.QueryIteratorYieldingException;
import datawave.query.function.Aggregation;
import datawave.query.function.DataTypeAsField;
import datawave.query.function.DocumentMetadata;
import datawave.query.function.DocumentPermutation;
import datawave.query.function.DocumentProjection;
import datawave.query.function.DocumentRangeProvider;
import datawave.query.function.IndexOnlyContextCreator;
import datawave.query.function.IndexOnlyContextCreatorBuilder;
import datawave.query.function.JexlContextCreator;
import datawave.query.function.JexlEvaluation;
import datawave.query.function.KeyToDocumentData;
import datawave.query.function.LimitFields;
import datawave.query.function.MaskedValueFilterFactory;
import datawave.query.function.MaskedValueFilterInterface;
import datawave.query.function.RangeProvider;
import datawave.query.function.RemoveGroupingContext;
import datawave.query.iterator.aggregation.DocumentData;
import datawave.query.iterator.ivarator.IvaratorCacheDirConfig;
import datawave.query.iterator.pipeline.PipelineFactory;
import datawave.query.iterator.pipeline.PipelineIterator;
import datawave.query.iterator.profile.EvaluationTrackingFunction;
import datawave.query.iterator.profile.EvaluationTrackingIterator;
import datawave.query.iterator.profile.EvaluationTrackingNestedIterator;
import datawave.query.iterator.profile.EvaluationTrackingPredicate;
import datawave.query.iterator.profile.FinalDocumentTrackingIterator;
import datawave.query.iterator.profile.MultiThreadedQuerySpan;
import datawave.query.iterator.profile.PipelineQuerySpanCollectionIterator;
import datawave.query.iterator.profile.QuerySpan;
import datawave.query.iterator.profile.QuerySpanCollector;
import datawave.query.iterator.profile.SourceTrackingIterator;
import datawave.query.jexl.DatawaveJexlContext;
import datawave.query.jexl.StatefulArithmetic;
import datawave.query.jexl.functions.FieldIndexAggregator;
import datawave.query.jexl.functions.IdentityAggregator;
import datawave.query.jexl.functions.KeyAdjudicator;
import datawave.query.jexl.visitors.DelayedNonEventSubTreeVisitor;
import datawave.query.jexl.visitors.IteratorBuildingVisitor;
import datawave.query.jexl.visitors.SatisfactionVisitor;
import datawave.query.jexl.visitors.VariableNameVisitor;
import datawave.query.postprocessing.tf.TFFactory;
import datawave.query.postprocessing.tf.TermFrequencyConfig;
import datawave.query.predicate.EmptyDocumentFilter;
import datawave.query.predicate.Projection;
import datawave.query.statsd.QueryStatsDClient;
import datawave.query.tracking.ActiveQuery;
import datawave.query.tracking.ActiveQueryLog;
import datawave.query.transformer.ExcerptTransform;
import datawave.query.transformer.SummaryTransform;
import datawave.query.transformer.UniqueTransform;
import datawave.query.util.EmptyContext;
import datawave.query.util.EntryToTuple;
import datawave.query.util.TraceIterators;
import datawave.query.util.Tuple2;
import datawave.query.util.Tuple3;
import datawave.query.util.TupleToEntry;
import datawave.query.util.TypeMetadata;
import datawave.query.util.sortedset.FileSortedSet;
import datawave.util.StringUtils;

/**
 * <p>
 * QueryIterator is the entry point to the Datawave query iterator stack. At a high level, this iterator has a source of Document Keys (row + columnfamily) and
 * applies a series of transformations and predicates to satisfy the Datawave query requirements.
 *
 * <br>
 *
 * <h1>Document Keys</h1>
 * <p>
 * The source of Document Keys is one of the following:
 * <ol>
 * <li>Boolean Logic Iterators</li>
 * <li>"Date-Range" scan (formerly known as "full-table scan")</li>
 * </ol>
 * In addition to the Accumulo Key pointing to the document, a Document containing index-only fields matched by the query and a {@link java.util.List} of the
 * {@link Entry}&lt;Key,Value&gt;
 *
 * <br>
 *
 * <h1>Transformations/Predicates</h1>
 * <p>
 * The following transformations/predicates are applied (order sensitive):
 * <ol>
 * <li>Filter on timestamp</li>
 * <li>Build Document from {@link Entry}&lt;Key,Value&gt; and {@link Type} mapping</li>
 * <li>Compute top-level visibility and apply to Document Key</li>
 * <li>Include the datatype as an attribute, if enabled</li>
 * <li>Remove empty documents</li>
 * <li>Jexl Evaluation - Construct JexlContext with term frequency information</li>
 * <li>Jexl Evaluation - Add Document to JexlContext and filter out non-matching Documents</li>
 * <li>PostProcessing Enrichment - Variable enrichment, e.g. term frequency enrichment</li>
 * <li>Serialize Document to a Value, e.g. Kryo, Writable, etc</li>
 * </ol>
 */
public class QueryIterator extends QueryOptions implements YieldingKeyValueIterator<Key,Value>, JexlContextCreator.JexlContextValueComparator,
                SourceFactory<Key,Value>, SortedKeyValueIterator<Key,Value> {

    private static final Logger log = Logger.getLogger(QueryIterator.class);

    protected SortedKeyValueIterator<Key,Value> source;
    protected SortedKeyValueIterator<Key,Value> sourceForDeepCopies;
    protected int numberOfDeepCopies = 0;
    protected List<String> deepCopyStages = new LinkedList<>();

    protected Map<String,String> documentOptions;
    protected NestedIterator<Key> initKeySource, seekKeySource;
    protected Iterator<Entry<Key,Document>> documentIterator;
    protected boolean fieldIndexSatisfiesQuery = false;

    protected Range range;
    protected Range originalRange;

    protected Key key;
    protected Value value;
    protected YieldCallback<Key> yield;

    protected IteratorEnvironment myEnvironment;

    protected JexlEvaluation myEvaluationFunction = null;

    protected QuerySpan trackingSpan = null;

    protected QuerySpanCollector querySpanCollector = new QuerySpanCollector();

    protected UniqueTransform uniqueTransform = null;

    protected GroupingIterator groupingIterator;

    protected boolean groupingContextAddedByMe = false;

    protected TypeMetadata typeMetadataWithNonIndexed = null;

    protected Map<String,Object> exceededOrEvaluationCache = null;

    protected ActiveQueryLog activeQueryLog;

    protected ExcerptTransform excerptTransform = null;

    protected SummaryTransform summaryTransform = null;

    protected RangeProvider rangeProvider;

    public QueryIterator() {}

    public QueryIterator(QueryIterator other, IteratorEnvironment env) {
        // Need to copy all members instantiated/modified during init()
        this.source = other.source.deepCopy(env);
        this.sourceForDeepCopies = source.deepCopy(env);
        this.initKeySource = other.initKeySource;
        this.seekKeySource = other.seekKeySource;
        this.myEnvironment = other.myEnvironment;
        this.myEvaluationFunction = other.myEvaluationFunction;
        this.documentOptions = other.documentOptions;
        this.fieldIndexSatisfiesQuery = other.fieldIndexSatisfiesQuery;
        this.groupingContextAddedByMe = other.groupingContextAddedByMe;
        this.typeMetadataWithNonIndexed = other.typeMetadataWithNonIndexed;
        this.typeMetadata = other.typeMetadata;
        this.exceededOrEvaluationCache = other.exceededOrEvaluationCache;
        this.trackingSpan = other.trackingSpan;
        // Defer to QueryOptions to re-set all the query options
        super.deepCopy(other);
    }

    private boolean gatherTimingDetails() {
        return collectTimingDetails || (statsdHostAndPort != null);
    }

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        if (log.isTraceEnabled()) {
            log.trace("QueryIterator init()");
        }

        if (!validateOptions(new SourcedOptions<>(source, env, options))) {
            throw new IllegalArgumentException("Could not initialize QueryIterator with " + options);
        }

        // We want to add in spoofed dataTypes for Aggregation/Evaluation to
        // ensure proper numeric evaluation.
        this.typeMetadata = new TypeMetadata(this.getTypeMetadata());
        this.typeMetadataWithNonIndexed = new TypeMetadata(this.typeMetadata);
        this.typeMetadataWithNonIndexed.addForAllIngestTypes(this.getNonIndexedDataTypeMap());

        this.exceededOrEvaluationCache = new HashMap<>();
        this.myEvaluationFunction = getJexlEvaluation(this.getQuery(), arithmetic);

        this.documentOptions = options;
        this.myEnvironment = env;

        if (gatherTimingDetails()) {
            this.trackingSpan = new MultiThreadedQuerySpan(getStatsdClient());
            this.source = new SourceTrackingIterator(trackingSpan, source);
        } else {
            this.source = source;
        }

        if (isDebugMultithreadedSources()) {
            this.source = new SourceThreadTrackingIterator<>(this.source);
        }

        this.sourceForDeepCopies = this.source.deepCopy(this.myEnvironment);

        // update ActiveQueryLog with (potentially) updated config
        if (env != null) {
            ActiveQueryLog.setConfig(env.getConfig());
        }

        DatawaveFieldIndexListIteratorJexl.FSTManager.setHdfsFileSystem(this.getFileSystemCache());
        DatawaveFieldIndexListIteratorJexl.FSTManager.setHdfsFileCompressionCodec(this.getHdfsFileCompressionCodec());

        pruneIvaratorCacheDirs();
    }

    // this method will prune any ivarator cache directories that do not have a valid configuration.
    private void pruneIvaratorCacheDirs() throws InterruptedIOException {
        List<IvaratorCacheDirConfig> configsToRemove = new ArrayList<>();
        for (IvaratorCacheDirConfig config : ivaratorCacheDirConfigs) {
            if (!hasValidBasePath(config)) {
                configsToRemove.add(config);
            }
        }
        ivaratorCacheDirConfigs.removeAll(configsToRemove);
    }

    private boolean hasValidBasePath(IvaratorCacheDirConfig config) throws InterruptedIOException {
        if (config.isValid()) {
            try {
                Path basePath = new Path(config.getBasePathURI());
                FileSystemCache cache = this.getFileSystemCache();
                if (cache != null) {
                    FileSystem fileSystem = cache.getFileSystem(basePath.toUri());

                    // Note: The ivarator config base paths are used by ALL queries which run on the system, so there
                    // should be no harm in creating these directories if they do not already exist by this point.
                    // Also, since we are selecting these directories intentionally for use by the ivarators, it
                    // should be a given that we have write permissions.
                    return fileSystem.exists(basePath) || fileSystem.mkdirs(basePath);
                }
            } catch (InterruptedIOException ioe) {
                throw ioe;
            } catch (IOException e) {
                log.error("Failure to validate path " + config, e);
            }
        }
        return false;
    }

    @Override
    public boolean hasTop() {
        boolean yielded = (this.yield != null) && this.yield.hasYielded();
        boolean result = (!yielded) && (this.key != null) && (this.value != null);
        if (log.isTraceEnabled()) {
            log.trace("hasTop() " + result);
        }
        return result;
    }

    @Override
    public void enableYielding(YieldCallback<Key> yieldCallback) {
        this.yield = yieldCallback;
    }

    @Override
    public void next() throws IOException {
        getActiveQueryLog().get(getQueryId()).beginCall(this.originalRange, ActiveQuery.CallType.NEXT);
        try {
            if (log.isTraceEnabled()) {
                log.trace("next");
            }
            prepareKeyValue();
        } catch (Exception e) {
            handleException(e);
        } finally {
            QueryStatsDClient client = getStatsdClient();
            if (client != null) {
                client.flush();
            }
            getActiveQueryLog().get(getQueryId()).endCall(this.originalRange, ActiveQuery.CallType.NEXT);
            if (this.key == null && this.value == null) {
                // no entries to return
                getActiveQueryLog().remove(getQueryId(), this.originalRange);
            }
        }
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        // preserve the original range for use with the Final Document tracking iterator because it is placed after the ResultCountingIterator
        // so the FinalDocumentTracking iterator needs the start key with the count already appended
        originalRange = range;
        getActiveQueryLog().get(getQueryId()).beginCall(this.originalRange, ActiveQuery.CallType.SEEK);
        ActiveQueryLog.getInstance().get(getQueryId()).beginCall(this.originalRange, ActiveQuery.CallType.SEEK);

        try {
            if (!this.isIncludeGroupingContext() && (this.query.contains("grouping:") || this.query.contains("matchesInGroup")
                            || this.query.contains("MatchesInGroup") || this.query.contains("atomValuesMatch"))) {
                this.setIncludeGroupingContext(true);
                this.groupingContextAddedByMe = true;
            } else {
                this.groupingContextAddedByMe = false;
            }

            if (log.isDebugEnabled()) {
                log.debug("Seek range: " + range + " " + query);
            }
            this.range = range;

            // determine whether this is a teardown/rebuild range
            long resultCount = 0;
            if (!range.isStartKeyInclusive()) {
                // see if we can fail fast. If we were rebuilt with the FinalDocument key, then we are already completely done
                if (collectTimingDetails && FinalDocumentTrackingIterator.isFinalDocumentKey(range.getStartKey())) {
                    this.seekKeySource = new EmptyTreeIterable();
                    this.documentIterator = EmptyIterator.emptyIterator();
                    prepareKeyValue();
                    return;
                }

                // see if we have a count in the cf
                Key startKey = range.getStartKey();
                String[] parts = StringUtils.split(startKey.getColumnFamily().toString(), '\0');
                if (parts.length == 3) {
                    resultCount = NumericalEncoder.decode(parts[0]).longValue();
                    // remove the count from the range
                    startKey = new Key(startKey.getRow(), new Text(parts[1] + '\0' + parts[2]), startKey.getColumnQualifier(), startKey.getColumnVisibility(),
                                    startKey.getTimestamp());
                    this.range = range = new Range(startKey, range.isStartKeyInclusive(), range.getEndKey(), range.isEndKeyInclusive());
                }
            }

            // determine whether this is a document specific range
            Range documentRange = isDocumentSpecificRange(range) ? range : null;

            // if we have a document specific range, but the key is not
            // inclusive then we have already returned the document; this scan
            // is done
            if (documentRange != null && !documentRange.isStartKeyInclusive()) {
                if (log.isTraceEnabled()) {
                    log.trace("Received non-inclusive event specific range: " + documentRange);
                }
                if (gatherTimingDetails()) {
                    this.seekKeySource = new EvaluationTrackingNestedIterator(QuerySpan.Stage.EmptyTree, trackingSpan, new EmptyTreeIterable(), myEnvironment);
                } else {
                    this.seekKeySource = new EmptyTreeIterable();
                }
            }

            // if the Range is for a single document and the query doesn't reference any index-only or tokenized fields
            else if (documentRange != null && (!this.isContainsIndexOnlyTerms() && this.getTermFrequencyFields().isEmpty() && !super.mustUseFieldIndex)) {
                if (log.isTraceEnabled()) {
                    log.trace("Received event specific range: " + documentRange);
                }
                // We can take a shortcut to the directly to the event
                Entry<Key,Document> documentKey = Maps.immutableEntry(super.getDocumentKey.apply(documentRange), new Document());
                if (log.isTraceEnabled()) {
                    log.trace("Transformed document key: " + documentKey);
                }
                // we can only trim if we're certain that the projected fields
                // aren't needed for evaluation

                if (gatherTimingDetails()) {
                    this.seekKeySource = new EvaluationTrackingNestedIterator(QuerySpan.Stage.DocumentSpecificTree, trackingSpan,
                                    new DocumentSpecificNestedIterator(documentKey), myEnvironment);
                } else {
                    this.seekKeySource = new DocumentSpecificNestedIterator(documentKey);
                }
            } else {
                this.seekKeySource = buildDocumentIterator(documentRange, range, columnFamilies, inclusive);
            }

            // Create the pipeline iterator for document aggregation and
            // evaluation within a thread pool

            SortedKeyValueIterator<Key,Value> pipelineSource = null;
            if (getMaxEvaluationPipelines() > 1) {
                // only need to create a source copy IFF more than one evaluation pipeline will be used
                // else, the query iterator itself is passed in and the method call to createDocumentPipeline
                // can use the proper method for requesting sources
                pipelineSource = getSourceDeepCopy("pipeline source");
            }
            PipelineIterator pipelineIter = PipelineFactory.createIterator(this.seekKeySource, getMaxEvaluationPipelines(), getMaxPipelineCachedResults(),
                            getSerialPipelineRequest(), querySpanCollector, trackingSpan, this, pipelineSource, myEnvironment, yield, yieldThresholdMs,
                            columnFamilies, inclusive);

            pipelineIter.setCollectTimingDetails(collectTimingDetails);
            // TODO pipelineIter.setStatsdHostAndPort(statsdHostAndPort);

            pipelineIter.startPipeline();

            // gather Key,Document Entries from the pipelines
            Iterator<Entry<Key,Document>> pipelineDocuments = pipelineIter;

            if (log.isTraceEnabled()) {
                pipelineDocuments = Iterators.filter(pipelineDocuments, keyDocumentEntry -> {
                    log.trace("after pipeline, keyDocumentEntry:" + keyDocumentEntry);
                    return true;
                });
            }

            // now apply the unique iterator if requested
            UniqueTransform uniquify = getUniqueTransform();
            if (uniquify != null) {
                pipelineDocuments = uniquify.getIterator(pipelineDocuments);
            }

            // apply the grouping iterator if requested and if the batch size is greater than zero
            // if the batch size is 0, then grouping is computed only on the web server
            if (this.groupFieldsBatchSize > 0) {
                GroupingIterator groupify = getGroupingIteratorInstance(pipelineDocuments);
                if (groupify != null) {
                    pipelineDocuments = groupify;

                    if (log.isTraceEnabled()) {
                        pipelineDocuments = Iterators.filter(pipelineDocuments, keyDocumentEntry -> {
                            log.trace("after grouping, keyDocumentEntry:" + keyDocumentEntry);
                            return true;
                        });
                    }
                }
            }

            pipelineDocuments = Iterators.filter(pipelineDocuments, keyDocumentEntry -> {
                // last chance before the documents are serialized
                getActiveQueryLog().get(getQueryId()).recordStats(keyDocumentEntry.getValue(), querySpanCollector.getCombinedQuerySpan(null));
                // Always return true since we just want to record data in the ActiveQueryLog
                return true;
            });

            this.documentIterator = pipelineDocuments;

            // now add the result count to the keys (required when not sorting UIDs)
            // Cannot do this on document specific ranges as the count would place the keys outside the initial range
            if (!sortedUIDs && documentRange == null) {
                this.documentIterator = new ResultCountingIterator(documentIterator, resultCount, yield);
            } else if (this.sortedUIDs) {
                // we have sorted UIDs, so we can mask out the cq
                this.documentIterator = new KeyAdjudicator<>(documentIterator, yield);
            }

            // only add the final document tracking iterator which sends stats back to the client if collectTimingDetails is true
            if (collectTimingDetails) {
                // if there is no document to return, then add an empty document
                // to store the timing metadata
                this.documentIterator = new FinalDocumentTrackingIterator(querySpanCollector, trackingSpan, originalRange, documentIterator, yield);
            }
            if (log.isTraceEnabled()) {
                this.documentIterator = Iterators.filter(this.documentIterator, keyValueEntry -> {
                    log.debug("finally, considering:" + keyValueEntry);
                    return true;
                });
            }

            // Determine if we have items to return
            prepareKeyValue();
        } catch (Exception e) {
            handleException(e);
        } finally {
            if (gatherTimingDetails() && trackingSpan != null && querySpanCollector != null) {
                querySpanCollector.addQuerySpan(trackingSpan);
            }
            QueryStatsDClient client = getStatsdClient();
            if (client != null) {
                client.flush();
            }
            getActiveQueryLog().get(getQueryId()).endCall(this.originalRange, ActiveQuery.CallType.SEEK);
            if (this.key == null && this.value == null) {
                // no entries to return
                getActiveQueryLog().remove(getQueryId(), this.originalRange);
            }
        }
    }

    /**
     * Handle an exception returned from seek or next. This will silently ignore IterationInterruptedException as that happens when the underlying iterator was
     * interrupted because the client is no longer listening.
     *
     * @param e
     *            the exception to handle
     * @throws IOException
     *             for read/write issues
     */
    private void handleException(Exception e) throws IOException {
        Throwable reason = e;

        // We need to pass IOException, IteratorInterruptedException, and TabletClosedExceptions up to the Tablet as they are
        // handled specially to ensure that the client will retry the scan elsewhere
        IOException ioe = null;
        IterationInterruptedException iie = null;
        QueryIteratorYieldingException qiy = null;
        TabletClosedException tce = null;
        if (reason instanceof IOException) {
            ioe = (IOException) reason;
        }
        if (reason instanceof IterationInterruptedException) {
            iie = (IterationInterruptedException) reason;
        }
        if (reason instanceof QueryIteratorYieldingException) {
            qiy = (QueryIteratorYieldingException) reason;
        }
        if (reason instanceof TabletClosedException) {
            tce = (TabletClosedException) reason;
        }

        int depth = 1;
        while (iie == null && reason.getCause() != null && reason.getCause() != reason && depth < 100) {
            reason = reason.getCause();
            if (reason instanceof IOException) {
                ioe = (IOException) reason;
            }
            if (reason instanceof IterationInterruptedException) {
                iie = (IterationInterruptedException) reason;
            }
            if (reason instanceof QueryIteratorYieldingException) {
                qiy = (QueryIteratorYieldingException) reason;
            }
            if (reason instanceof TabletClosedException) {
                tce = (TabletClosedException) reason;
            }
            depth++;
        }

        // NOTE: Only logging debug (for the most part) here because the Tablet/LookupTask will log the exception
        // as a WARN if we actually have a problem here
        if (iie != null) {
            log.debug("Query interrupted " + queryId, e);
            throw iie;
        } else if (qiy != null) {
            // exit gracefully if we are yielding as a qiy is expected in this case
            if ((this.yield != null) && this.yield.hasYielded()) {
                log.debug("Query yielded " + queryId);
            } else {
                log.error("QueryIteratorYieldingException throws but yield callback not set for " + queryId, qiy);
                throw qiy;
            }
        } else if (tce != null) {
            log.debug("Query tablet closed " + queryId, e);
            throw tce;
        } else if (ioe != null) {
            log.debug("Query io exception " + queryId, e);
            throw ioe;
        } else {
            log.error("Failure for query " + queryId, e);
            throw new RuntimeException("Failure for query " + queryId, e);
        }
    }

    /**
     * Build the document iterator
     *
     * @param documentRange
     *            the document range
     * @param seekRange
     *            the seek range
     * @param columnFamilies
     *            a list of column families
     * @param inclusive
     *            boolean marker for if this is inclusive
     * @return the document iterator
     * @throws IOException
     *             for read/write issues
     * @throws ConfigException
     *             for issues with the configuration
     * @throws InstantiationException
     *             for issues with class instantiation
     * @throws IllegalAccessException
     *             for issues with access
     */
    protected NestedIterator<Key> buildDocumentIterator(Range documentRange, Range seekRange, Collection<ByteSequence> columnFamilies, boolean inclusive)
                    throws IOException, ConfigException, InstantiationException, IllegalAccessException {
        // If we had an event-specific range previously, we need to reset it back
        // to the source we created during init
        NestedIterator<Key> docIter = getOrSetKeySource(documentRange, getScript());

        initKeySource = docIter;

        if (log.isTraceEnabled()) {
            log.trace("Using init()'ialized source: " + this.initKeySource.getClass().getName());
        }

        if (gatherTimingDetails()) {
            docIter = new EvaluationTrackingNestedIterator(QuerySpan.Stage.FieldIndexTree, trackingSpan, docIter, myEnvironment);
        }

        // Seek() the boolean logic stuff
        docIter.seek(range, columnFamilies, inclusive);

        // now lets start off the nested iterator
        docIter.initialize();

        return docIter;
    }

    /**
     * There was a request to create a serial pipeline. The factory may not choose to honor this.
     *
     * @return the serial pipeline
     */
    private boolean getSerialPipelineRequest() {
        return serialEvaluationPipeline;
    }

    /**
     * A routine which should always be used to create deep copies of the source. This ensures that we are thread safe when doing these copies.
     *
     * @return the deep copy of the source
     */
    @Override
    public SortedKeyValueIterator<Key,Value> getSourceDeepCopy() {
        return getSourceDeepCopy(null);
    }

    /**
     * A version of {@link #getSourceDeepCopy()} that tracks the number of deep copies made and optionally tracks which stages requested a source
     *
     * @param stage
     *            the stage name, may be null
     * @return a source deep copy
     */
    @Override
    @SuppressWarnings("SynchronizeOnNonFinalField")
    public SortedKeyValueIterator<Key,Value> getSourceDeepCopy(String stage) {
        synchronized (sourceForDeepCopies) {
            numberOfDeepCopies++;
            deepCopyStages.add(stage);
            return sourceForDeepCopies.deepCopy(this.myEnvironment);
        }
    }

    /**
     * Returns the elements of {@code unfiltered} that satisfy a predicate. This is used instead of the google commons Iterators.filter to create a non-stateful
     * filtering iterator.
     *
     * @param unfiltered
     *            the unfiltered iterator
     * @param predicate
     *            the predicate
     * @param <T>
     *            type for the iterator
     * @return an iterator to elements that satisfy the predicate
     */
    public static <T> UnmodifiableIterator<T> statelessFilter(final Iterator<T> unfiltered, final Predicate<? super T> predicate) {
        checkNotNull(unfiltered);
        checkNotNull(predicate);
        return new UnmodifiableIterator<T>() {
            private T next;

            protected T computeNext() {
                while (unfiltered.hasNext()) {
                    T element = unfiltered.next();
                    if (predicate.apply(element)) {
                        return element;
                    }
                }
                return null;
            }

            @Override
            public final boolean hasNext() {
                if (next == null) {
                    next = computeNext();
                }
                return (next != null);
            }

            @Override
            public T next() {
                T next = this.next;
                this.next = null;
                return next;
            }
        };
    }

    /**
     * Create the pipeline. It is very important that this pipeline can handle resetting the bottom iterator with a new value. This means that hasNext() needs
     * to call the next iterator. The only state that can be maintained is the next value ready after hasNext() has been called. Once next returns the value,
     * the next hasNext() call must call the next iterator again. So for example Iterators.filter() cannot be used as it uses a google commons AbstractIterator
     * that maintains an iterator state (failed, ready, done); use statelessFilter above instead.
     *
     * @param deepSourceCopy
     *            the deep source copy
     * @param documentSpecificSource
     *            the document source
     * @param columnFamilies
     *            the column families
     * @param inclusive
     *            flag for inclusive range
     * @param querySpanCollector
     *            the query span collector
     * @return iterator of keys and values
     */
    public Iterator<Entry<Key,Document>> createDocumentPipeline(SortedKeyValueIterator<Key,Value> deepSourceCopy,
                    final NestedQueryIterator<Key> documentSpecificSource, Collection<ByteSequence> columnFamilies, boolean inclusive,
                    QuerySpanCollector querySpanCollector) {

        QuerySpan trackingSpan = null;
        if (gatherTimingDetails()) {
            trackingSpan = new QuerySpan(getStatsdClient());
        }
        if (log.isTraceEnabled()) {
            log.trace("createDocumentPipeline");
        }
        final Function<Entry<Key,Document>,Entry<DocumentData,Document>> docMapper;

        if (isFieldIndexSatisfyingQuery()) {
            if (log.isTraceEnabled()) {
                log.trace("isFieldIndexSatisfyingQuery");
            }
            docMapper = new Function<>() {
                @Nullable
                @Override
                public Entry<DocumentData,Document> apply(@Nullable Entry<Key,Document> input) {
                    Entry<DocumentData,Document> entry = null;
                    if (input != null) {
                        entry = Maps.immutableEntry(new DocumentData(input.getKey(), Collections.singleton(input.getKey()), Collections.emptyList(), true),
                                        input.getValue());
                    }
                    return entry;
                }
            };
        } else {
            //  @formatter:off
            SortedKeyValueIterator<Key, Value> docMapperSource = getSourceDeepCopy("document pipeline - key to document data");
            docMapper = new KeyToDocumentData(docMapperSource, myEnvironment, documentOptions, getEquality(), getEventEvaluationFilter(), this.includeHierarchyFields,
                            this.includeHierarchyFields)
                            .withRangeProvider(getRangeProvider())
                            .withAggregationThreshold(getDocAggregationThresholdMs());
            //  @formatter:on
        }

        Iterator<Entry<DocumentData,Document>> sourceIterator = Iterators.transform(documentSpecificSource, from -> {
            Entry<Key,Document> entry = Maps.immutableEntry(from, documentSpecificSource.document());
            return docMapper.apply(entry);
        });

        // Take the document Keys and transform it into Entry<Key,Document>,
        // removing Attributes for this Document
        // which do not fall within the expected time range
        Iterator<Entry<Key,Document>> documents = null;
        Aggregation a = new Aggregation(this.getTimeFilter(), this.typeMetadataWithNonIndexed, compositeMetadata, this.isIncludeGroupingContext(),
                        this.includeRecordId, this.disableIndexOnlyDocuments(), getEventEvaluationFilter(), isTrackSizes());
        if (gatherTimingDetails()) {
            documents = Iterators.transform(sourceIterator, new EvaluationTrackingFunction<>(QuerySpan.Stage.Aggregation, trackingSpan, a));
        } else {
            documents = Iterators.transform(sourceIterator, a);
        }

        // Inject the data type as a field if the user requested it
        if (this.includeDatatype) {
            if (gatherTimingDetails()) {
                documents = Iterators.transform(documents,
                                new EvaluationTrackingFunction<>(QuerySpan.Stage.DataTypeAsField, trackingSpan, new DataTypeAsField(this.datatypeKey)));
            } else {
                documents = Iterators.transform(documents, new DataTypeAsField(this.datatypeKey));
            }
        }

        // Inject the document permutations if required
        if (!this.getDocumentPermutations().isEmpty()) {
            if (gatherTimingDetails()) {
                documents = Iterators.transform(documents, new EvaluationTrackingFunction<>(QuerySpan.Stage.DocumentPermutation, trackingSpan,
                                new DocumentPermutation.DocumentPermutationAggregation(this.getDocumentPermutations())));
            } else {
                documents = Iterators.transform(documents, new DocumentPermutation.DocumentPermutationAggregation(this.getDocumentPermutations()));
            }
        }

        SortedKeyValueIterator<Key,Value> evaluationSource = getSourceDeepCopy("document pipeline - evaluation");

        if (gatherTimingDetails()) {
            documents = new EvaluationTrackingIterator(QuerySpan.Stage.DocumentEvaluation, trackingSpan, getEvaluation(documentSpecificSource, evaluationSource,
                            documents, compositeMetadata, typeMetadataWithNonIndexed, columnFamilies, inclusive));
        } else {
            documents = getEvaluation(documentSpecificSource, evaluationSource, documents, compositeMetadata, typeMetadataWithNonIndexed, columnFamilies,
                            inclusive);
        }

        ExcerptTransform excerptTransform = getExcerptTransform();
        if (excerptTransform != null) {
            documents = excerptTransform.getIterator(documents);
        }

        SummaryTransform summaryTransform = getSummaryTransform();
        if (summaryTransform != null) {
            documents = summaryTransform.getIterator(documents);
        }

        // a hook to allow mapping the document such as with the TLD or Parent
        // query logics
        // or if the document was not aggregated in the first place because the
        // field index fields completely satisfied the query
        // No source is passed, mapDocument() uses the correct method to request a source
        documents = mapDocument(null, documents, compositeMetadata);

        // apply any configured post-processing
        documents = getPostProcessingChain(documents);
        if (gatherTimingDetails()) {
            documents = new EvaluationTrackingIterator(QuerySpan.Stage.PostProcessing, trackingSpan, documents);
        }

        // Filter out masked values if requested
        if (this.filterMaskedValues) {
            MaskedValueFilterInterface mvfi = MaskedValueFilterFactory.get(this.isIncludeGroupingContext(), this.isReducedResponse());
            if (gatherTimingDetails()) {
                documents = Iterators.transform(documents, new EvaluationTrackingFunction<>(QuerySpan.Stage.MaskedValueFilter, trackingSpan, mvfi));
            } else {
                documents = Iterators.transform(documents, mvfi);
            }
        }

        // now filter the attributes to those with the keep flag set true
        if (gatherTimingDetails()) {
            documents = Iterators.transform(documents,
                            new EvaluationTrackingFunction<>(QuerySpan.Stage.AttributeKeepFilter, trackingSpan, new AttributeKeepFilter<>()));
        } else {
            documents = Iterators.transform(documents, new AttributeKeepFilter<>());
        }

        // Project fields using an allowlist or a disallowlist before serialization
        if (this.projectResults) {
            if (gatherTimingDetails()) {
                documents = Iterators.transform(documents, new EvaluationTrackingFunction<>(DocumentProjection, trackingSpan, getProjection()));
            } else {
                documents = Iterators.transform(documents, getProjection());
            }
        }

        // remove the composite entries
        documents = Iterators.transform(documents, this.getCompositeProjection());

        // Filter out any Documents which are empty (e.g. due to attribute
        // projection or visibility filtering)
        if (gatherTimingDetails()) {
            documents = statelessFilter(documents,
                            new EvaluationTrackingPredicate<>(QuerySpan.Stage.EmptyDocumentFilter, trackingSpan, new EmptyDocumentFilter()));
            documents = Iterators.transform(documents,
                            new EvaluationTrackingFunction<>(QuerySpan.Stage.DocumentMetadata, trackingSpan, new DocumentMetadata()));
        } else {
            documents = statelessFilter(documents, new EmptyDocumentFilter());
            documents = Iterators.transform(documents, new DocumentMetadata());
        }

        if (!this.limitFieldsMap.isEmpty()) {
            // note that we have already reduced the document to those attributes to keep. This will reduce the attributes further
            // base on those fields we are limiting.
            if (gatherTimingDetails()) {
                documents = Iterators.transform(documents, new EvaluationTrackingFunction<>(QuerySpan.Stage.LimitFields, trackingSpan, getLimitFields()));
            } else {
                documents = Iterators.transform(documents, getLimitFields());
            }
        }

        // do I need to remove the grouping context I added above?
        if (groupingContextAddedByMe) {
            if (gatherTimingDetails()) {
                documents = Iterators.transform(documents,
                                new EvaluationTrackingFunction<>(QuerySpan.Stage.RemoveGroupingContext, trackingSpan, new RemoveGroupingContext()));
            } else {
                documents = Iterators.transform(documents, new RemoveGroupingContext());
            }
        }

        // only add the pipeline query span collection iterator which will cache metrics with each document if collectTimingDetails is true
        if (collectTimingDetails) {
            // if there is not a result, then add the trackingSpan to the
            // QuerySpanCollector
            // if there was a result, then the metrics from the trackingSpan
            // will be added here
            documents = new PipelineQuerySpanCollectionIterator(querySpanCollector, trackingSpan, documents);
        }

        return documents;
    }

    protected Iterator<Entry<Key,Document>> getEvaluation(SortedKeyValueIterator<Key,Value> sourceDeepCopy, Iterator<Entry<Key,Document>> documents,
                    CompositeMetadata compositeMetadata, TypeMetadata typeMetadataForEval, Collection<ByteSequence> columnFamilies, boolean inclusive) {
        return getEvaluation(null, sourceDeepCopy, documents, compositeMetadata, typeMetadataForEval, columnFamilies, inclusive);
    }

    protected Iterator<Entry<Key,Document>> getEvaluation(NestedQueryIterator<Key> documentSource, SortedKeyValueIterator<Key,Value> sourceDeepCopy,
                    Iterator<Entry<Key,Document>> documents, CompositeMetadata compositeMetadata, TypeMetadata typeMetadataForEval,
                    Collection<ByteSequence> columnFamilies, boolean inclusive) {
        // Filter the Documents by testing them against the JEXL query
        if (!this.disableEvaluation) {

            JexlEvaluation jexlEvaluationFunction = getJexlEvaluation(documentSource);
            Collection<String> variables = null;
            if (null != documentSource && null != documentSource.getQuery()) {

                variables = VariableNameVisitor.parseQuery(jexlEvaluationFunction.parse(documentSource.getQuery()));
            } else {
                variables = VariableNameVisitor.parseQuery(jexlEvaluationFunction.parse(query));
            }

            final Iterator<Tuple2<Key,Document>> tupleItr = Iterators.transform(documents, new EntryToTuple<>());

            // get the function we use for the tf functionality. Note we are
            // getting an additional source deep copy for this function
            final Iterator<Tuple3<Key,Document,Map<String,Object>>> itrWithContext;
            // TODO: this should be dynamic based on the query fields, not a flag passed to the iterator
            if (this.isTermFrequenciesRequired()) {

                SortedKeyValueIterator<Key,Value> tfSource = getSourceDeepCopy("tf aggregation");

                TermFrequencyConfig tfConfig = new TermFrequencyConfig();
                tfConfig.setScript(getScript(documentSource));
                tfConfig.setSource(tfSource);
                tfConfig.setContentExpansionFields(getContentExpansionFields());
                tfConfig.setTfFields(getTermFrequencyFields());
                tfConfig.setTypeMetadata(getTypeMetadata());
                tfConfig.setEquality(getEquality());
                tfConfig.setEvaluationFilter(getEvaluationFilter());
                tfConfig.setTfAggregationThreshold(getTfAggregationThresholdMs());

                Function<Tuple2<Key,Document>,Tuple3<Key,Document,Map<String,Object>>> tfFunction = buildTfFunction(tfConfig);
                itrWithContext = TraceIterators.transform(tupleItr, tfFunction, "Term Frequency Lookup");
            } else {
                itrWithContext = Iterators.transform(tupleItr, new EmptyContext<>());
            }

            try {
                IteratorBuildingVisitor iteratorBuildingVisitor = createIteratorBuildingVisitor(getDocumentRange(documentSource), false, this.sortedUIDs);
                Multimap<String,JexlNode> delayedNonEventFieldMap = DelayedNonEventSubTreeVisitor.getDelayedNonEventFieldMap(iteratorBuildingVisitor,
                                getScript(), getNonEventFields());

                IndexOnlyContextCreatorBuilder contextCreatorBuilder = new IndexOnlyContextCreatorBuilder().setSource(sourceDeepCopy)
                                .setRange(getDocumentRange(documentSource)).setTypeMetadata(typeMetadataForEval).setCompositeMetadata(compositeMetadata)
                                .setOptions(this).setVariables(variables).setIteratorBuildingVisitor(iteratorBuildingVisitor)
                                .setDelayedNonEventFieldMap(delayedNonEventFieldMap).setEquality(getEquality()).setColumnFamilies(columnFamilies)
                                .setInclusive(inclusive).setComparatorFactory(this);
                final IndexOnlyContextCreator contextCreator = contextCreatorBuilder.build();

                if (exceededOrEvaluationCache != null) {
                    contextCreator.addAdditionalEntries(exceededOrEvaluationCache);
                }

                final Iterator<Tuple3<Key,Document,DatawaveJexlContext>> itrWithDatawaveJexlContext = Iterators.transform(itrWithContext, contextCreator);
                Iterator<Tuple3<Key,Document,DatawaveJexlContext>> matchedDocuments = statelessFilter(itrWithDatawaveJexlContext, jexlEvaluationFunction);
                if (log.isTraceEnabled()) {
                    log.trace("arithmetic:" + arithmetic + " range:" + getDocumentRange(documentSource) + ", thread:" + Thread.currentThread());
                }
                return Iterators.transform(matchedDocuments, new TupleToEntry<>());
            } catch (InstantiationException | MalformedURLException | IllegalAccessException | ConfigException e) {
                throw new IllegalStateException("Could not perform delayed index only evaluation", e);
            }
        } else if (log.isTraceEnabled()) {
            log.trace("Evaluation is disabled, not instantiating Jexl evaluation logic");
        }
        return documents;
    }

    /**
     * This method exists so that extending classes can implement specific versions of the TFFunction. Specifically, so the
     * {@link datawave.query.tld.TLDQueryIterator} can set the {@link TermFrequencyConfig#setTld(boolean)} option to true
     *
     * @param tfConfig
     *            a TermFrequencyConfig
     * @return a TFFunction
     */
    protected Function<Tuple2<Key,Document>,Tuple3<Key,Document,Map<String,Object>>> buildTfFunction(TermFrequencyConfig tfConfig) {
        return TFFactory.getFunction(tfConfig);
    }

    private Range getDocumentRange(NestedQueryIterator<Key> documentSource) {
        if (null == documentSource) {
            return range;
        }
        NestedQuery<Key> nestedQuery = documentSource.getNestedQuery();
        if (null == nestedQuery) {
            return range;
        } else {
            return nestedQuery.getRange();
        }
    }

    protected JexlEvaluation getJexlEvaluation(NestedQueryIterator<Key> documentSource) {
        return getJexlEvaluation(query, documentSource, getArithmetic());
    }

    protected JexlEvaluation getJexlEvaluation(String query) {
        return getJexlEvaluation(query, null, getArithmetic());
    }

    protected JexlEvaluation getJexlEvaluation(String query, JexlArithmetic arithmetic) {
        return getJexlEvaluation(query, null, arithmetic);
    }

    protected JexlEvaluation getJexlEvaluation(String query, NestedQueryIterator<Key> documentSource) {
        return getJexlEvaluation(query, documentSource, getArithmetic());
    }

    protected JexlEvaluation getJexlEvaluation(String query, NestedQueryIterator<Key> documentSource, JexlArithmetic arithmetic) {
        JexlEvaluation jexlEvaluationFunction = null;

        if (arithmetic == null) {
            arithmetic = getArithmetic();
        }

        if (null == documentSource) {
            jexlEvaluationFunction = new JexlEvaluation(query, arithmetic);
        } else {
            NestedQuery<Key> nestedQuery = documentSource.getNestedQuery();
            if (null == nestedQuery) {
                jexlEvaluationFunction = new JexlEvaluation(query, arithmetic);
            } else {
                jexlEvaluationFunction = nestedQuery.getEvaluation();
                if (null == jexlEvaluationFunction) {
                    jexlEvaluationFunction = new JexlEvaluation(query, arithmetic);
                }
            }
        }

        // update the jexl evaluation to gather phrase offsets if required for excerpts
        ExcerptFields excerptFields = getExcerptFields();
        if (excerptFields != null && !excerptFields.isEmpty()) {
            jexlEvaluationFunction.setGatherPhraseOffsets(true);
            jexlEvaluationFunction.setPhraseOffsetFields(excerptFields.getFields());
        }

        return jexlEvaluationFunction;
    }

    protected LimitFields getLimitFields() {
        return new LimitFields(this.getLimitFieldsMap(), this.getMatchingFieldSets());
    }

    @Override
    public JexlArithmetic getArithmetic() {
        JexlArithmetic myArithmetic = this.arithmetic;
        if (myArithmetic instanceof StatefulArithmetic) {
            myArithmetic = ((StatefulArithmetic) arithmetic).clone();
        }
        return myArithmetic;
    }

    protected ASTJexlScript getScript(NestedQueryIterator<Key> documentSource) {
        if (null == documentSource) {
            return getScript();
        }
        NestedQuery<Key> query = documentSource.getNestedQuery();
        if (null == query) {
            return getScript();
        } else {
            ASTJexlScript rangeScript = query.getScript();
            if (null == rangeScript) {
                return getScript();
            }
            return rangeScript;
        }
    }

    protected Iterator<Entry<Key,Document>> mapDocument(SortedKeyValueIterator<Key,Value> deepSourceCopy, Iterator<Entry<Key,Document>> documents,
                    CompositeMetadata compositeMetadata) {
        // now lets pull the data if we need to
        if (log.isTraceEnabled()) {
            log.trace("mapDocument " + fieldIndexSatisfiesQuery);
        }
        if (fieldIndexSatisfiesQuery) {
            //  @formatter:off

            SortedKeyValueIterator<Key, Value> docMapperSource = getSourceDeepCopy("map document - key to document");

            final KeyToDocumentData docMapper = new KeyToDocumentData(docMapperSource, this.myEnvironment, this.documentOptions, getEquality(),
                            getEventEvaluationFilter(), this.includeHierarchyFields, this.includeHierarchyFields)
                            .withRangeProvider(getRangeProvider())
                            .withAggregationThreshold(getDocAggregationThresholdMs());
            //  @formatter:on

            Iterator<Tuple2<Key,Document>> mappedDocuments = Iterators.transform(documents,
                            new GetDocument(docMapper,
                                            new Aggregation(this.getTimeFilter(), typeMetadataWithNonIndexed, compositeMetadata,
                                                            this.isIncludeGroupingContext(), this.includeRecordId, this.disableIndexOnlyDocuments(),
                                                            getEventEvaluationFilter(), isTrackSizes())));
            Iterator<Entry<Key,Document>> retDocuments = Iterators.transform(mappedDocuments, new TupleToEntry<>());

            // Inject the document permutations if required
            if (!this.getDocumentPermutations().isEmpty()) {
                if (gatherTimingDetails()) {
                    retDocuments = Iterators.transform(retDocuments, new EvaluationTrackingFunction<>(QuerySpan.Stage.DocumentPermutation, trackingSpan,
                                    new DocumentPermutation.DocumentPermutationAggregation(this.getDocumentPermutations())));
                } else {
                    retDocuments = Iterators.transform(retDocuments, new DocumentPermutation.DocumentPermutationAggregation(this.getDocumentPermutations()));
                }
            }
            return retDocuments;
        }
        return documents;
    }

    public class GetDocument implements Function<Entry<Key,Document>,Tuple2<Key,Document>> {
        private final KeyToDocumentData fetchDocData;
        private final Aggregation makeDocument;
        private final EntryToTuple<Key,Document> convert = new EntryToTuple<>();

        public GetDocument(KeyToDocumentData fetchDocData, Aggregation makeDocument) {
            this.fetchDocData = fetchDocData;
            this.makeDocument = makeDocument;
        }

        public Tuple2<Key,Document> apply(Entry<Key,Document> from) {
            from = makeDocument.apply(this.fetchDocData.apply(from));
            return convert.apply(from);
        }
    }

    /**
     * Gets the next document and serializes it for return
     */
    protected void prepareKeyValue() {
        if (documentIterator.hasNext()) {

            // just in time serialization
            Entry<Key,Document> docEntry = documentIterator.next();
            Entry<Key,Value> entry = getDocumentSerializer().apply(docEntry);

            if (log.isTraceEnabled()) {
                log.trace("next() returned " + entry.getKey());
            }

            this.key = entry.getKey();
            this.value = entry.getValue();

        } else {
            if (log.isTraceEnabled()) {
                log.trace("Exhausted all keys");
            }
            this.key = null;
            this.value = null;

            if (log.isTraceEnabled()) {
                synchronized (this) {
                    log.trace("=== produced " + numberOfDeepCopies + " source deep copies ===");
                    for (String stage : deepCopyStages) {
                        log.trace(stage);
                    }
                }
            }
        }
    }

    @Override
    public Key getTopKey() {
        return this.key;
    }

    @Override
    public Value getTopValue() {
        return this.value;
    }

    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        return new QueryIterator(this, env);
    }

    /**
     * If we are performing evaluation (have a query) and are not performing a full-table scan, then we want to instantiate the boolean logic iterators
     *
     * @return Whether or not the boolean logic iterators should be used
     */
    public boolean instantiateBooleanLogic() {
        return !this.disableEvaluation && !this.fullTableScanOnly;
    }

    public void debugBooleanLogicIterators(NestedIterator<Key> root) {
        if (log.isDebugEnabled()) {
            debugBooleanLogicIterator(root, "");
        }
    }

    private void debugBooleanLogicIterator(NestedIterator<Key> root, String prefix) {
        log.debug(root);

        if (null == root || null == root.children()) {
            return;
        }

        for (NestedIterator<Key> child : root.children()) {
            debugBooleanLogicIterator(child, prefix + "  ");
        }
    }

    protected DocumentProjection getProjection() {

        if (this.useAllowListedFields) {
            // make sure we include any fields being matched in the limit fields mechanism
            if (!this.matchingFieldSets.isEmpty()) {
                this.allowListedFields.addAll(getMatchingFieldList());
            }
            return new DocumentProjection(this.isIncludeGroupingContext(), this.isReducedResponse(), isTrackSizes(),
                            new Projection(this.allowListedFields, Projection.ProjectionType.INCLUDES));
        } else if (this.useDisallowListedFields) {
            // make sure we are not excluding any fields being matched in the limit fields mechanism
            if (!this.matchingFieldSets.isEmpty()) {
                this.disallowListedFields.removeAll(getMatchingFieldList());
            }
            return new DocumentProjection(this.isIncludeGroupingContext(), this.isReducedResponse(), isTrackSizes(),
                            new Projection(this.disallowListedFields, Projection.ProjectionType.EXCLUDES));
        } else {
            String msg = "Configured to use projection, but no allowlist or disallowlist was provided";
            log.error(msg);
            throw new IllegalArgumentException(msg);
        }
    }

    protected DocumentProjection getCompositeProjection() {
        Set<String> composites = Sets.newHashSet();
        if (compositeMetadata != null) {
            for (Multimap<String,String> val : this.compositeMetadata.getCompositeFieldMapByType().values()) {
                for (String compositeField : val.keySet()) {
                    if (!CompositeIngest.isOverloadedCompositeField(val, compositeField)) {
                        composites.add(compositeField);
                    }
                }
            }
        }
        // make sure we include any fields being matched in the limit fields mechanism
        if (!this.matchingFieldSets.isEmpty()) {
            composites.removeAll(getMatchingFieldList());
        }
        return new DocumentProjection(this.isIncludeGroupingContext(), this.isReducedResponse(), isTrackSizes(), composites,
                        Projection.ProjectionType.EXCLUDES);
    }

    /**
     * Determines if a range is document specific according to the following criteria
     *
     * <pre>
     *     1. Cannot have a null start or end key
     *     2. Cannot span multiple rows
     *     3. ColumnFamily must contain a null byte separator
     * </pre>
     *
     * @param r
     *            - {@link Range} to be evaluated
     * @return - true if this is a document specific range, false if not.
     */
    public static boolean isDocumentSpecificRange(Range r) {
        Preconditions.checkNotNull(r);

        // Also @see datawave.query.index.lookup.TupleToRange
        // We have already made the assertion that the client is sending us
        // an inclusive start key due to the inability to ascertain the
        // difference between and event-specific range and a continueMultiScan.
        //
        // As such, it is acceptable for us to make the same assertion on the
        // inclusivity of the start key.

        // Cannot have a null start or end key
        if (r.isInfiniteStartKey() || r.isInfiniteStopKey()) {
            return false;
        }

        // Cannot span multiple rows.
        Key startKey = r.getStartKey();
        Key endKey = r.getEndKey();
        if (!startKey.getRowData().equals(endKey.getRowData())) {
            return false;
        }

        // Column Family must contain a null byte separator.
        Text startCF = startKey.getColumnFamily();
        Text endCF = endKey.getColumnFamily();
        if (startCF.find(Constants.NULL) == -1 || endCF.find(Constants.NULL) == -1) {
            return false;
        }
        return true;
    }

    /**
     * Convert the given key's row &amp; column family to a string.
     *
     * @param k
     *            - a {@link Key}
     * @return - a string representation of the given key's row &amp; column family.
     */
    public static String rowColFamToString(Key k) {
        if (null == k) {
            return "null";
        }

        final Text holder = new Text();
        StringBuilder sb = new StringBuilder(40);

        k.getRow(holder);
        Key.appendPrintableString(holder.getBytes(), 0, holder.getLength(), org.apache.accumulo.core.Constants.MAX_DATA_TO_PRINT, sb);
        sb.append(Constants.SPACE);

        k.getColumnFamily(holder);
        Key.appendPrintableString(holder.getBytes(), 0, holder.getLength(), org.apache.accumulo.core.Constants.MAX_DATA_TO_PRINT, sb);

        sb.append(Constants.COLON);

        k.getColumnQualifier(holder);
        Key.appendPrintableString(holder.getBytes(), 0, holder.getLength(), org.apache.accumulo.core.Constants.MAX_DATA_TO_PRINT, sb);

        return sb.toString();
    }

    protected NestedIterator<Key> getOrSetKeySource(final Range documentRange, ASTJexlScript rangeScript)
                    throws IOException, ConfigException, IllegalAccessException, InstantiationException {
        NestedIterator<Key> sourceIter = null;
        // If we're doing field index or a non-fulltable (aka a normal
        // query)
        if (!this.isFullTableScanOnly()) {

            // we assume the query is satisfiable as an initial state
            boolean isQueryFullySatisfiedInitialState = true;
            String hitListOptionString = documentOptions.get(QueryOptions.HIT_LIST);

            if (hitListOptionString != null) {
                boolean hitListOption = Boolean.parseBoolean(hitListOptionString);
                if (hitListOption) {
                    isQueryFullySatisfiedInitialState = false; // if hit
                    // list is
                    // on, don't
                    // attempt
                    // satisfiability
                    // don't even make a SatisfactionVisitor.....
                }
            }
            if (isQueryFullySatisfiedInitialState) {
                SatisfactionVisitor satisfactionVisitor = this.createSatisfiabilityVisitor(true); // we'll
                // charge
                // in
                // with
                // optimism

                // visit() and get the root which is the root of a tree of
                // Boolean Logic Iterator<Key>'s
                rangeScript.jjtAccept(satisfactionVisitor, null);

                isQueryFullySatisfiedInitialState = satisfactionVisitor.isQueryFullySatisfied();

            }

            IteratorBuildingVisitor visitor = createIteratorBuildingVisitor(documentRange, isQueryFullySatisfiedInitialState, this.sortedUIDs);

            // visit() and get the root which is the root of a tree of
            // Boolean Logic Iterator<Key>'s
            rangeScript.jjtAccept(visitor, null);

            sourceIter = visitor.root();

            if (visitor.isQueryFullySatisfied()) {
                this.fieldIndexSatisfiesQuery = true;
            }

            // Print out the boolean logic tree of iterators
            debugBooleanLogicIterators(sourceIter);

            if (sourceIter != null) {
                sourceIter = new SeekableNestedIterator(sourceIter, this.myEnvironment);
            }
        }

        // resort to a full table scan otherwise
        if (sourceIter == null) {
            sourceIter = getEventDataNestedIterator(source);
        }

        return sourceIter;
    }

    /**
     * Determine whether the query can be completely satisfied by the field index
     *
     * @return true if it can be completely satisfied.
     */
    protected boolean isFieldIndexSatisfyingQuery() {
        return this.fieldIndexSatisfiesQuery;
    }

    protected NestedIterator<Key> getEventDataNestedIterator(SortedKeyValueIterator<Key,Value> source) {
        return new EventDataScanNestedIterator(source, getEventEntryKeyDataTypeFilter());
    }

    protected IteratorBuildingVisitor createIteratorBuildingVisitor(final Range documentRange, boolean isQueryFullySatisfied, boolean sortedUIDs)
                    throws ConfigException, MalformedURLException, InstantiationException, IllegalAccessException {
        return createIteratorBuildingVisitor(IteratorBuildingVisitor.class, documentRange, isQueryFullySatisfied, sortedUIDs);
    }

    protected IteratorBuildingVisitor createIteratorBuildingVisitor(Class<? extends IteratorBuildingVisitor> c, final Range documentRange,
                    boolean isQueryFullySatisfied, boolean sortedUIDs)
                    throws MalformedURLException, ConfigException, IllegalAccessException, InstantiationException {
        if (log.isTraceEnabled()) {
            log.trace(documentRange);
        }

        // determine the list of indexed fields
        Set<String> indexedFields = this.getIndexedFields();
        Set<String> nonIndexedFields = this.getNonIndexedDataTypeMap().keySet();
        indexedFields.removeAll(nonIndexedFields);

        // @formatter:off
        return c.newInstance()
                .setSource(this, this.myEnvironment)
                .setTimeFilter(this.getTimeFilter())
                .setTypeMetadata(this.getTypeMetadata())
                .setFieldsToAggregate(this.getNonEventFields())
                .setAttrFilter(this.getEvaluationFilter())
                .setFiAttrFilter(this.getFiEvaluationFilter())
                .setEventAttrFilter(this.getEventEvaluationFilter())    //  needed document range TF optimization
                .setDatatypeFilter(this.getFieldIndexKeyDataTypeFilter())
                .setFiAggregator(this.getFiAggregator())
                .setHdfsFileSystem(this.getFileSystemCache())
                .setQueryLock(this.getQueryLock())
                .setIvaratorCacheDirConfigs(this.getIvaratorCacheDirConfigs())
                .setQueryId(this.getQueryId())
                .setScanId(this.getScanId())
                .setIvaratorCacheSubDirPrefix(this.getHdfsCacheSubDirPrefix())
                .setHdfsFileCompressionCodec(this.getHdfsFileCompressionCodec())
                .setIvaratorCacheBufferSize(this.getIvaratorCacheBufferSize())
                .setIvaratorCacheScanPersistThreshold(this.getIvaratorCacheScanPersistThreshold())
                .setIvaratorCacheScanTimeout(this.getIvaratorCacheScanTimeout())
                .setMaxRangeSplit(this.getMaxIndexRangeSplit())
                .setIvaratorMaxOpenFiles(this.getIvaratorMaxOpenFiles())
                .setIvaratorNumRetries(this.getIvaratorNumRetries())
                .setIvaratorPersistOptions(this.getIvaratorPersistOptions())
                .setUnsortedIvaratorSource(this.sourceForDeepCopies)
                .setIvaratorSourcePool(createIvaratorSourcePool(this.maxIvaratorSources, this.ivaratorCacheScanTimeout))
                .setMaxIvaratorResults(this.getMaxIvaratorResults())
                .setIncludes(indexedFields)
                .setUnindexedFields(nonIndexedFields)
                .setTermFrequencyFields(this.getTermFrequencyFields())
                .setIsQueryFullySatisfied(isQueryFullySatisfied)
                .setSortedUIDs(sortedUIDs)
                .limit(documentRange)
                .disableIndexOnly(disableFiEval)
                .limit(this.sourceLimit)
                .setCollectTimingDetails(this.collectTimingDetails)
                .setQuerySpanCollector(this.querySpanCollector)
                .setIndexOnlyFields(this.getAllIndexOnlyFields())
                .setAllowTermFrequencyLookup(this.allowTermFrequencyLookup)
                .setCompositeMetadata(compositeMetadata)
                .setFiNextSeek(this.getFiNextSeek())
                .setEventNextSeek(this.getEventNextSeek())
                .setTfNextSeek(this.getTfNextSeek())
                .setUseRegexFilter(getCardinality() < getCardinalityThreshold())
                .setExceededOrEvaluationCache(exceededOrEvaluationCache);
        // @formatter:on
        // TODO: .setStatsPort(this.statsdHostAndPort);
    }

    protected GenericObjectPool<SortedKeyValueIterator<Key,Value>> createIvaratorSourcePool(int maxIvaratorSources, long maxWait) {
        return new GenericObjectPool<>(createIvaratorSourceFactory(this), createIvaratorSourcePoolConfig(maxIvaratorSources, maxWait));
    }

    private BasePoolableObjectFactory<SortedKeyValueIterator<Key,Value>> createIvaratorSourceFactory(SourceFactory<Key,Value> sourceFactory) {
        return new BasePoolableObjectFactory<SortedKeyValueIterator<Key,Value>>() {
            @Override
            public SortedKeyValueIterator<Key,Value> makeObject() throws Exception {
                return sourceFactory.getSourceDeepCopy("ivarator");
            }
        };
    }

    private GenericObjectPool.Config createIvaratorSourcePoolConfig(int maxIvaratorSources, long maxWait) {
        GenericObjectPool.Config poolConfig = new GenericObjectPool.Config();
        poolConfig.maxActive = maxIvaratorSources;
        poolConfig.maxIdle = maxIvaratorSources;
        poolConfig.minIdle = 0;
        poolConfig.whenExhaustedAction = WHEN_EXHAUSTED_BLOCK;
        poolConfig.maxWait = maxWait;
        return poolConfig;
    }

    protected String getHdfsCacheSubDirPrefix() {
        // if we have a document specific range, or a list of specific doc ids (bundled document specific range per-se), then
        // we could have multiple iterators running against this shard for this query at the same time.
        // In this case we need to differentiate between the ivarator directories being created. However this is
        // a situation we do not want to be in, so we will also display a large warning to be seen by the accumulo monitor.
        String hdfsPrefix = null;
        if (isDocumentSpecificRange(this.range)) {
            hdfsPrefix = range.getStartKey().getColumnFamily().toString().replace('\0', '_');
        }
        return hdfsPrefix;
    }

    protected SatisfactionVisitor createSatisfiabilityVisitor(boolean isQueryFullySatisfiedInitialState) {

        // determine the list of indexed fields
        Set<String> indexedFields = this.getIndexedFields();

        indexedFields.removeAll(this.getNonIndexedDataTypeMap().keySet());

        return new SatisfactionVisitor(getNonEventFields(), indexedFields, Collections.emptySet(), isQueryFullySatisfiedInitialState);
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public String getQuery() {
        return query;
    }

    /**
     * A comparator which will ensure that values with a grouping context are sorted first. This allows us to not use exhaustive matching within the jexl
     * context to determine whether the document of interest is actually a match. Matching on the grouping context one (from the Event, not from the field
     * index) means that the hit terms will also have the grouping context on the field, and have the non-normalized value.
     */
    private static class ValueComparator implements Comparator<Object> {
        final Text fi;

        public ValueComparator(Key metadata) {
            fi = (metadata == null ? new Text() : metadata.getColumnFamily());
        }

        @Override
        public int compare(Object o1, Object o2) {
            if (fi.getLength() == 0) {
                return new CompareToBuilder().append(o1, o2).toComparison();
            } else {
                boolean o1Matches = (o1 instanceof ValueTuple && (((ValueTuple) o1).first().indexOf('.') != -1));
                boolean o2Matches = (o2 instanceof ValueTuple && (((ValueTuple) o2).first().indexOf('.') != -1));
                if (o1Matches == o2Matches) {
                    return new CompareToBuilder().append(o1, o2).toComparison();
                } else if (o1Matches) {
                    return -1;
                } else {
                    return 1;
                }
            }
        }
    }

    /**
     * This can be overridden to supply a value comparator for use within the jexl context. Useful when using the HitListArithmetic which pulls back which value
     * tuples were actually hit upon.
     *
     * @param from
     *            the from tuple
     * @return A comparator for values within the jexl context.
     */
    @Override
    public Comparator<Object> getValueComparator(Tuple3<Key,Document,Map<String,Object>> from) {
        return new ValueComparator(from.second().getMetadata());
    }

    protected UniqueTransform getUniqueTransform() throws IOException {
        if (uniqueTransform == null && getUniqueFields() != null && !getUniqueFields().isEmpty()) {
            synchronized (getUniqueFields()) {
                if (uniqueTransform == null) {
                    // @formatter:off
                    uniqueTransform = new UniqueTransform.Builder()
                            .withUniqueFields(getUniqueFields())
                            .withQueryExecutionForPageTimeout(getResultTimeout())
                            .withBufferPersistThreshold(getUniqueCacheBufferSize())
                            .withIvaratorCacheDirConfigs(getIvaratorCacheDirConfigs())
                            .withHdfsSiteConfigURLs(getHdfsSiteConfigURLs())
                            .withSubDirectory(getQueryId() + "-" + getScanId())
                            .withMaxOpenFiles(getIvaratorMaxOpenFiles())
                            .withNumRetries(getIvaratorNumRetries())
                            .withPersistOptions(new FileSortedSet.PersistOptions(true, false, 0))
                            .build();
                    // @formatter:on
                }
            }
        }
        return uniqueTransform;
    }

    protected GroupingIterator getGroupingIteratorInstance(Iterator<Entry<Key,Document>> in) {
        if (groupingIterator == null && getGroupFields() != null && getGroupFields().hasGroupByFields()) {
            synchronized (getGroupFields()) {
                if (groupingIterator == null) {
                    groupingIterator = new GroupingIterator(in, MarkingFunctionsFactory.createMarkingFunctions(), getGroupFields(), this.groupFieldsBatchSize,
                                    this.yield);
                }
            }
        }
        return groupingIterator;
    }

    protected ActiveQueryLog getActiveQueryLog() {
        if (this.activeQueryLog == null) {
            this.activeQueryLog = ActiveQueryLog.getInstance(getActiveQueryLogName());
        }
        return this.activeQueryLog;
    }

    /**
     * Gets a default implementation of a FieldIndexAggregator
     *
     * @return a {@link IdentityAggregator}
     */
    @Override
    public FieldIndexAggregator getFiAggregator() {
        if (fiAggregator == null) {
            fiAggregator = new IdentityAggregator(getAllIndexOnlyFields(), getFiEvaluationFilter(), getEventNextSeek());
        }
        return fiAggregator;
    }

    protected ExcerptTransform getExcerptTransform() {
        if (excerptTransform == null && getExcerptFields() != null && !getExcerptFields().isEmpty()) {
            synchronized (getExcerptFields()) {
                if (excerptTransform == null) {
                    try {
                        SortedKeyValueIterator<Key,Value> excerptSource = getSourceDeepCopy("excerpt transform");

                        excerptTransform = new ExcerptTransform(excerptFields, myEnvironment, excerptSource,
                                        excerptIterator.getDeclaredConstructor().newInstance());
                    } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
                        throw new RuntimeException("Could not create excerpt transform", e);
                    }
                }
            }
        }
        return excerptTransform;
    }

    protected SummaryTransform getSummaryTransform() {
        if (summaryTransform == null && getSummaryOptions() != null && getSummaryOptions().getSummarySize() != 0) {
            synchronized (getSummaryOptions()) {
                if (summaryTransform == null) {
                    try {
                        summaryTransform = new SummaryTransform(summaryOptions, myEnvironment, sourceForDeepCopies.deepCopy(myEnvironment),
                                        summaryIterator.getDeclaredConstructor().newInstance());
                    } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
                        throw new RuntimeException("Could not create summary transform", e);
                    }
                }
            }
        }
        return summaryTransform;
    }

    /**
     * Get a default implementation of a {@link RangeProvider}
     *
     * @return a {@link DocumentRangeProvider}
     */
    protected RangeProvider getRangeProvider() {
        if (rangeProvider == null) {
            rangeProvider = new DocumentRangeProvider();
        }
        return rangeProvider;
    }
}
