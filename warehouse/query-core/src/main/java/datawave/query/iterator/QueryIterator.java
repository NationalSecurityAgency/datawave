package datawave.query.iterator;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.collect.UnmodifiableIterator;
import datawave.data.type.Type;
import datawave.data.type.util.NumericalEncoder;
import datawave.ingest.data.config.ingest.CompositeIngest;
import datawave.marking.MarkingFunctionsFactory;
import datawave.query.Constants;
import datawave.query.DocumentSerialization.ReturnType;
import datawave.query.attributes.AttributeKeepFilter;
import datawave.query.attributes.Document;
import datawave.query.attributes.ValueTuple;
import datawave.query.composite.CompositeMetadata;
import datawave.query.function.Aggregation;
import datawave.query.function.DataTypeAsField;
import datawave.query.function.DocumentMetadata;
import datawave.query.function.DocumentPermutation;
import datawave.query.function.DocumentProjection;
import datawave.query.function.IndexOnlyContextCreator;
import datawave.query.function.JexlContextCreator;
import datawave.query.function.JexlEvaluation;
import datawave.query.function.KeyToDocumentData;
import datawave.query.function.LimitFields;
import datawave.query.function.MaskedValueFilterFactory;
import datawave.query.function.MaskedValueFilterInterface;
import datawave.query.function.RemoveGroupingContext;
import datawave.query.function.deserializer.KryoDocumentDeserializer;
import datawave.query.function.serializer.KryoDocumentSerializer;
import datawave.query.function.serializer.ToStringDocumentSerializer;
import datawave.query.function.serializer.WritableDocumentSerializer;
import datawave.query.iterator.aggregation.DocumentData;
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
import datawave.query.jexl.DefaultArithmetic;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.StatefulArithmetic;
import datawave.query.jexl.functions.IdentityAggregator;
import datawave.query.jexl.functions.KeyAdjudicator;
import datawave.query.jexl.visitors.IteratorBuildingVisitor;
import datawave.query.jexl.visitors.SatisfactionVisitor;
import datawave.query.jexl.visitors.VariableNameVisitor;
import datawave.query.postprocessing.tf.TFFactory;
import datawave.query.predicate.EmptyDocumentFilter;
import datawave.query.statsd.QueryStatsDClient;
import datawave.query.tracking.ActiveQuery;
import datawave.query.tracking.ActiveQueryLog;
import datawave.query.transformer.GroupingTransform;
import datawave.query.transformer.UniqueTransform;
import datawave.query.util.EmptyContext;
import datawave.query.util.EntryToTuple;
import datawave.query.util.TraceIterators;
import datawave.query.util.Tuple2;
import datawave.query.util.Tuple3;
import datawave.query.util.TupleToEntry;
import datawave.query.util.TypeMetadata;
import datawave.util.StringUtils;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IterationInterruptedException;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.YieldCallback;
import org.apache.accumulo.core.iterators.YieldingKeyValueIterator;
import datawave.webservice.query.runner.Span;
import datawave.webservice.query.runner.Trace;
import org.apache.accumulo.tserver.tablet.TabletClosedException;
import org.apache.commons.jexl2.JexlArithmetic;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

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
 *
 */
public class QueryIterator extends QueryOptions implements YieldingKeyValueIterator<Key,Value>, JexlContextCreator.JexlContextValueComparator,
                SourceFactory<Key,Value>, SortedKeyValueIterator<Key,Value> {
    
    private static final Logger log = Logger.getLogger(QueryIterator.class);
    
    protected SortedKeyValueIterator<Key,Value> source;
    protected SortedKeyValueIterator<Key,Value> sourceForDeepCopies;
    protected Map<String,String> documentOptions;
    protected NestedIterator<Key> initKeySource, seekKeySource;
    protected Iterator<Entry<Key,Value>> serializedDocuments;
    protected boolean fieldIndexSatisfiesQuery = false;
    
    protected Range range;
    protected Range originalRange;
    
    protected Key key;
    protected Value value;
    protected YieldCallback<Key> yield;
    
    protected IteratorEnvironment myEnvironment;
    
    protected ASTJexlScript script = null;
    
    protected JexlEvaluation myEvaluationFunction = null;
    
    protected QuerySpan trackingSpan = null;
    
    protected QuerySpanCollector querySpanCollector = new QuerySpanCollector();
    
    protected UniqueTransform uniqueTransform = null;
    
    protected GroupingTransform groupingTransform;
    
    protected boolean groupingContextAddedByMe = false;
    
    protected TypeMetadata typeMetadataWithNonIndexed = null;
    protected TypeMetadata typeMetadata = null;
    
    protected Map<String,Object> exceededOrEvaluationCache = null;
    
    public QueryIterator() {}
    
    public QueryIterator(QueryIterator other, IteratorEnvironment env) {
        // Need to copy all members instantiated/modified during init()
        this.source = other.source.deepCopy(env);
        this.sourceForDeepCopies = source.deepCopy(env);
        this.initKeySource = other.initKeySource;
        this.seekKeySource = other.seekKeySource;
        this.myEnvironment = other.myEnvironment;
        this.myEvaluationFunction = other.myEvaluationFunction;
        this.script = other.script;
        this.documentOptions = other.documentOptions;
        this.fieldIndexSatisfiesQuery = other.fieldIndexSatisfiesQuery;
        this.groupingContextAddedByMe = other.groupingContextAddedByMe;
        this.typeMetadataWithNonIndexed = other.typeMetadataWithNonIndexed;
        this.typeMetadata = other.typeMetadata;
        this.exceededOrEvaluationCache = other.exceededOrEvaluationCache;
        this.trackingSpan = other.trackingSpan;
        // Defer to QueryOptions to re-set all of the query options
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
        
        // Parse the query
        try {
            this.script = JexlASTHelper.parseJexlQuery(this.getQuery());
            this.myEvaluationFunction = new JexlEvaluation(this.getQuery(), arithmetic);
            
        } catch (Exception e) {
            throw new IOException("Could not parse the JEXL query: '" + this.getQuery() + "'", e);
        }
        
        this.documentOptions = options;
        this.myEnvironment = env;
        
        if (gatherTimingDetails()) {
            this.trackingSpan = new MultiThreadedQuerySpan(getStatsdClient());
            this.source = new SourceTrackingIterator(trackingSpan, source);
        } else {
            this.source = source;
        }
        
        this.fiAggregator = new IdentityAggregator(getAllIndexOnlyFields(), getEvaluationFilter(), getEvaluationFilter() != null ? getEvaluationFilter()
                        .getMaxNextCount() : -1);
        
        if (isDebugMultithreadedSources()) {
            this.source = new SourceThreadTrackingIterator(this.source);
        }
        
        this.sourceForDeepCopies = this.source.deepCopy(this.myEnvironment);
        
        // update ActiveQueryLog with (potentially) updated config
        if (env != null) {
            ActiveQueryLog.setConfig(env.getConfig());
        }
    }
    
    @Override
    public boolean hasTop() {
        boolean yielded = (this.yield != null) && this.yield.hasYielded();
        boolean result = (!yielded) && (this.key != null) && (this.value != null);
        if (log.isTraceEnabled())
            log.trace("hasTop() " + result);
        return result;
    }
    
    @Override
    public void enableYielding(YieldCallback<Key> yieldCallback) {
        this.yield = yieldCallback;
    }
    
    @Override
    public void next() throws IOException {
        ActiveQueryLog.getInstance().get(getQueryId()).beginCall(this.originalRange, ActiveQuery.CallType.NEXT);
        Span s = Trace.start("QueryIterator.next()");
        if (log.isTraceEnabled()) {
            log.trace("next");
        }
        
        try {
            prepareKeyValue(s);
        } catch (Exception e) {
            handleException(e);
        } finally {
            if (null != s) {
                s.stop();
            }
            QueryStatsDClient client = getStatsdClient();
            if (client != null) {
                client.flush();
            }
            ActiveQueryLog.getInstance().get(getQueryId()).endCall(this.originalRange, ActiveQuery.CallType.NEXT);
            if (this.key == null && this.value == null) {
                // no entries to return
                ActiveQueryLog.getInstance().remove(getQueryId(), this.originalRange);
            }
        }
    }
    
    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        // preserve the original range for use with the Final Document tracking iterator because it is placed after the ResultCountingIterator
        // so the FinalDocumentTracking iterator needs the start key with the count already appended
        originalRange = range;
        ActiveQueryLog.getInstance().get(getQueryId()).beginCall(this.originalRange, ActiveQuery.CallType.SEEK);
        Span span = Trace.start("QueryIterator.seek");
        
        if (this.isIncludeGroupingContext() == false
                        && (this.query.contains("grouping:") || this.query.contains("matchesInGroup") || this.query.contains("MatchesInGroup") || this.query
                                        .contains("atomValuesMatch"))) {
            this.setIncludeGroupingContext(true);
            this.groupingContextAddedByMe = true;
        } else {
            this.groupingContextAddedByMe = false;
        }
        
        try {
            if (log.isDebugEnabled()) {
                log.debug("Seek range: " + range + " " + query);
            }
            this.range = range;
            
            // determine whether this is a teardown/rebuild range
            long resultCount = 0;
            if (!range.isStartKeyInclusive()) {
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
                if (log.isTraceEnabled())
                    log.trace("Received event specific range: " + documentRange);
                // We can take a shortcut to the directly to the event
                Map.Entry<Key,Document> documentKey = Maps.immutableEntry(super.getDocumentKey.apply(documentRange), new Document());
                if (log.isTraceEnabled())
                    log.trace("Transformed document key: " + documentKey);
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
            PipelineIterator pipelineIter = PipelineFactory.createIterator(this.seekKeySource, getMaxEvaluationPipelines(), getMaxPipelineCachedResults(),
                            getSerialPipelineRequest(), querySpanCollector, trackingSpan, this, sourceForDeepCopies.deepCopy(myEnvironment), myEnvironment,
                            yield, yieldThresholdMs);
            
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
            
            // now apply the unique transform if requested
            UniqueTransform uniquify = getUniqueTransform();
            if (uniquify != null) {
                pipelineDocuments = Iterators.filter(pipelineDocuments, uniquify.getUniquePredicate());
            }
            
            // apply the grouping transform if requested and if the batch size is greater than zero
            // if the batch size is 0, then grouping is computed only on the web server
            GroupingTransform groupify = getGroupingTransform();
            if (groupify != null && this.groupFieldsBatchSize > 0) {
                
                pipelineDocuments = groupingTransform.getGroupingIterator(pipelineDocuments, this.groupFieldsBatchSize, this.yield);
                
                if (log.isTraceEnabled()) {
                    pipelineDocuments = Iterators.filter(pipelineDocuments, keyDocumentEntry -> {
                        log.trace("after grouping, keyDocumentEntry:" + keyDocumentEntry);
                        return true;
                    });
                }
            }
            
            pipelineDocuments = Iterators.filter(
                            pipelineDocuments,
                            keyDocumentEntry -> {
                                // last chance before the documents are serialized
                                ActiveQueryLog.getInstance().get(getQueryId())
                                                .recordStats(keyDocumentEntry.getValue(), querySpanCollector.getCombinedQuerySpan(null));
                                // Always return true since we just want to record data in the ActiveQueryLog
                                return true;
                            });
            
            if (this.getReturnType() == ReturnType.kryo) {
                // Serialize the Document using Kryo
                this.serializedDocuments = Iterators.transform(pipelineDocuments, new KryoDocumentSerializer(isReducedResponse(), isCompressResults()));
            } else if (this.getReturnType() == ReturnType.writable) {
                // Use the Writable interface to serialize the Document
                this.serializedDocuments = Iterators.transform(pipelineDocuments, new WritableDocumentSerializer(isReducedResponse()));
            } else if (this.getReturnType() == ReturnType.tostring) {
                // Just return a toString() representation of the document
                this.serializedDocuments = Iterators.transform(pipelineDocuments, new ToStringDocumentSerializer(isReducedResponse()));
            } else {
                throw new IllegalArgumentException("Unknown return type of: " + this.getReturnType());
            }
            
            if (log.isTraceEnabled()) {
                KryoDocumentDeserializer dser = new KryoDocumentDeserializer();
                this.serializedDocuments = Iterators.filter(this.serializedDocuments, keyValueEntry -> {
                    log.trace("after serializing, keyValueEntry:" + dser.apply(keyValueEntry));
                    return true;
                });
            }
            
            // now add the result count to the keys (required when not sorting UIDs)
            // Cannot do this on document specific ranges as the count would place the keys outside the initial range
            if (!sortedUIDs && documentRange == null) {
                this.serializedDocuments = new ResultCountingIterator(serializedDocuments, resultCount, yield);
            } else if (this.sortedUIDs) {
                // we have sorted UIDs, so we can mask out the cq
                this.serializedDocuments = new KeyAdjudicator<>(serializedDocuments, yield);
            }
            
            // only add the final document tracking iterator which sends stats back to the client if collectTimingDetails is true
            if (collectTimingDetails) {
                // if there is no document to return, then add an empty document
                // to store the timing metadata
                this.serializedDocuments = new FinalDocumentTrackingIterator(querySpanCollector, trackingSpan, originalRange, this.serializedDocuments,
                                this.getReturnType(), this.isReducedResponse(), this.isCompressResults(), this.yield);
            }
            if (log.isTraceEnabled()) {
                KryoDocumentDeserializer dser = new KryoDocumentDeserializer();
                this.serializedDocuments = Iterators.filter(this.serializedDocuments, keyValueEntry -> {
                    log.debug("finally, considering:" + dser.apply(keyValueEntry));
                    return true;
                });
            }
            
            // Determine if we have items to return
            prepareKeyValue(span);
        } catch (Exception e) {
            handleException(e);
        } finally {
            if (gatherTimingDetails() && trackingSpan != null && querySpanCollector != null) {
                querySpanCollector.addQuerySpan(trackingSpan);
            }
            if (null != span) {
                span.stop();
            }
            QueryStatsDClient client = getStatsdClient();
            if (client != null) {
                client.flush();
            }
            ActiveQueryLog.getInstance().get(getQueryId()).endCall(this.originalRange, ActiveQuery.CallType.SEEK);
            if (this.key == null && this.value == null) {
                // no entries to return
                ActiveQueryLog.getInstance().remove(getQueryId(), this.originalRange);
            }
        }
    }
    
    /**
     * Handle an exception returned from seek or next. This will silently ignore IterationInterruptedException as that happens when the underlying iterator was
     * interrupted because the client is no longer listening.
     * 
     * @param e
     */
    private void handleException(Exception e) throws IOException {
        Throwable reason = e;
        
        // We need to pass IOException, IteratorInterruptedException, and TabletClosedExceptions up to the Tablet as they are
        // handled specially to ensure that the client will retry the scan elsewhere
        IOException ioe = null;
        IterationInterruptedException iie = null;
        TabletClosedException tce = null;
        if (reason instanceof IOException)
            ioe = (IOException) reason;
        if (reason instanceof IterationInterruptedException)
            iie = (IterationInterruptedException) reason;
        if (reason instanceof TabletClosedException)
            tce = (TabletClosedException) reason;
        
        int depth = 1;
        while (iie == null && reason.getCause() != null && reason.getCause() != reason && depth < 100) {
            reason = reason.getCause();
            if (reason instanceof IOException)
                ioe = (IOException) reason;
            if (reason instanceof IterationInterruptedException)
                iie = (IterationInterruptedException) reason;
            if (reason instanceof TabletClosedException)
                tce = (TabletClosedException) reason;
            depth++;
        }
        
        // NOTE: Only logging debug here because the Tablet/LookupTask will log the exception as a WARN if we actually have an problem here
        if (iie != null) {
            // exit gracefully if we are yielding as an iie is expected in this case
            if ((this.yield != null) && this.yield.hasYielded()) {
                log.debug("Query yielded " + queryId);
            } else {
                log.debug("Query interrupted " + queryId, e);
                throw iie;
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
     * @param seekRange
     * @param columnFamilies
     * @param inclusive
     * @return
     * @throws IOException
     */
    protected NestedIterator<Key> buildDocumentIterator(Range documentRange, Range seekRange, Collection<ByteSequence> columnFamilies, boolean inclusive)
                    throws IOException, ConfigException, InstantiationException, IllegalAccessException {
        NestedIterator<Key> docIter = null;
        if (log.isTraceEnabled())
            log.trace("Batched queries is " + batchedQueries);
        if (batchedQueries >= 1) {
            List<NestedQuery<Key>> nests = Lists.newArrayList();
            
            for (Entry<Range,String> queries : batchStack) {
                
                Range myRange = queries.getKey();
                
                if (log.isTraceEnabled())
                    log.trace("Adding " + myRange + " from seekrange " + seekRange);
                
                /*
                 * Only perform the following checks if start key is not infinite and document range is specified
                 */
                if (null != seekRange && !seekRange.isInfiniteStartKey()) {
                    Key seekStartKey = seekRange.getStartKey();
                    Key myStartKey = myRange.getStartKey();
                    
                    /*
                     * if our seek key is greater than our start key we can skip this batched query. myStartKey.compareTo(seekStartKey) must be <= 0, which
                     * means that startKey must be greater than or equal be seekStartKey
                     */
                    if (null != myStartKey && null != seekStartKey && !seekRange.contains(myStartKey)) {
                        
                        if (log.isTraceEnabled()) {
                            log.trace("skipping " + myRange);
                        }
                        
                        continue;
                    }
                }
                
                JexlArithmetic myArithmetic;
                if (arithmetic instanceof StatefulArithmetic) {
                    myArithmetic = ((StatefulArithmetic) arithmetic).clone();
                } else {
                    myArithmetic = new DefaultArithmetic();
                }
                
                // Parse the query
                ASTJexlScript myScript = null;
                JexlEvaluation eval = null;
                try {
                    
                    myScript = JexlASTHelper.parseJexlQuery(queries.getValue());
                    eval = new JexlEvaluation(queries.getValue(), myArithmetic);
                    
                } catch (Exception e) {
                    throw new IOException("Could not parse the JEXL query: '" + this.getQuery() + "'", e);
                }
                // If we had an event-specific range previously, we need to
                // reset it back
                // to the source we created during init
                NestedIterator<Key> subDocIter = getOrSetKeySource(myRange, myScript);
                
                if (log.isTraceEnabled()) {
                    log.trace("Using init()'ialized source: " + subDocIter.getClass().getName());
                }
                
                if (gatherTimingDetails()) {
                    subDocIter = new EvaluationTrackingNestedIterator(QuerySpan.Stage.FieldIndexTree, trackingSpan, subDocIter, myEnvironment);
                }
                
                // Seek() the boolean logic stuff
                ((SeekableIterator) subDocIter).seek(myRange, columnFamilies, inclusive);
                
                NestedQuery<Key> nestedQueryObj = new NestedQuery<>();
                nestedQueryObj.setQuery(queries.getValue());
                nestedQueryObj.setIterator(subDocIter);
                nestedQueryObj.setQueryScript(myScript);
                nestedQueryObj.setEvaluation(eval);
                nestedQueryObj.setRange(queries.getKey());
                nests.add(nestedQueryObj);
            }
            
            docIter = new NestedQueryIterator<>(nests);
            
            // now lets start off the nested iterator
            docIter.initialize();
            
            initKeySource = docIter;
            
        } else {
            // If we had an event-specific range previously, we need to reset it back
            // to the source we created during init
            docIter = getOrSetKeySource(documentRange, script);
            
            initKeySource = docIter;
            
            if (log.isTraceEnabled()) {
                log.trace("Using init()'ialized source: " + this.initKeySource.getClass().getName());
            }
            
            if (gatherTimingDetails()) {
                docIter = new EvaluationTrackingNestedIterator(QuerySpan.Stage.FieldIndexTree, trackingSpan, docIter, myEnvironment);
            }
            
            // Seek() the boolean logic stuff
            ((SeekableIterator) docIter).seek(range, columnFamilies, inclusive);
            
            // now lets start off the nested iterator
            docIter.initialize();
        }
        
        return docIter;
    }
    
    /**
     * There was a request to create a serial pipeline. The factory may not choose to honor this.
     * 
     * @return
     */
    private boolean getSerialPipelineRequest() {
        return serialEvaluationPipeline;
    }
    
    /**
     * A routine which should always be used to create deep copies of the source. This ensures that we are thread safe when doing these copies.
     * 
     * @return
     */
    public SortedKeyValueIterator<Key,Value> getSourceDeepCopy() {
        SortedKeyValueIterator<Key,Value> sourceDeepCopy = null;
        sourceDeepCopy = sourceForDeepCopies.deepCopy(this.myEnvironment);
        return sourceDeepCopy;
    }
    
    /**
     * Returns the elements of {@code unfiltered} that satisfy a predicate. This is used instead of the google commons Iterators.filter to create a non-stateful
     * filtering iterator.
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
     * @param documentSpecificSource
     * @return iterator of keys and values
     */
    public Iterator<Entry<Key,Document>> createDocumentPipeline(SortedKeyValueIterator<Key,Value> deepSourceCopy,
                    final NestedQueryIterator<Key> documentSpecificSource, QuerySpanCollector querySpanCollector) {
        
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
            docMapper = new Function<Entry<Key,Document>,Entry<DocumentData,Document>>() {
                @Nullable
                @Override
                public Entry<DocumentData,Document> apply(@Nullable Entry<Key,Document> input) {
                    
                    Entry<DocumentData,Document> entry = null;
                    if (input != null) {
                        entry = Maps.immutableEntry(new DocumentData(input.getKey(), Collections.singleton(input.getKey()), Collections.EMPTY_LIST),
                                        input.getValue());
                    }
                    return entry;
                }
            };
        } else {
            docMapper = new KeyToDocumentData(deepSourceCopy, myEnvironment, documentOptions, super.equality, getEvaluationFilter(),
                            this.includeHierarchyFields, this.includeHierarchyFields);
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
                        this.includeRecordId, this.disableIndexOnlyDocuments(), getEvaluationFilter(), isTrackSizes());
        if (gatherTimingDetails()) {
            documents = Iterators.transform(sourceIterator, new EvaluationTrackingFunction<>(QuerySpan.Stage.Aggregation, trackingSpan, a));
        } else {
            documents = Iterators.transform(sourceIterator, a);
        }
        
        // Inject the data type as a field if the user requested it
        if (this.includeDatatype) {
            if (gatherTimingDetails()) {
                documents = Iterators.transform(documents, new EvaluationTrackingFunction<>(QuerySpan.Stage.DataTypeAsField, trackingSpan, new DataTypeAsField(
                                this.datatypeKey)));
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
        
        if (gatherTimingDetails()) {
            documents = new EvaluationTrackingIterator(QuerySpan.Stage.DocumentEvaluation, trackingSpan, getEvaluation(documentSpecificSource, deepSourceCopy,
                            documents, compositeMetadata, typeMetadataWithNonIndexed));
        } else {
            documents = getEvaluation(documentSpecificSource, deepSourceCopy, documents, compositeMetadata, typeMetadataWithNonIndexed);
        }
        
        // a hook to allow mapping the document such as with the TLD or Parent
        // query logics
        // or if the document was not aggregated in the first place because the
        // field index fields completely satisfied the query
        documents = mapDocument(deepSourceCopy, documents, compositeMetadata);
        
        // apply any configured post processing
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
            documents = Iterators.transform(documents, new EvaluationTrackingFunction<>(QuerySpan.Stage.AttributeKeepFilter, trackingSpan,
                            new AttributeKeepFilter<>()));
        } else {
            documents = Iterators.transform(documents, new AttributeKeepFilter<>());
        }
        
        // Project fields using a whitelist or a blacklist before serialization
        if (this.projectResults) {
            if (gatherTimingDetails()) {
                documents = Iterators.transform(documents, new EvaluationTrackingFunction<>(QuerySpan.Stage.DocumentProjection, trackingSpan, getProjection()));
            } else {
                documents = Iterators.transform(documents, getProjection());
            }
        }
        
        // remove the composite entries
        documents = Iterators.transform(documents, this.getCompositeProjection());
        
        // Filter out any Documents which are empty (e.g. due to attribute
        // projection or visibility filtering)
        if (gatherTimingDetails()) {
            documents = statelessFilter(documents, new EvaluationTrackingPredicate<>(QuerySpan.Stage.EmptyDocumentFilter, trackingSpan,
                            new EmptyDocumentFilter()));
            documents = Iterators
                            .transform(documents, new EvaluationTrackingFunction<>(QuerySpan.Stage.DocumentMetadata, trackingSpan, new DocumentMetadata()));
        } else {
            documents = statelessFilter(documents, new EmptyDocumentFilter());
            documents = Iterators.transform(documents, new DocumentMetadata());
        }
        
        if (!this.limitFieldsMap.isEmpty()) {
            if (gatherTimingDetails()) {
                documents = Iterators.transform(documents,
                                new EvaluationTrackingFunction<>(QuerySpan.Stage.LimitFields, trackingSpan, new LimitFields(this.getLimitFieldsMap())));
            } else {
                documents = Iterators.transform(documents, new LimitFields(this.getLimitFieldsMap()));
            }
        }
        
        // do I need to remove the grouping context I added above?
        if (groupingContextAddedByMe) {
            if (gatherTimingDetails()) {
                documents = Iterators.transform(documents, new EvaluationTrackingFunction<>(QuerySpan.Stage.RemoveGroupingContext, trackingSpan,
                                new RemoveGroupingContext()));
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
                    CompositeMetadata compositeMetadata, TypeMetadata typeMetadataForEval) {
        return getEvaluation(null, sourceDeepCopy, documents, compositeMetadata, typeMetadataForEval);
    }
    
    protected Iterator<Entry<Key,Document>> getEvaluation(NestedQueryIterator<Key> documentSource, SortedKeyValueIterator<Key,Value> sourceDeepCopy,
                    Iterator<Entry<Key,Document>> documents, CompositeMetadata compositeMetadata, TypeMetadata typeMetadataForEval) {
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
            if (this.isTermFrequenciesRequired()) {
                Function<Tuple2<Key,Document>,Tuple3<Key,Document,Map<String,Object>>> tfFunction;
                tfFunction = TFFactory.getFunction(getScript(documentSource), getContentExpansionFields(), getTermFrequencyFields(), this.getTypeMetadata(),
                                super.equality, getEvaluationFilter(), sourceDeepCopy.deepCopy(myEnvironment));
                
                itrWithContext = TraceIterators.transform(tupleItr, tfFunction, "Term Frequency Lookup");
            } else {
                itrWithContext = Iterators.transform(tupleItr, new EmptyContext<>());
            }
            
            final IndexOnlyContextCreator contextCreator = new IndexOnlyContextCreator(sourceDeepCopy, getDocumentRange(documentSource), typeMetadataForEval,
                            compositeMetadata, this, variables, QueryIterator.this);
            
            if (exceededOrEvaluationCache != null && !exceededOrEvaluationCache.isEmpty())
                contextCreator.addAdditionalEntries(exceededOrEvaluationCache);
            
            final Iterator<Tuple3<Key,Document,DatawaveJexlContext>> itrWithDatawaveJexlContext = Iterators.transform(itrWithContext, contextCreator);
            Iterator<Tuple3<Key,Document,DatawaveJexlContext>> matchedDocuments = statelessFilter(itrWithDatawaveJexlContext, jexlEvaluationFunction);
            if (log.isTraceEnabled()) {
                log.trace("arithmetic:" + arithmetic + " range:" + getDocumentRange(documentSource) + ", thread:" + Thread.currentThread());
            }
            return Iterators.transform(matchedDocuments, new TupleToEntry<>());
        } else if (log.isTraceEnabled()) {
            log.trace("Evaluation is disabled, not instantiating Jexl evaluation logic");
        }
        return documents;
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
        
        if (null == documentSource) {
            return new JexlEvaluation(query, getArithmetic());
        }
        JexlEvaluation jexlEvaluationFunction = null;
        NestedQuery<Key> nestedQuery = documentSource.getNestedQuery();
        if (null == nestedQuery) {
            jexlEvaluationFunction = new JexlEvaluation(query, getArithmetic());
        } else {
            jexlEvaluationFunction = nestedQuery.getEvaluation();
            if (null == jexlEvaluationFunction) {
                return new JexlEvaluation(query, getArithmetic());
            }
        }
        return jexlEvaluationFunction;
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
            return script;
        }
        NestedQuery<Key> query = documentSource.getNestedQuery();
        if (null == query) {
            return script;
        } else {
            ASTJexlScript rangeScript = query.getScript();
            if (null == rangeScript) {
                return script;
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
            final KeyToDocumentData docMapper = new KeyToDocumentData(deepSourceCopy, this.myEnvironment, this.documentOptions, super.equality,
                            getEvaluationFilter(), this.includeHierarchyFields, this.includeHierarchyFields);
            Iterator<Tuple2<Key,Document>> mappedDocuments = Iterators.transform(
                            documents,
                            new GetDocument(docMapper, new Aggregation(this.getTimeFilter(), typeMetadataWithNonIndexed, compositeMetadata, this
                                            .isIncludeGroupingContext(), this.includeRecordId, this.disableIndexOnlyDocuments(), getEvaluationFilter(),
                                            isTrackSizes())));
            
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
    
    private void prepareKeyValue(Span span) {
        if (this.serializedDocuments.hasNext()) {
            Entry<Key,Value> entry = this.serializedDocuments.next();
            
            if (log.isTraceEnabled()) {
                log.trace("next() returned " + entry);
            }
            
            this.key = entry.getKey();
            this.value = entry.getValue();
            
            if (Trace.isTracing()) {
                span.data("Key", rowColFamToString(this.key));
            }
        } else {
            if (log.isTraceEnabled()) {
                log.trace("Exhausted all keys");
            }
            this.key = null;
            this.value = null;
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
        DocumentProjection projection = new DocumentProjection(this.isIncludeGroupingContext(), this.isReducedResponse(), isTrackSizes());
        
        if (this.useWhiteListedFields) {
            projection.initializeWhitelist(this.whiteListedFields);
            return projection;
        } else if (this.useBlackListedFields) {
            projection.initializeBlacklist(this.blackListedFields);
            return projection;
        } else {
            String msg = "Configured to use projection, but no whitelist or blacklist was provided";
            log.error(msg);
            throw new IllegalArgumentException(msg);
        }
    }
    
    protected DocumentProjection getCompositeProjection() {
        DocumentProjection projection = new DocumentProjection(this.isIncludeGroupingContext(), this.isReducedResponse(), isTrackSizes());
        Set<String> composites = Sets.newHashSet();
        if (compositeMetadata != null) {
            for (Multimap<String,String> val : this.compositeMetadata.getCompositeFieldMapByType().values())
                for (String compositeField : val.keySet())
                    if (!CompositeIngest.isOverloadedCompositeField(val, compositeField))
                        composites.add(compositeField);
        }
        projection.initializeBlacklist(composites);
        return projection;
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
    
    protected NestedIterator<Key> getOrSetKeySource(final Range documentRange, ASTJexlScript rangeScript) throws IOException, ConfigException,
                    IllegalAccessException, InstantiationException {
        NestedIterator<Key> sourceIter = null;
        // If we're doing field index or a non-fulltable (aka a normal
        // query)
        if (!this.isFullTableScanOnly()) {
            
            boolean isQueryFullySatisfiedInitialState = batchedQueries <= 0;
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
                    boolean isQueryFullySatisfied, boolean sortedUIDs) throws MalformedURLException, ConfigException, IllegalAccessException,
                    InstantiationException {
        if (log.isTraceEnabled()) {
            log.trace(documentRange);
        }
        
        // determine the list of indexed fields
        Set<String> indexedFields = this.getIndexedFields();
        indexedFields.removeAll(this.getNonIndexedDataTypeMap().keySet());
        
        // @formatter:off
        return c.newInstance()
                .setSource(this, this.myEnvironment)
                .setTimeFilter(this.getTimeFilter())
                .setTypeMetadata(this.getTypeMetadata())
                .setFieldsToAggregate(this.getNonEventFields())
                .setAttrFilter(this.getEvaluationFilter())
                .setDatatypeFilter(this.getFieldIndexKeyDataTypeFilter())
                .setFiAggregator(this.fiAggregator)
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
                .setIvaratorSources(this, this.getMaxIvaratorSources())
                .setMaxIvaratorResults(this.getMaxIvaratorResults())
                .setIncludes(indexedFields)
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
                .setExceededOrEvaluationCache(exceededOrEvaluationCache);
        // @formatter:on
        // TODO: .setStatsPort(this.statsdHostAndPort);
    }
    
    protected String getHdfsCacheSubDirPrefix() {
        // if we have a document specific range, or a list of specific doc ids (bundled document specific range per-se), then
        // we could have multiple iterators running against this shard for this query at the same time.
        // In this case we need to differentiate between the ivarator directories being created. However this is
        // a situation we do not want to be in, so we will also display a large warning to be seen by the accumulo monitor.
        String hdfsPrefix = null;
        if (isDocumentSpecificRange(this.range)) {
            hdfsPrefix = range.getStartKey().getColumnFamily().toString().replace('\0', '_');
        } else if (batchedQueries > 0) {
            StringBuilder sb = new StringBuilder();
            for (Entry<Range,String> queries : batchStack) {
                if (sb.length() > 0) {
                    sb.append('-');
                }
                sb.append(queries.getKey().getStartKey().getColumnFamily().toString().replace('\0', '_'));
            }
            hdfsPrefix = sb.toString();
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
     * @return A comparator for values within the jexl context.
     */
    @Override
    public Comparator<Object> getValueComparator(Tuple3<Key,Document,Map<String,Object>> from) {
        return new ValueComparator(from.second().getMetadata());
    }
    
    protected UniqueTransform getUniqueTransform() {
        if (uniqueTransform == null && getUniqueFields() != null & !getUniqueFields().isEmpty()) {
            synchronized (getUniqueFields()) {
                if (uniqueTransform == null) {
                    uniqueTransform = new UniqueTransform(getUniqueFields());
                }
            }
        }
        return uniqueTransform;
    }
    
    protected GroupingTransform getGroupingTransform() {
        if (groupingTransform == null && getGroupFields() != null & !getGroupFields().isEmpty()) {
            synchronized (getGroupFields()) {
                if (groupingTransform == null) {
                    groupingTransform = new GroupingTransform(null, getGroupFields(), true);
                    groupingTransform.initialize(null, MarkingFunctionsFactory.createMarkingFunctions());
                }
            }
        }
        return groupingTransform;
    }
}
