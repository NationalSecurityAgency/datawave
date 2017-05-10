package datawave.query.rewrite.iterator;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.*;
import java.net.MalformedURLException;
import datawave.data.type.Type;
import datawave.data.type.util.NumericalEncoder;
import datawave.query.jexl.DatawaveJexlContext;
import datawave.query.rewrite.Constants;
import datawave.query.rewrite.DocumentSerialization.ReturnType;
import datawave.query.rewrite.attributes.AttributeKeepFilter;
import datawave.query.rewrite.attributes.Document;
import datawave.query.rewrite.attributes.ValueTuple;
import datawave.query.rewrite.function.*;
import datawave.query.rewrite.function.serializer.KryoDocumentSerializer;
import datawave.query.rewrite.function.serializer.ToStringDocumentSerializer;
import datawave.query.rewrite.function.serializer.WritableDocumentSerializer;
import datawave.query.rewrite.iterator.aggregation.DocumentData;
import datawave.query.rewrite.iterator.pipeline.PipelineFactory;
import datawave.query.rewrite.iterator.pipeline.PipelineIterator;
import datawave.query.rewrite.iterator.profile.*;
import datawave.query.rewrite.jexl.DefaultArithmetic;
import datawave.query.rewrite.jexl.JexlASTHelper;
import datawave.query.rewrite.jexl.StatefulArithmetic;
import datawave.query.rewrite.jexl.functions.IdentityAggregator;
import datawave.query.rewrite.jexl.functions.KeyAdjudicator;
import datawave.query.rewrite.jexl.visitors.IteratorBuildingVisitor;
import datawave.query.rewrite.jexl.visitors.SatisfactionVisitor;
import datawave.query.rewrite.jexl.visitors.VariableNameVisitor;
import datawave.query.rewrite.postprocessing.tf.TFFactory;
import datawave.query.rewrite.predicate.EmptyDocumentFilter;
import datawave.query.rewrite.predicate.EventDataQueryFilter;
import datawave.query.rewrite.predicate.FilteredDocumentData;
import datawave.query.rewrite.util.TraceIterators;
import datawave.query.util.*;
import datawave.util.StringUtils;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IterationInterruptedException;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.trace.instrument.Span;
import org.apache.accumulo.trace.instrument.Trace;
import org.apache.commons.jexl2.JexlArithmetic;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;

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
public class QueryIterator extends QueryOptions implements SortedKeyValueIterator<Key,Value>, JexlContextCreator.JexlContextValueComparator,
                SourceFactory<Key,Value> {
    
    private static final Logger log = Logger.getLogger(QueryIterator.class);
    
    protected SortedKeyValueIterator<Key,Value> source;
    protected SortedKeyValueIterator<Key,Value> sourceForDeepCopies;
    protected Map<String,String> documentOptions;
    protected NestedIterator<Key> initKeySource, seekKeySource;
    // protected AccumuloTreeIterable<Key,DocumentData> initKeySource;
    // protected AccumuloTreeIterable<Key,DocumentData> seekKeySource;
    protected Iterator<Entry<Key,Value>> serializedDocuments;
    protected boolean fieldIndexSatisfiesQuery = false;
    
    protected Range range;
    
    protected Key key;
    protected Value value;
    
    protected IteratorEnvironment myEnvironment;
    
    protected ASTJexlScript script = null;
    
    protected JexlEvaluation myEvaluationFunction = null;
    
    protected QuerySpan trackingSpan = null;
    
    protected QuerySpanCollector querySpanCollector = new QuerySpanCollector();
    
    protected boolean groupingContextAddedByMe = false;
    
    protected TypeMetadata typeMetadataWithNonIndexed = null;
    protected TypeMetadata typeMetadata = null;
    
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
        
        if (!validateOptions(new SourcedOptions<String,String>(source, env, options))) {
            throw new IllegalArgumentException("Could not initialize QueryIterator with " + options.toString());
        }
        
        // We want to add in spoofed dataTypes for Aggregation/Evaluation to
        // ensure proper numeric evaluation.
        this.typeMetadata = new TypeMetadata(this.getTypeMetadata());
        typeMetadataWithNonIndexed = new TypeMetadata(this.typeMetadata);
        typeMetadataWithNonIndexed.addForAllIngestTypes(this.getNonIndexedDataTypeMap());
        
        // Parse the query
        try {
            
            script = JexlASTHelper.parseJexlQuery(this.getQuery());
            myEvaluationFunction = new JexlEvaluation(this.getQuery(), arithmetic);
            
        } catch (Exception e) {
            throw new IOException("Could not parse the JEXL query: '" + this.getQuery() + "'", e);
        }
        
        this.documentOptions = options;
        this.myEnvironment = env;
        
        if (gatherTimingDetails()) {
            trackingSpan = new MultiThreadedQuerySpan(getStatsdClient());
            this.source = new SourceTrackingIterator(trackingSpan, source);
        } else {
            this.source = source;
        }
        
        this.fiAggregator = new IdentityAggregator(indexOnlyFields, this.getCompositeMetadata().keySet(), getEvaluationFilter());
        
        if (isDebugMultithreadedSources()) {
            this.source = new SourceThreadTrackingIterator(this.source);
        }
        
        this.sourceForDeepCopies = this.source.deepCopy(this.myEnvironment);
    }
    
    @Override
    public boolean hasTop() {
        
        boolean result = this.key != null && this.value != null;
        if (log.isTraceEnabled())
            log.trace("hasTop() " + result);
        return result;
    }
    
    @Override
    public void next() throws IOException {
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
        }
    }
    
    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
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
            boolean reseek = false;
            long resultCount = 0;
            if (!range.isStartKeyInclusive()) {
                // see if we have a count in the cf
                Key startKey = range.getStartKey();
                String[] parts = StringUtils.split(startKey.getColumnFamily().toString(), '\0');
                if (parts.length == 3) {
                    reseek = true;
                    resultCount = NumericalEncoder.decode(parts[0]).longValue();
                    // remove the count from the range
                    startKey = new Key(startKey.getRow(), new Text(parts[1] + '\0' + parts[2]), startKey.getColumnQualifier(), startKey.getColumnVisibility(),
                                    startKey.getTimestamp());
                    this.range = range = new Range(startKey, range.isStartKeyInclusive(), range.getEndKey(), range.isEndKeyInclusive());
                }
            }
            
            // determine whether this is a document specific range
            Range documentRange = isDocumentSpecificRange(range) ? range : null;
            
            /**
             * to determine whether or not we need to use the tf, and subsequently whether or not we must use an AccumuloTreeIteratable vice a
             * DocumentSpecificTreeIterable, we check to see first if we have any tf fields, and the we check that tfFunction is not an empty tf function. This
             * tells us that we have no content functions
             */
            boolean requiresTermFrequencies = !this.getTermFrequencyFields().isEmpty();
            // if we have a document specific range, but the key is not
            // inclusive then we have already returned the document; this scan
            // is done
            if (documentRange != null && !documentRange.isStartKeyInclusive()) {
                if (log.isTraceEnabled()) {
                    log.trace("Received non-inclusive event specific range: " + documentRange);
                }
                if (gatherTimingDetails()) {
                    this.seekKeySource = new EvaluationTrackingNestedIterator(QuerySpan.Stage.EmptyTree, trackingSpan, new EmptyTreeIterable());
                } else {
                    this.seekKeySource = new EmptyTreeIterable();
                }
            }
            
            // if the Range is for a single document and the query doesn't
            // reference any index-only or tokenized fields
            // (termFrequencyFields)
            else if (documentRange != null && (!this.isContainsIndexOnlyTerms() && !requiresTermFrequencies && !super.mustUseFieldIndex)) {
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
                                    new DocumentSpecificNestedIterator(documentKey));
                } else {
                    this.seekKeySource = new DocumentSpecificNestedIterator(documentKey);
                }
            } else {
                
                this.seekKeySource = buildDocumentIterator(documentRange, range, columnFamilies, inclusive);
            }
            
            // Create the pipeline iterator for document aggregation and
            // evaluation within a thread pool
            PipelineIterator pipelineIter = PipelineFactory.createIterator(this.seekKeySource, getMaxEvaluationPipelines(), getMaxPipelineCachedResults(),
                            getSerialPipelineRequest(), querySpanCollector, trackingSpan, this, sourceForDeepCopies.deepCopy(myEnvironment), myEnvironment);
            
            pipelineIter.setCollectTimingDetails(collectTimingDetails);
            // TODO pipelineIter.setStatsdHostAndPort(statsdHostAndPort);
            
            pipelineIter.startPipeline();
            
            this.serializedDocuments = pipelineIter;
            
            // now add the result count to the keys (required when not sorting UIDs)
            // Cannot do this on document specific ranges as the count would place the keys outside the initial range
            if (!sortedUIDs && documentRange == null) {
                this.serializedDocuments = new ResultCountingIterator(serializedDocuments, resultCount);
            }
            
            // only add the final document tracking iterator which sends stats back to the client if collectTimingDetails is true
            if (collectTimingDetails) {
                Range r = (documentRange == null) ? this.range : documentRange;
                // if there is no document to return, then add an empty document
                // to store the timing metadata
                this.serializedDocuments = new FinalDocumentTrackingIterator(querySpanCollector, trackingSpan, r, this.serializedDocuments,
                                this.getReturnType(), this.isReducedResponse(), this.isCompressResults());
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
        }
    }
    
    /**
     * Handle an exception returned from seek or next. This will silently ignore IterationInterruptedException as that happens when the underlying iterator was
     * interrupted because the client is no longer listening.
     * 
     * @param e
     */
    private void handleException(Exception e) {
        // if this is an IterationInterruptedException, then we can ignore and return silently
        Throwable reason = e;
        int depth = 1;
        while (reason.getCause() != null && reason.getCause() != reason && depth < 100) {
            reason = reason.getCause();
            depth++;
        }
        if (!(reason instanceof IterationInterruptedException)) {
            log.error("Failure for query " + queryId, e);
            throw new RuntimeException("Failure for query " + queryId + " " + query, e);
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
                    throws IOException, ConfigException {
        NestedIterator<Key> docIter = null;
        if (log.isTraceEnabled())
            log.trace("Batched queries is " + batchedQueries);
        if (batchedQueries >= 1) {
            List<NestedQuery<Key>> nests = Lists.newArrayList();
            
            int i = 0;
            for (Entry<Range,String> queries : batchStack) {
                
                Range myRange = queries.getKey();
                
                if (log.isTraceEnabled())
                    log.trace("Adding " + myRange + " from seekrange " + seekRange);
                
                /**
                 * Only perform the following checks if start key is not infinite and document range is specified
                 */
                if (null != seekRange && !seekRange.isInfiniteStartKey()) {
                    Key seekStartKey = seekRange.getStartKey();
                    Key myStartKey = myRange.getStartKey();
                    
                    /*
                     * if our seek key is greater than our start key we can skip this batched query. myStartKey.compareto(seekstartKey) must be <= 0, which
                     * means that startkey must be greater than or equal be seekstartkey
                     */
                    if (null != myStartKey && null != seekStartKey && !seekRange.contains(myStartKey)) {
                        
                        if (log.isTraceEnabled()) {
                            log.trace("skipping " + myRange);
                        }
                        
                        continue;
                    }
                    
                }
                
                JexlArithmetic myArithmetic = this.arithmetic;
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
                    subDocIter = new EvaluationTrackingNestedIterator(QuerySpan.Stage.FieldIndexTree, trackingSpan, subDocIter);
                }
                
                // Seek() the boolean logic stuff
                ((SeekableIterator) subDocIter).seek(myRange, columnFamilies, inclusive);
                
                NestedQuery<Key> nestedQueryObj = new NestedQuery<Key>();
                nestedQueryObj.setQuery(queries.getValue());
                nestedQueryObj.setIterator(subDocIter);
                nestedQueryObj.setQueryScript(myScript);
                nestedQueryObj.setEvaluation(eval);
                nestedQueryObj.setRange(queries.getKey());
                nests.add(nestedQueryObj);
            }
            
            docIter = new NestedQueryIterator<Key>(nests);
            
            // now lets start off the nested iterator
            docIter.initialize();
            
            initKeySource = docIter;
            
        } else {
            // If we had an event-specific range previously, we need to
            // reset it back
            // to the source we created during init
            docIter = getOrSetKeySource(documentRange, script);
            
            initKeySource = docIter;
            
            if (log.isTraceEnabled()) {
                log.trace("Using init()'ialized source: " + this.initKeySource.getClass().getName());
            }
            
            if (gatherTimingDetails()) {
                docIter = new EvaluationTrackingNestedIterator(QuerySpan.Stage.FieldIndexTree, trackingSpan, docIter);
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
     * Returns the elements of {@code unfiltered} that satisfy a predicate. This is used instead of the google commons Iterators.filter to create a
     * non-stateless filtering iterator.
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
    public Iterator<Entry<Key,Value>> createDocumentPipeline(SortedKeyValueIterator<Key,Value> deepSourceCopy,
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
            if (projectResultsAndTrim) {
                EventDataQueryFilter project = new EventDataQueryFilter();
                project.initializeWhitelist(getProjection().getProjection().getWhitelist());
                docMapper = new FilteredDocumentData(deepSourceCopy, super.equality, project, this.includeHierarchyFields, this.includeHierarchyFields);
            } else {
                
                docMapper = new KeyToDocumentData(deepSourceCopy, myEnvironment, documentOptions, super.equality, getEvaluationFilter(),
                                this.includeHierarchyFields, this.includeHierarchyFields);
            }
        }
        
        Iterator<Entry<DocumentData,Document>> sourceIterator = Iterators.transform(documentSpecificSource, new Function<Key,Entry<DocumentData,Document>>() {
            
            @Override
            public Entry<DocumentData,Document> apply(Key from) {
                Entry<Key,Document> entry = Maps.immutableEntry(from, documentSpecificSource.document());
                return docMapper.apply(entry);
            }
            
        });
        
        // Take the document Keys and transform it into Entry<Key,Document>,
        // removing Attributes for this Document
        // which do not fall within the expected time range
        Iterator<Entry<Key,Document>> documents = null;
        CompositeMetadata compositeMetadata = new CompositeMetadata(this.getCompositeMetadata());
        Aggregation a = new Aggregation(this.getTimeFilter(), this.typeMetadataWithNonIndexed, compositeMetadata, this.isIncludeGroupingContext(),
                        this.disableIndexOnlyDocuments(), this.getEvaluationFilter());
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
                            new AttributeKeepFilter<Key>()));
        } else {
            documents = Iterators.transform(documents, new AttributeKeepFilter<Key>());
        }
        
        // Project fields using a whitelist or a blacklist before serialization
        if (this.projectResults) {
            if (gatherTimingDetails()) {
                documents = Iterators.transform(documents, new EvaluationTrackingFunction<>(QuerySpan.Stage.DocumentProjection, trackingSpan, getProjection()));
            } else {
                documents = Iterators.transform(documents, getProjection());
            }
        }
        
        documents = Iterators.transform(documents, this.getCompositeProjection());
        
        // Filter out any Documents which are empty (e.g. due to attribute
        // projection or visibility filtering)
        if (gatherTimingDetails()) {
            documents = statelessFilter(documents, new EvaluationTrackingPredicate<>(QuerySpan.Stage.EmptyDocumentFilter, trackingSpan,
                            new EmptyDocumentFilter()));
            // if the UIDs are sorted, then we can mask out the CQ using the KeyAdjudicator
            if (this.sortedUIDs) {
                documents = Iterators.transform(documents, new EvaluationTrackingFunction<>(QuerySpan.Stage.KeyAdjudicator, trackingSpan,
                                new KeyAdjudicator<Document>()));
            }
            documents = Iterators
                            .transform(documents, new EvaluationTrackingFunction<>(QuerySpan.Stage.DocumentMetadata, trackingSpan, new DocumentMetadata()));
        } else {
            documents = statelessFilter(documents, new EmptyDocumentFilter());
            // if the UIDs are sorted, then we can mask out the CQ using the KeyAdjudicator
            if (this.sortedUIDs) {
                documents = Iterators.transform(documents, new KeyAdjudicator<Document>());
            }
            documents = Iterators.transform(documents, new DocumentMetadata());
        }
        
        if (this.limitFieldsMap.size() > 0) {
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
        
        if (this.getReturnType() == ReturnType.kryo) {
            // Serialize the Document using Kryo
            return Iterators.transform(documents, new KryoDocumentSerializer(isReducedResponse(), isCompressResults()));
        } else if (this.getReturnType() == ReturnType.writable) {
            // Use the Writable interface to serialize the Document
            return Iterators.transform(documents, new WritableDocumentSerializer(isReducedResponse()));
        } else if (this.getReturnType() == ReturnType.tostring) {
            // Just return a toString() representation of the document
            return Iterators.transform(documents, new ToStringDocumentSerializer(isReducedResponse()));
        } else {
            throw new IllegalArgumentException("Unknown return type of: " + this.getReturnType());
        }
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
            
            final Iterator<Tuple2<Key,Document>> tupleItr = Iterators.transform(documents, new EntryToTuple<Key,Document>());
            
            // get the function we use for the tf functionality. Note we are
            // getting an additional source deep copy for this function
            Function<Tuple2<Key,Document>,Tuple3<Key,Document,Map<String,Object>>> tfFunction;
            tfFunction = TFFactory.getFunction(getScript(documentSource), getContentExpansionFields(), getTermFrequencyFields(), this.getTypeMetadata(),
                            super.equality, getEvaluationFilter(), sourceDeepCopy.deepCopy(myEnvironment));
            
            final Iterator<Tuple3<Key,Document,Map<String,Object>>> itrWithContext = TraceIterators.transform(tupleItr, tfFunction, "Term Frequency Lookup");
            
            final IndexOnlyContextCreator contextCreator = new IndexOnlyContextCreator(sourceDeepCopy, getDocumentRange(documentSource), typeMetadataForEval,
                            compositeMetadata, this, variables, QueryIterator.this);
            
            final Iterator<Tuple3<Key,Document,DatawaveJexlContext>> itrWithDatawaveJexlContext = Iterators.transform(itrWithContext, contextCreator);
            Iterator<Tuple3<Key,Document,DatawaveJexlContext>> matchedDocuments = statelessFilter(itrWithDatawaveJexlContext, jexlEvaluationFunction);
            if (log.isTraceEnabled()) {
                log.trace("arithmetic:" + arithmetic + " range:" + getDocumentRange(documentSource) + ", thread:" + Thread.currentThread());
            }
            return Iterators.transform(matchedDocuments, new TupleToEntry<Key,Document>());
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
            final KeyToDocumentData docMapper;
            // TODO: no evaluation filter here as this is post evaluation
            if (projectResultsAndTrim) {
                EventDataQueryFilter project = new EventDataQueryFilter();
                project.initializeWhitelist(getProjection().getProjection().getWhitelist());
                docMapper = new FilteredDocumentData(deepSourceCopy, super.equality, project, this.includeHierarchyFields, this.includeHierarchyFields);
            } else {
                docMapper = new KeyToDocumentData(deepSourceCopy, this.myEnvironment, this.documentOptions, super.equality, getEvaluationFilter(),
                                this.includeHierarchyFields, this.includeHierarchyFields);
            }
            Iterator<Tuple2<Key,Document>> mappedDocuments = Iterators.transform(
                            documents,
                            new GetDocument(docMapper, new Aggregation(this.getTimeFilter(), typeMetadataWithNonIndexed, compositeMetadata, this
                                            .isIncludeGroupingContext(), this.disableIndexOnlyDocuments(), this.getEvaluationFilter())));
            
            Iterator<Entry<Key,Document>> retDocuments = Iterators.transform(mappedDocuments, new TupleToEntry<Key,Document>());
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
                span.data("Key", rowColfamToString(this.key));
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
        DocumentProjection projection = new DocumentProjection(this.isIncludeGroupingContext(), this.isReducedResponse());
        
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
        DocumentProjection projection = new DocumentProjection(this.isIncludeGroupingContext(), this.isReducedResponse());
        Set<String> composites = Sets.newHashSet();
        for (Multimap<String,String> val : this.compositeMetadata.getCompositeToFieldMap().values()) {
            composites.addAll(val.asMap().keySet());
        }
        projection.initializeBlacklist(composites);
        return projection;
    }
    
    public static boolean isDocumentSpecificRange(Range r) {
        Preconditions.checkNotNull(r);
        
        // A Range for a document...
        
        // Cannot have a null start or end key
        //
        // Also @see datawave.query.index.lookup.TupleToRange
        // We have already made the assertion that the client is sending us
        // an inclusive start key due to the inability to ascertain the
        // difference
        // between and event-specific range and a continueMultiScan.
        //
        // As such, it is acceptable for us to make the same assertion on the
        // inclusivity
        // of the start key.
        if (r.isInfiniteStartKey() || r.isInfiniteStopKey()) {
            return false;
        } else {
            Key startKey = r.getStartKey(), endKey = r.getEndKey();
            
            // Cannot span multiple rows
            if (!startKey.getRowData().equals(endKey.getRowData())) {
                return false;
            }
            
            Text startColfam = startKey.getColumnFamily(), endColfam = endKey.getColumnFamily();
            
            // must contain a null byte separator in the column family
            if (startColfam.find(Constants.NULL) == -1 || endColfam.find(Constants.NULL) == -1) {
                return false;
            }
        }
        
        return true;
    }
    
    public static String rowColfamToString(Key k) {
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
    
    protected NestedIterator<Key> getOrSetKeySource(final Range documentRange, ASTJexlScript rangeScript) throws IOException, ConfigException {
        NestedIterator<Key> sourceIter = null;
        // If we're doing field index or a non-fulltable (aka a normal
        // query)
        if (!this.isFullTableScanOnly()) {
            
            boolean isQueryFullySatisfiedInitialState = batchedQueries <= 0 ? true : false;
            String hitListOptionString = documentOptions.get("hit.list");
            
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
                sourceIter = new SeekableNestedIterator(sourceIter);
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
                    throws MalformedURLException, ConfigException {
        if (log.isTraceEnabled()) {
            log.trace(documentRange);
        }
        
        // We need to pull tokenized fields from the field index as well as
        // index only fields
        Set<String> indexOnlyFields = new HashSet<String>(this.getIndexOnlyFields());
        indexOnlyFields.addAll(this.getTermFrequencyFields());
        
        // determine the list of indexed fields
        Set<String> indexedFields = new HashSet<String>(this.getTypeMetadata().keySet());
        indexedFields.removeAll(this.getNonIndexedDataTypeMap().keySet());
        
        IteratorBuildingVisitor iteratorBuildingVisitor = new IteratorBuildingVisitor(this, this.myEnvironment, this.getTimeFilter(), this.getTypeMetadata(),
                        indexOnlyFields, this.getFieldIndexKeyDataTypeFilter(), this.fiAggregator, this.getFileSystemCache(), this.getQueryLock(),
                        this.getIvaratorCacheBaseURIsAsList(), this.getQueryId(), this.getHdfsCacheSubDirPrefix(), this.getHdfsFileCompressionCodec(),
                        this.getIvaratorCacheBufferSize(), this.getIvaratorCacheScanPersistThreshold(), this.getIvaratorCacheScanTimeout(),
                        this.getMaxIndexRangeSplit(), this.getIvaratorMaxOpenFiles(), this.getMaxIvaratorSources(), indexedFields,
                        Collections.<String> emptySet(), this.getTermFrequencyFields(), isQueryFullySatisfied, sortedUIDs).limit(documentRange)
                        .disableIndexOnly(disableFiEval).limit(this.sourceLimit);
        
        iteratorBuildingVisitor.setCollectTimingDetails(this.collectTimingDetails);
        // TODO: iteratorBuildingVisitor.setStatsPort(this.statsdHostAndPort);
        iteratorBuildingVisitor.setQuerySpanCollector(this.querySpanCollector);
        
        return iteratorBuildingVisitor;
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
            StringBuilder hdfsPrefixBuilder = new StringBuilder();
            for (Entry<Range,String> queries : batchStack) {
                if (hdfsPrefixBuilder.length() > 0) {
                    hdfsPrefixBuilder.append('-');
                }
                hdfsPrefixBuilder.append(queries.getKey().getStartKey().getColumnFamily().toString().replace('\0', '_'));
            }
        }
        return hdfsPrefix;
    }
    
    protected SatisfactionVisitor createSatisfiabilityVisitor(boolean isQueryFullySatisfiedInitialState) {
        
        Set<String> indexOnlyFields = new HashSet<String>(this.getIndexOnlyFields());
        indexOnlyFields.addAll(this.getTermFrequencyFields());
        
        // determine the list of indexed fields
        Set<String> indexedFields = new HashSet<String>(this.getTypeMetadata().keySet());
        indexedFields.removeAll(this.getNonIndexedDataTypeMap().keySet());
        
        SatisfactionVisitor satisfactionVisitor = new SatisfactionVisitor(indexOnlyFields, indexedFields, Collections.<String> emptySet(),
                        isQueryFullySatisfiedInitialState);
        return satisfactionVisitor;
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
    
}
