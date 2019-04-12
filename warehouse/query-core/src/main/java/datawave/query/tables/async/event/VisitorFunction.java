package datawave.query.tables.async.event;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import datawave.core.iterators.filesystem.FileSystemCache;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.exceptions.InvalidQueryException;
import datawave.query.iterator.QueryOptions;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.DateIndexCleanupVisitor;
import datawave.query.jexl.visitors.ExecutableDeterminationVisitor;
import datawave.query.jexl.visitors.ExecutableDeterminationVisitor.STATE;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.jexl.visitors.PullupUnexecutableNodesVisitor;
import datawave.query.jexl.visitors.PushdownLargeFieldedListsVisitor;
import datawave.query.jexl.visitors.PushdownUnexecutableNodesVisitor;
import datawave.query.planner.DefaultQueryPlanner;
import datawave.query.tables.SessionOptions;
import datawave.query.tables.async.RangeDefinition;
import datawave.query.tables.async.ScannerChunk;
import datawave.query.util.MetadataHelper;
import datawave.util.StringUtils;
import datawave.webservice.query.Query;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.PreConditionFailedQueryException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * Purpose: Perform intermediate transformations on ScannerChunks as they are before being sent to the tablet server.
 *
 * Justification: The benefit of using this Function is that we can perform necessary transformations in the context of a thread about to send a request to
 * tabletservers. This parallelizes requests sent to the tablet servers.
 */
public class VisitorFunction implements Function<ScannerChunk,ScannerChunk> {
    
    protected static FileSystemCache fileSystemCache = null;
    
    private ShardQueryConfiguration config;
    protected MetadataHelper metadataHelper;
    protected Set<String> indexedFields;
    protected Set<String> indexOnlyFields;
    protected Set<String> nonEventFields;
    
    Map<String,String> previouslyExpanded = Maps.newHashMap();
    
    private static final Logger log = Logger.getLogger(VisitorFunction.class);
    
    public VisitorFunction(ShardQueryConfiguration config, MetadataHelper metadataHelper) throws MalformedURLException {
        this.config = config;
        
        if (VisitorFunction.fileSystemCache == null && this.config.getHdfsSiteConfigURLs() != null) {
            VisitorFunction.fileSystemCache = new FileSystemCache(this.config.getHdfsSiteConfigURLs());
        }
        
        this.metadataHelper = metadataHelper;
        
        if (config.getIndexedFields() != null && !config.getIndexedFields().isEmpty()) {
            indexedFields = config.getIndexedFields();
        } else {
            try {
                indexedFields = this.metadataHelper.getIndexedFields(config.getDatatypeFilter());
            } catch (TableNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
        
        try {
            indexOnlyFields = this.metadataHelper.getIndexOnlyFields(config.getDatatypeFilter());
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }
        
        try {
            nonEventFields = this.metadataHelper.getNonEventFields(config.getDatatypeFilter());
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }
        
    }
    
    @Override
    @Nullable
    public ScannerChunk apply(@Nullable ScannerChunk input) {
        
        SessionOptions options = input.getOptions();
        
        ScannerChunk newSettings = new ScannerChunk(null, input.getRanges(), input.getLastKnownLocation());
        
        SessionOptions newOptions = new SessionOptions(options);
        
        for (IteratorSetting setting : options.getIterators()) {
            
            final String query = setting.getOptions().get(QueryOptions.QUERY);
            if (null != query) {
                IteratorSetting newIteratorSetting = new IteratorSetting(setting.getPriority(), setting.getName(), setting.getIteratorClass());
                
                newIteratorSetting.addOptions(setting.getOptions());
                try {
                    
                    ASTJexlScript script = null;
                    
                    boolean evaluatedPreviously = previouslyExecutable(query);
                    
                    boolean madeChange = false;
                    
                    if (!evaluatedPreviously && config.isCleanupShardsAndDaysQueryHints()) {
                        script = JexlASTHelper.parseJexlQuery(query);
                        script = DateIndexCleanupVisitor.cleanup(script);
                        madeChange = true;
                    }
                    
                    String newQuery = evaluatedPreviously ? previouslyExpanded.get(query) : query;
                    
                    List<String> debug = null;
                    if (log.isTraceEnabled())
                        debug = Lists.newArrayList();
                    if (!config.isBypassExecutabilityCheck() || !evaluatedPreviously) {
                        if (null == script)
                            script = JexlASTHelper.parseJexlQuery(query);
                        
                        if (!ExecutableDeterminationVisitor.isExecutable(script, config, indexedFields, indexOnlyFields, nonEventFields, debug,
                                        this.metadataHelper)) {
                            
                            if (log.isTraceEnabled()) {
                                log.trace("Need to pull up non-executable query: " + JexlStringBuildingVisitor.buildQuery(script));
                                for (String debugStatement : debug) {
                                    log.trace(debugStatement);
                                }
                                DefaultQueryPlanner.logQuery(script, "Failing query:");
                            }
                            script = (ASTJexlScript) PullupUnexecutableNodesVisitor.pullupDelayedPredicates(script, config, indexedFields, indexOnlyFields,
                                            nonEventFields, metadataHelper);
                            madeChange = true;
                            
                            STATE state = ExecutableDeterminationVisitor.getState(script, config, indexedFields, indexOnlyFields, nonEventFields, false, debug,
                                            metadataHelper);
                            
                            /**
                             * We could achieve better performance if we live with the small number of queries that error due to the full table scan exception.
                             *
                             * Either look at improving PushdownUnexecutableNodesVisitor or avoid the process altogether.
                             */
                            if (state != STATE.EXECUTABLE) {
                                if (log.isTraceEnabled()) {
                                    log.trace("Need to push down non-executable query: " + JexlStringBuildingVisitor.buildQuery(script));
                                    for (String debugStatement : debug) {
                                        log.trace(debugStatement);
                                    }
                                }
                                script = (ASTJexlScript) PushdownUnexecutableNodesVisitor.pushdownPredicates(script, config, indexedFields, indexOnlyFields,
                                                nonEventFields, metadataHelper);
                            }
                            
                            state = ExecutableDeterminationVisitor.getState(script, config, indexedFields, indexOnlyFields, nonEventFields, false, debug,
                                            metadataHelper);
                            
                            if (state != STATE.EXECUTABLE) {
                                if (state == STATE.ERROR) {
                                    log.warn("After expanding the query, it is determined that the query cannot be executed due to index-only fields mixed with expressions that cannot be run against the index.");
                                    BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INDEX_ONLY_FIELDS_MIXED_INVALID_EXPRESSIONS);
                                    throw new InvalidQueryException(qe);
                                }
                                log.warn("After expanding the query, it is determined that the query cannot be executed against the field index and a full table scan is required");
                                if (!config.getFullTableScanEnabled()) {
                                    
                                    if (log.isTraceEnabled()) {
                                        log.trace("Full Table fail of " + JexlStringBuildingVisitor.buildQuery(script));
                                        for (String debugStatement : debug) {
                                            log.trace(debugStatement);
                                        }
                                        DefaultQueryPlanner.logQuery(script, "Failing query:");
                                    }
                                    PreConditionFailedQueryException qe = new PreConditionFailedQueryException(
                                                    DatawaveErrorCode.FULL_TABLE_SCAN_REQUIRED_BUT_DISABLED);
                                    throw new DatawaveFatalQueryException(qe);
                                }
                            }
                            
                            if (log.isTraceEnabled()) {
                                for (String debugStatement : debug) {
                                    log.trace(debugStatement);
                                }
                                DefaultQueryPlanner.logQuery(script, "Query pushing down large fielded lists:");
                            }
                        }
                    }
                    
                    if (config.getSerializeQueryIterator()) {
                        serializeQuery(newIteratorSetting);
                    } else {
                        // only expand if we have non doc specific ranges.
                        if (!RangeDefinition.allDocSpecific(input.getRanges())) {
                            if (!evaluatedPreviously) {
                                // if we have an hdfs configuration, then we can pushdown large fielded lists to an ivarator
                                if (config.getHdfsSiteConfigURLs() != null && setting.getOptions().get(QueryOptions.BATCHED_QUERY) == null) {
                                    if (null == script)
                                        script = JexlASTHelper.parseJexlQuery(query);
                                    try {
                                        script = pushdownLargeFieldedLists(config, script);
                                        madeChange = true;
                                    } catch (IOException ioe) {
                                        log.error("Unable to pushdown large fielded lists....leaving in expanded form", ioe);
                                    }
                                }
                            }
                            
                        } else {
                            if (input.getRanges().size() == 1) {
                                if (log.isTraceEnabled()) {
                                    log.trace("Ensuring max pipelines is set to 1");
                                    
                                }
                                serializeQuery(newIteratorSetting);
                            }
                        }
                    }
                    
                    // only recompile the script if changes were made to the query
                    if (madeChange)
                        newQuery = JexlStringBuildingVisitor.buildQuery(script);
                    
                    previouslyExpanded.put(query, newQuery);
                    
                    newIteratorSetting.addOption(QueryOptions.QUERY, newQuery);
                    newOptions.removeScanIterator(setting.getName());
                    newOptions.addScanIterator(newIteratorSetting);
                    
                } catch (ParseException e) {
                    throw new DatawaveFatalQueryException(e);
                }
            }
            
        }
        
        newSettings.setOptions(newOptions);
        return newSettings;
    }
    
    private boolean previouslyExecutable(String query) {
        return previouslyExpanded.containsKey(query);
    }
    
    /**
     * Serializes the query iterator
     * 
     * @param newIteratorSetting
     */
    private void serializeQuery(IteratorSetting newIteratorSetting) {
        newIteratorSetting.addOption(QueryOptions.SERIAL_EVALUATION_PIPELINE, "true");
        newIteratorSetting.addOption(QueryOptions.MAX_EVALUATION_PIPELINES, "1");
        newIteratorSetting.addOption(QueryOptions.MAX_PIPELINE_CACHED_RESULTS, "1");
        
    }
    
    // push down large fielded lists. Assumes that the hdfs query cache uri and
    // site config urls are configured
    protected ASTJexlScript pushdownLargeFieldedLists(ShardQueryConfiguration config, ASTJexlScript queryTree) throws IOException {
        Query settings = config.getQuery();
        
        URI hdfsQueryCacheUri = getFstHdfsQueryCacheUri(config, settings);
        
        if (hdfsQueryCacheUri != null) {
            FileSystem fs = VisitorFunction.fileSystemCache.getFileSystem(hdfsQueryCacheUri);
            // Find large lists of values against the same field and push down into
            // an Ivarator
            return PushdownLargeFieldedListsVisitor.pushdown(config, queryTree, fs, hdfsQueryCacheUri.toString());
        } else {
            return PushdownLargeFieldedListsVisitor.pushdown(config, queryTree, null, null);
        }
    }
    
    protected URI getFstHdfsQueryCacheUri(ShardQueryConfiguration config, Query settings) {
        if (config.getIvaratorFstHdfsBaseURIs() != null) {
            String[] choices = StringUtils.split(config.getIvaratorFstHdfsBaseURIs(), ',');
            int index = new Random().nextInt(choices.length);
            Path path = new Path(choices[index], settings.getId().toString());
            return path.toUri();
        }
        return null;
    }
}
