package nsa.datawave.query.tables.async.event;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;

import nsa.datawave.query.rewrite.jexl.visitors.PrintingVisitor;
import nsa.datawave.query.rewrite.jexl.visitors.PushdownUnexecutableNodesVisitor;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

import nsa.datawave.data.type.Type;
import nsa.datawave.query.rewrite.config.RefactoredShardQueryConfiguration;
import nsa.datawave.query.rewrite.exceptions.DatawaveFatalQueryException;
import nsa.datawave.query.rewrite.exceptions.InvalidQueryException;
import nsa.datawave.query.rewrite.iterator.QueryOptions;
import nsa.datawave.query.rewrite.jexl.JexlASTHelper;
import nsa.datawave.query.rewrite.jexl.visitors.DateIndexCleanupVisitor;
import nsa.datawave.query.rewrite.jexl.visitors.ExecutableDeterminationVisitor;
import nsa.datawave.query.rewrite.jexl.visitors.ExecutableDeterminationVisitor.STATE;
import nsa.datawave.query.rewrite.jexl.visitors.JexlStringBuildingVisitor;
import nsa.datawave.query.rewrite.jexl.visitors.PrintingVisitor;
import nsa.datawave.query.rewrite.jexl.visitors.PullupUnexecutableNodesVisitor;
import nsa.datawave.query.rewrite.jexl.visitors.PushdownLargeFieldedListsVisitor;
import nsa.datawave.query.rewrite.planner.DefaultQueryPlanner;
import nsa.datawave.query.tables.SessionOptions;
import nsa.datawave.query.tables.async.RangeDefinition;
import nsa.datawave.query.tables.async.ScannerChunk;
import nsa.datawave.query.util.MetadataHelper;
import nsa.datawave.webservice.query.Query;
import nsa.datawave.webservice.query.exception.BadRequestQueryException;
import nsa.datawave.webservice.query.exception.DatawaveErrorCode;
import nsa.datawave.webservice.query.exception.PreConditionFailedQueryException;

public class VisitorFunction implements Function<ScannerChunk,ScannerChunk> {
    
    protected static Cache<String,FileSystem> fileSystemCache = CacheBuilder.newBuilder().concurrencyLevel(5).maximumSize(100).build();
    
    private RefactoredShardQueryConfiguration config;
    protected MetadataHelper metadataHelper;
    protected Set<String> indexOnlyFields;
    protected Multimap<String,Type<?>> typeMap;
    
    private static final Logger log = Logger.getLogger(VisitorFunction.class);
    
    public VisitorFunction(RefactoredShardQueryConfiguration config, MetadataHelper metadataHelper) {
        this.config = config;
        
        this.metadataHelper = metadataHelper;
        
        try {
            indexOnlyFields = this.metadataHelper.getIndexOnlyFields(config.getDatatypeFilter());
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
                    
                    ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
                    
                    PrintingVisitor.printQuery(script);
                    if (config.isCleanupShardsAndDaysQueryHints()) {
                        script = DateIndexCleanupVisitor.cleanup(script);
                    }
                    
                    if (config.getSerializeQueryIterator()) {
                        serializeQuery(newIteratorSetting);
                    } else {
                        // only expand if we have non doc specific ranges.
                        if (!RangeDefinition.allDocSpecific(input.getRanges())) {
                            // if we have an hdfs cached base URI, then we can
                            // push
                            // down large fielded lists to an ivarator
                            if (config.getHdfsCacheBaseURI() != null && config.getHdfsSiteConfigURLs() != null
                                            && setting.getOptions().get(QueryOptions.BATCHED_QUERY) == null) {
                                script = pushdownLargeFieldedLists(config, script);
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
                    
                    List<String> debug = null;
                    if (log.isTraceEnabled())
                        debug = Lists.newArrayList();
                    if (!config.bypassExecutabilityCheck()) {
                        
                        if (!ExecutableDeterminationVisitor.isExecutable(script, config, indexOnlyFields, debug, this.metadataHelper)) {
                            
                            if (log.isTraceEnabled()) {
                                log.trace("Need to pull up non-executable query: " + JexlStringBuildingVisitor.buildQuery(script));
                                for (String debugStatement : debug) {
                                    log.trace(debugStatement);
                                }
                                DefaultQueryPlanner.logQuery(script, "Failing query:");
                            }
                            script = (ASTJexlScript) PullupUnexecutableNodesVisitor.pullupDelayedPredicates(script, config, metadataHelper);
                        }
                        
                        if (log.isTraceEnabled()) {
                            debug = Lists.newArrayList();
                        }
                        if (!ExecutableDeterminationVisitor.isExecutable(script, config, indexOnlyFields, debug, this.metadataHelper)) {
                            if (log.isTraceEnabled()) {
                                log.trace("Need to push down non-executable query: " + JexlStringBuildingVisitor.buildQuery(script));
                                for (String debugStatement : debug) {
                                    log.trace(debugStatement);
                                }
                                DefaultQueryPlanner.logQuery(script, "Failing query:");
                            }
                            script = (ASTJexlScript) PushdownUnexecutableNodesVisitor.pushdownPredicates(script, config, metadataHelper);
                        }
                        
                        STATE state = ExecutableDeterminationVisitor.getState(script, config, metadataHelper, debug);
                        
                        if (state != STATE.EXECUTABLE) {
                            if (log.isDebugEnabled()) {
                                log.debug("Instead of being executable, state is:" + state + " for query:" + PrintingVisitor.formattedQueryString(script));
                            }
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
                    String newQuery = JexlStringBuildingVisitor.buildQuery(script);
                    
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
    protected ASTJexlScript pushdownLargeFieldedLists(RefactoredShardQueryConfiguration config, ASTJexlScript queryTree) {
        Query settings = config.getQuery();
        String hdfsQueryCacheUri = getHdfsQueryCacheUri(config, settings);
        Configuration conf = new Configuration();
        for (String url : StringUtils.split(config.getHdfsSiteConfigURLs(), ',')) {
            try {
                conf.addResource(new URL(url));
            } catch (MalformedURLException e) {
                throw new DatawaveFatalQueryException("Cannot parse hdfs site config url " + url);
            }
        }
        
        FileSystem fs = fileSystemCache.getIfPresent(config.getHdfsCacheBaseURI());
        if (null == fs) {
            URI hdfsCacheURI;
            try {
                hdfsCacheURI = new URI(hdfsQueryCacheUri);
            } catch (URISyntaxException e) {
                throw new DatawaveFatalQueryException("Cannot parse hdfs query cache uri " + hdfsQueryCacheUri);
            }
            
            try {
                fs = FileSystem.get(hdfsCacheURI, conf);
            } catch (IOException e) {
                throw new DatawaveFatalQueryException("Cannot create FileSystem from " + hdfsCacheURI + " and " + config.getHdfsSiteConfigURLs());
            }
            fileSystemCache.put(config.getHdfsCacheBaseURI(), fs);
        }
        // Find large lists of values against the same field and push down into
        // an Ivarator
        return PushdownLargeFieldedListsVisitor.pushdown(config, queryTree, fs, hdfsQueryCacheUri);
    }
    
    protected String getHdfsQueryCacheUri(RefactoredShardQueryConfiguration config, Query settings) {
        StringBuilder baseUri = new StringBuilder();
        baseUri.append(config.getHdfsCacheBaseURI());
        if (baseUri.charAt(baseUri.length() - 1) != Path.SEPARATOR_CHAR) {
            baseUri.append(Path.SEPARATOR_CHAR);
        }
        baseUri.append(settings.getId().toString());
        return baseUri.toString();
    }
}
