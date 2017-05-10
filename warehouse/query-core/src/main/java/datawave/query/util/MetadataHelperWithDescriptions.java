package datawave.query.util;

import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import datawave.data.ColumnFamilyConstants;
import datawave.marking.MarkingFunctions;
import datawave.marking.MarkingFunctionsFactory;
import datawave.security.util.ScannerHelper;
import datawave.webservice.query.result.event.DefaultResponseObjectFactory;
import datawave.webservice.query.result.event.ResponseObjectFactory;

import datawave.webservice.results.datadictionary.DescriptionBase;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;

import javax.inject.Inject;

/**
 * <p>
 * Helper class to fetch the set of field names which are only indexed, i.e. do not occur as attributes in the event.
 * </p>
 * 
 * <p>
 * This set would normally includes all tokenized content fields. In terms of keys in the DatawaveMetadata table, this set would contain all rows in the
 * {@code DatawaveMetadata} table which have a {@link ColumnFamilyConstants#COLF_I} but not a {@link ColumnFamilyConstants#COLF_E}
 * </p>
 * 
 * 
 * TODO -- Break this class apart
 * 
 */
@Configuration
@EnableCaching
@Component("metadataHelperWithDescriptions")
public class MetadataHelperWithDescriptions extends MetadataHelper {
    private static final Logger log = Logger.getLogger(MetadataHelperWithDescriptions.class);
    
    protected MarkingFunctions markingFunctions = MarkingFunctionsFactory.createMarkingFunctions();
    
    @Inject
    private ResponseObjectFactory responseObjectFactory;
    
    /**
     * Create and instance of a metadata helper. this is for unit tests only
     * 
     * @param connector
     * @param metadataTableName
     * @param auths
     * @return the metadata helper
     */
    public static MetadataHelperWithDescriptions getInstance(Connector connector, String metadataTableName, Authorizations allMetadataAuths,
                    Set<Authorizations> auths) {
        MetadataHelperWithDescriptions mhwd = new MetadataHelperWithDescriptions();
        mhwd.allFieldMetadataHelper = new AllFieldMetadataHelper();
        mhwd.allFieldMetadataHelper.typeMetadataHelper = new TypeMetadataHelper();
        mhwd.allFieldMetadataHelper.compositeMetadataHelper = new CompositeMetadataHelper();
        mhwd.setAllMetadataAuths(Collections.singleton(allMetadataAuths));
        mhwd.setResponseObjectFactory(new DefaultResponseObjectFactory());
        return mhwd.initialize(connector, connector.getInstance(), metadataTableName, auths);
    }
    
    public MetadataHelperWithDescriptions initialize(Connector connector, String metadataTableName, Set<Authorizations> auths) {
        return initialize(connector, connector.getInstance(), metadataTableName, auths);
    };
    
    /**
     * Initializes the instance with a provided update interval.
     * 
     * @param connector
     *            A Connector to Accumulo
     * @param metadataTableName
     *            The name of the DatawaveMetadata table
     * @param auths
     *            Any {@link Authorizations} to use
     */
    public MetadataHelperWithDescriptions initialize(Connector connector, Instance instance, String metadataTableName, Set<Authorizations> auths,
                    boolean useSubstitutions) {
        super.initialize(connector, instance, metadataTableName, auths, useSubstitutions);
        return this;
    }
    
    public MetadataHelperWithDescriptions initialize(Connector connector, Instance instance, String metadataTableName, Set<Authorizations> auths) {
        super.initialize(connector, instance, metadataTableName, auths, false);
        return this;
    }
    
    /**
     * Get the metadata fully populated
     * 
     * @return
     * @throws TableNotFoundException
     * @throws ExecutionException
     */
    @Cacheable(value = "getMetadata", key = "{#root.target.auths,#root.target.metadataTableName}", cacheManager = "metadataHelperCacheManager")
    public MetadataWithDescriptions getMetadata() throws TableNotFoundException, ExecutionException, ExecutionException, MarkingFunctions.Exception {
        return getMetadata(null);
    }
    
    /**
     * Get the metadata fully populated
     * 
     * @return
     * @throws TableNotFoundException
     * @throws ExecutionException
     */
    @Cacheable(value = "getMetadata", key = "{#root.target.auths,#root.target.metadataTableName,#ingestTypeFilter}",
                    cacheManager = "metadataHelperCacheManager")
    public MetadataWithDescriptions getMetadata(Set<String> ingestTypeFilter) throws TableNotFoundException, ExecutionException, MarkingFunctions.Exception {
        log.debug("cache fault for getMetadata(" + this.auths + "," + this.metadataTableName + "," + ingestTypeFilter + ")");
        return new MetadataWithDescriptions(this, ingestTypeFilter);
    }
    
    public SetMultimap<MetadataEntry,DescriptionBase> getDescriptions(Set<String> ingestTypeFilter) throws TableNotFoundException, MarkingFunctions.Exception {
        
        SetMultimap<MetadataEntry,DescriptionBase> descriptions = loadDescriptions();
        
        SetMultimap<MetadataEntry,DescriptionBase> descs = HashMultimap.create();
        
        if (ingestTypeFilter == null || ingestTypeFilter.isEmpty()) {
            descs.putAll(descriptions);
        } else {
            for (MetadataEntry entry : descriptions.keySet()) {
                if (ingestTypeFilter.contains(entry.getDatatype())) {
                    descs.putAll(entry, descriptions.get(entry));
                }
            }
        }
        
        return descs;
    }
    
    public SetMultimap<String,DescriptionBase> getFieldDescriptions(Set<String> ingestTypeFilter) throws TableNotFoundException, ExecutionException,
                    MarkingFunctions.Exception {
        SetMultimap<MetadataEntry,DescriptionBase> descriptions = getDescriptions(ingestTypeFilter);
        SetMultimap<String,DescriptionBase> fieldDescriptions = HashMultimap.create();
        
        for (MetadataEntry entry : descriptions.keySet()) {
            String fieldName = entry.getFieldName();
            
            fieldDescriptions.putAll(fieldName, descriptions.get(entry));
        }
        
        return fieldDescriptions;
    }
    
    public SetMultimap<MetadataEntry,DescriptionBase> getDescriptions(String datatype) throws TableNotFoundException, ExecutionException,
                    MarkingFunctions.Exception {
        return getDescriptions(Collections.singleton(datatype));
    }
    
    public Set<DescriptionBase> getDescriptions(String fieldname, String datatype) throws TableNotFoundException, ExecutionException,
                    MarkingFunctions.Exception {
        SetMultimap<MetadataEntry,DescriptionBase> descriptions = getDescriptions(datatype);
        MetadataEntry desired = new MetadataEntry(fieldname, datatype);
        if (descriptions.containsKey(desired)) {
            return descriptions.get(desired);
        }
        
        return Collections.emptySet();
    }
    
    public void setDescription(MetadataEntry entry, DescriptionBase desc) throws TableNotFoundException, MutationsRejectedException, MarkingFunctions.Exception {
        setDescriptions(entry, Collections.singleton(desc));
    }
    
    public void setDescriptions(MetadataEntry entry, Set<? extends DescriptionBase> descs) throws TableNotFoundException, MutationsRejectedException,
                    MarkingFunctions.Exception {
        BatchWriter bw = null;
        try {
            BatchWriterConfig bwConfig = new BatchWriterConfig().setMaxMemory(10000L).setMaxLatency(100L, TimeUnit.MILLISECONDS).setMaxWriteThreads(1);
            bw = connector.createBatchWriter(metadataTableName, bwConfig);
            Mutation m = new Mutation(entry.getFieldName());
            for (DescriptionBase desc : descs) {
                m.put(ColumnFamilyConstants.COLF_DESC, new Text(entry.getDatatype()), markingFunctions.translateToColumnVisibility(desc.getMarkings()),
                                new Value(desc.getDescription().getBytes()));
            }
            bw.addMutation(m);
        } finally {
            if (null != bw) {
                bw.close();
            }
        }
        
        if (log.isTraceEnabled()) {
            log.trace("Invalidating base table cache and metadata cache to add " + entry.getFieldName());
        }
    }
    
    /**
     * Remove model descriptions
     * 
     * @param entry
     *            metadata entry
     * @param desc
     *            description to remove
     * @throws TableNotFoundException
     * @throws MutationsRejectedException
     */
    public void removeDescription(MetadataEntry entry, DescriptionBase desc) throws TableNotFoundException, MutationsRejectedException,
                    MarkingFunctions.Exception {
        BatchWriter bw = null;
        try {
            BatchWriterConfig bwConfig = new BatchWriterConfig().setMaxMemory(10000L).setMaxLatency(100L, TimeUnit.MILLISECONDS).setMaxWriteThreads(1);
            bw = connector.createBatchWriter(metadataTableName, bwConfig);
            Mutation m = new Mutation(entry.getFieldName());
            m.putDelete(ColumnFamilyConstants.COLF_DESC, new Text(entry.getDatatype()), this.markingFunctions.translateToColumnVisibility(desc.getMarkings()));
            bw.addMutation(m);
        } finally {
            if (null != bw) {
                bw.close();
            }
        }
        
        if (log.isTraceEnabled()) {
            log.trace("Invalidating base table cache and metadata cache to add " + entry.getFieldName());
        }
        
    }
    
    protected SetMultimap<MetadataEntry,DescriptionBase> loadDescriptions() throws TableNotFoundException, MarkingFunctions.Exception {
        if (log.isTraceEnabled())
            log.trace("loadDescriptions from table: " + metadataTableName);
        // unlike other entries, the desc colf entries have many auths set. We'll use the fullUserAuths in the scanner instead
        // of the minimal set in this.auths
        Scanner scanner = ScannerHelper.createScanner(connector, metadataTableName, fullUserAuths);
        
        SetMultimap<MetadataEntry,DescriptionBase> descriptions = HashMultimap.create();
        
        scanner.setRange(new Range());
        scanner.fetchColumnFamily(ColumnFamilyConstants.COLF_DESC);
        for (Entry<Key,Value> entry : scanner) {
            MetadataEntry mentry = new MetadataEntry(entry.getKey());
            if (log.isTraceEnabled()) {
                log.trace(entry.getKey());
                
            }
            DescriptionBase desc = this.responseObjectFactory.getDescription();
            desc.setDescription(entry.getValue().toString());
            desc.setMarkings(getMarkings(entry.getKey()));
            
            descriptions.put(mentry, desc);
        }
        
        return Multimaps.unmodifiableSetMultimap(descriptions);
        
    }
    
    private static String getKey(Instance instance, String metadataTableName, Set<Authorizations> auths) {
        StringBuilder builder = new StringBuilder();
        builder.append(instance.getInstanceID()).append('\0');
        builder.append(metadataTableName).append('\0');
        builder.append(auths);
        return builder.toString();
    }
    
    private static String getKey(MetadataHelperWithDescriptions helper) {
        return getKey(helper.instance, helper.metadataTableName, helper.auths);
    }
    
    private Map<String,String> getMarkings(Key k) throws MarkingFunctions.Exception {
        return getMarkings(k.getColumnVisibilityParsed());
    }
    
    private Map<String,String> getMarkings(ColumnVisibility visibility) throws MarkingFunctions.Exception {
        return markingFunctions.translateFromColumnVisibility(visibility);
    }
    
    private ColumnVisibility getVisibility(Map<String,String> markings) throws MarkingFunctions.Exception {
        return markingFunctions.translateToColumnVisibility(markings);
    }
    
    public void setResponseObjectFactory(ResponseObjectFactory responseObjectFactory) {
        this.responseObjectFactory = responseObjectFactory;
    }
    
    public ResponseObjectFactory getResponseObjectFactory() {
        return this.responseObjectFactory;
    }
    
    /**
     * Invalidates all elements in all internal caches
     */
    @CacheEvict(value = {"getMetadata"}, allEntries = true, cacheManager = "metadataHelperCacheManager")
    public void evictCaches() {
        log.debug("evictCaches");
        super.evictCaches();
    }
}
