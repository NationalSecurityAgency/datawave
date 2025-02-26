package datawave.microservice.metadata;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;

import datawave.data.ColumnFamilyConstants;
import datawave.marking.MarkingFunctions;
import datawave.microservice.dictionary.config.ResponseObjectFactory;
import datawave.query.util.MetadataEntry;
import datawave.security.util.ScannerHelper;
import datawave.webservice.dictionary.data.DescriptionBase;

/**
 * Helper class to handle get/set of descriptions on the metadata table.
 */
@EnableCaching
@Component("metadataDescriptionsHelper")
@Scope("prototype")
public class MetadataDescriptionsHelper<DESC extends DescriptionBase<DESC>> {
    private static final Logger log = LoggerFactory.getLogger(MetadataDescriptionsHelper.class);
    
    private final MarkingFunctions markingFunctions;
    private final ResponseObjectFactory<DESC,?,?,?,?> responseObjectFactory;
    
    private String metadataTableName;
    private AccumuloClient accumuloClient;
    private Set<Authorizations> fullUserAuths;
    
    public MetadataDescriptionsHelper(MarkingFunctions markingFunctions, ResponseObjectFactory<DESC,?,?,?,?> responseObjectFactory) {
        this.markingFunctions = markingFunctions;
        this.responseObjectFactory = responseObjectFactory;
    }
    
    public void initialize(AccumuloClient accumuloClient, String metadataTableName, Set<Authorizations> fullUserAuths) {
        this.accumuloClient = accumuloClient;
        this.metadataTableName = metadataTableName;
        this.fullUserAuths = fullUserAuths;
    }
    
    public SetMultimap<MetadataEntry,DESC> getDescriptions(Set<String> ingestTypeFilter) throws TableNotFoundException, MarkingFunctions.Exception {
        
        SetMultimap<MetadataEntry,DESC> descriptions = loadDescriptions();
        
        SetMultimap<MetadataEntry,DESC> descs = HashMultimap.create();
        
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
    
    public SetMultimap<String,DESC> getFieldDescriptions(Set<String> ingestTypeFilter)
                    throws TableNotFoundException, ExecutionException, MarkingFunctions.Exception {
        SetMultimap<MetadataEntry,DESC> descriptions = getDescriptions(ingestTypeFilter);
        SetMultimap<String,DESC> fieldDescriptions = HashMultimap.create();
        
        for (MetadataEntry entry : descriptions.keySet()) {
            String fieldName = entry.getFieldName();
            
            fieldDescriptions.putAll(fieldName, descriptions.get(entry));
        }
        
        return fieldDescriptions;
    }
    
    public SetMultimap<MetadataEntry,DESC> getDescriptions(String datatype) throws TableNotFoundException, MarkingFunctions.Exception {
        return getDescriptions(Collections.singleton(datatype));
    }
    
    public Set<DESC> getDescriptions(String fieldname, String datatype) throws TableNotFoundException, MarkingFunctions.Exception {
        SetMultimap<MetadataEntry,DESC> descriptions = getDescriptions(datatype);
        MetadataEntry desired = new MetadataEntry(fieldname, datatype);
        if (descriptions.containsKey(desired)) {
            return descriptions.get(desired);
        }
        
        return Collections.emptySet();
    }
    
    public void setDescription(MetadataEntry entry, DescriptionBase desc)
                    throws TableNotFoundException, MutationsRejectedException, MarkingFunctions.Exception {
        setDescriptions(entry, Collections.singleton(desc));
    }
    
    public void setDescriptions(MetadataEntry entry, Set<? extends DescriptionBase> descs)
                    throws TableNotFoundException, MutationsRejectedException, MarkingFunctions.Exception {
        BatchWriter bw = null;
        try {
            BatchWriterConfig bwConfig = new BatchWriterConfig().setMaxMemory(10000L).setMaxLatency(100L, TimeUnit.MILLISECONDS).setMaxWriteThreads(1);
            bw = accumuloClient.createBatchWriter(metadataTableName, bwConfig);
            Mutation m = new Mutation(entry.getFieldName());
            for (DescriptionBase desc : descs) {
                m.put(ColumnFamilyConstants.COLF_DESC, new Text(entry.getDatatype()), markingFunctions.translateToColumnVisibility(desc.getMarkings()),
                                new Value(desc.getDescription().getBytes(UTF_8)));
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
     *             if the configured metadataTableName does not exist
     * @throws MutationsRejectedException
     *             if writing the update to Accumulo fails
     */
    public void removeDescription(MetadataEntry entry, DescriptionBase desc)
                    throws TableNotFoundException, MutationsRejectedException, MarkingFunctions.Exception {
        BatchWriter bw = null;
        try {
            BatchWriterConfig bwConfig = new BatchWriterConfig().setMaxMemory(10000L).setMaxLatency(100L, TimeUnit.MILLISECONDS).setMaxWriteThreads(1);
            bw = accumuloClient.createBatchWriter(metadataTableName, bwConfig);
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
    
    protected SetMultimap<MetadataEntry,DESC> loadDescriptions() throws TableNotFoundException, MarkingFunctions.Exception {
        if (log.isTraceEnabled())
            log.trace("loadDescriptions from table: " + metadataTableName);
        // unlike other entries, the desc colf entries have many auths set. We'll use the fullUserAuths in the scanner instead
        // of the minimal set in this.auths
        Scanner scanner = ScannerHelper.createScanner(accumuloClient, metadataTableName, fullUserAuths);
        
        SetMultimap<MetadataEntry,DESC> descriptions = HashMultimap.create();
        
        scanner.setRange(new Range());
        scanner.fetchColumnFamily(ColumnFamilyConstants.COLF_DESC);
        for (Entry<Key,Value> entry : scanner) {
            MetadataEntry mentry = new MetadataEntry(entry.getKey());
            log.trace("{}", entry.getKey());
            DESC desc = this.responseObjectFactory.getDescription();
            desc.setDescription(entry.getValue().toString());
            desc.setMarkings(getMarkings(entry.getKey()));
            
            descriptions.put(mentry, desc);
        }
        
        return Multimaps.unmodifiableSetMultimap(descriptions);
        
    }
    
    private Map<String,String> getMarkings(Key k) throws MarkingFunctions.Exception {
        return getMarkings(k.getColumnVisibilityParsed());
    }
    
    private Map<String,String> getMarkings(ColumnVisibility visibility) throws MarkingFunctions.Exception {
        return markingFunctions.translateFromColumnVisibility(visibility);
    }
}
