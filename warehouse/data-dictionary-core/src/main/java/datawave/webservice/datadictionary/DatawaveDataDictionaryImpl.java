package datawave.webservice.datadictionary;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;

import javax.inject.Inject;

import datawave.configuration.spring.SpringBean;
import datawave.data.ColumnFamilyConstants;
import datawave.marking.MarkingFunctions;
import datawave.query.model.QueryModel;
import datawave.query.util.MetadataEntry;
import datawave.query.util.MetadataHelperWithDescriptions;
import datawave.security.util.ScannerHelper;
import datawave.util.time.DateHelper;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.query.result.metadata.DefaultMetadataField;
import datawave.webservice.query.result.metadata.MetadataFieldBase;
import datawave.webservice.results.datadictionary.DescriptionBase;
import datawave.webservice.results.datadictionary.DictionaryFieldBase;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

/**
 * 
 */
public class DatawaveDataDictionaryImpl implements DatawaveDataDictionary {
    private static final Logger log = Logger.getLogger(DatawaveDataDictionaryImpl.class);
    
    private Map<String,String> normalizerMapping = Maps.newHashMap();
    
    @Inject
    @SpringBean(refreshable = true)
    private MarkingFunctions markingFunctions;
    
    @Inject
    @SpringBean(name = "allMetadataAuths")
    private Set<Authorizations> allMetadataAuths;
    
    @Inject
    private ResponseObjectFactory responseObjectFactory;
    
    /*
     * (non-Javadoc)
     * 
     * @see datawave.webservice.datadictionary.DatawaveDataDictionary#getNormalizerMapping()
     */
    @Override
    public Map<String,String> getNormalizerMapping() {
        return normalizerMapping;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see datawave.webservice.datadictionary.DatawaveDataDictionary#setNormalizerMapping(java.util.Map)
     */
    @Override
    public void setNormalizerMapping(Map<String,String> normalizerMapping) {
        this.normalizerMapping = normalizerMapping;
    }
    
    /*
     * (non-Javadoc)
     * 
     * Note: dataTypeFilters can be empty, which means all the fields will be returned
     * 
     * @see datawave.webservice.datadictionary.DatawaveDataDictionary#getFields(java.lang.String, java.lang.String, java.lang.String,
     * Java.util.Collection<java.lang.String>, org.apache.accumulo.core.client.Connector, accumulo.core.security.Authorizations, int)
     */
    @Override
    public Collection<MetadataFieldBase> getFields(String modelName, String modelTableName, String metadataTableName, Collection<String> dataTypeFilters,
                    Connector connector, Set<Authorizations> auths, int numThreads) throws Exception {
        // Get a MetadataHelper
        MetadataHelperWithDescriptions metadataHelper = MetadataHelperWithDescriptions.getInstance(connector, metadataTableName, allMetadataAuths.iterator()
                        .next(), auths);
        // So we can get a QueryModel
        QueryModel queryModel = metadataHelper.getQueryModel(modelTableName, modelName, metadataHelper.getIndexOnlyFields(null));
        
        Map<String,String> reverseMapping = null;
        
        // So we can pull the reverse mapping for this model
        if (null != queryModel) {
            reverseMapping = queryModel.getReverseQueryMapping();
        } else {
            reverseMapping = Collections.emptyMap();
        }
        
        // Fetch the results from Accumulo
        BatchScanner bs = fetchResults(connector, metadataTableName, auths, numThreads);
        
        // Convert them into a response object
        Collection<MetadataFieldBase> fields = transformResults(bs.iterator(), reverseMapping, dataTypeFilters);
        
        // Close the BatchScanner and return the Accumulo connector
        bs.close();
        
        // Convert them into the DataDictionary response object
        return fields;
    }
    
    private BatchScanner fetchResults(Connector connector, String metadataTableName, Set<Authorizations> auths, int numThreads) throws TableNotFoundException {
        BatchScanner bs = ScannerHelper.createBatchScanner(connector, metadataTableName, auths, numThreads);
        bs.addScanIterator(new IteratorSetting(21, WholeRowIterator.class));
        bs.setRanges(Collections.singletonList(new Range()));
        bs.fetchColumnFamily(ColumnFamilyConstants.COLF_E);
        bs.fetchColumnFamily(ColumnFamilyConstants.COLF_I);
        bs.fetchColumnFamily(ColumnFamilyConstants.COLF_RI);
        bs.fetchColumnFamily(ColumnFamilyConstants.COLF_DESC);
        bs.fetchColumnFamily(ColumnFamilyConstants.COLF_H);
        bs.fetchColumnFamily(ColumnFamilyConstants.COLF_T);
        
        return bs;
    }
    
    private Collection<MetadataFieldBase> transformResults(Iterator<Entry<Key,Value>> iterator, Map<String,String> reverseMapping,
                    Collection<String> dataTypeFilters) {
        HashMap<String,Map<String,MetadataFieldBase>> fieldMap = Maps.newHashMap();
        boolean shouldNotFilterDataTypes = CollectionUtils.isEmpty(dataTypeFilters);
        
        final Text holder = new Text(), row = new Text();
        
        // Each Entry is the entire row
        while (iterator.hasNext()) {
            Entry<Key,Value> entry = iterator.next();
            
            try {
                // Handle batch scanner bug
                if (entry.getKey() == null && entry.getValue() == null)
                    return null;
                if (null == entry.getKey() || null == entry.getValue()) {
                    throw new IllegalArgumentException("Null key or value. Key:" + entry.getKey() + ", Value: " + entry.getValue());
                }
                
                SortedMap<Key,Value> rowEntries = WholeRowIterator.decodeRow(entry.getKey(), entry.getValue());
                for (Entry<Key,Value> rowEntry : rowEntries.entrySet()) {
                    Key key = rowEntry.getKey();
                    Value value = rowEntry.getValue();
                    key.getRow(row);
                    key.getColumnFamily(holder);
                    
                    // If event should be hidden skip
                    if (ColumnFamilyConstants.COLF_H.equals(holder)) {
                        fieldMap.remove(row.toString());
                        break;
                    }
                    
                    String cq = key.getColumnQualifier().toString();
                    int nullPos = cq.indexOf('\0');
                    String dataType = (nullPos < 0) ? cq : cq.substring(0, nullPos);
                    if (shouldNotFilterDataTypes || dataTypeFilters.contains(dataType)) {
                        MetadataFieldBase field = getFieldForDatatype(row.toString(), dataType, fieldMap);
                        
                        if (ColumnFamilyConstants.COLF_E.equals(holder)) {
                            field.setIndexOnly(false);
                            String fieldName = key.getRow().toString();
                            if (reverseMapping.containsKey(fieldName)) {
                                field.setFieldName(reverseMapping.get(fieldName));
                                field.setInternalFieldName(fieldName);
                            } else {
                                field.setFieldName(fieldName);
                            }
                            field.setLastUpdated(parseTimestamp(key.getTimestamp()));
                        } else if (ColumnFamilyConstants.COLF_I.equals(holder)) {
                            field.setForwardIndexed(true);
                        } else if (ColumnFamilyConstants.COLF_RI.equals(holder)) {
                            field.setReverseIndexed(true);
                        } else if (ColumnFamilyConstants.COLF_DESC.equals(holder)) {
                            DescriptionBase<?> desc = this.responseObjectFactory.getDescription();
                            desc.setDescription(value.toString());
                            desc.setMarkings(getMarkings(key));
                            field.getDescriptions().add(desc);
                        } else if (ColumnFamilyConstants.COLF_T.equals(holder)) {
                            field.addType(translate(cq.substring(nullPos + 1)));
                        } else {
                            log.warn("Unknown entry with key=" + key + ", value=" + value);
                        }
                        
                        // Handle index-only fields, which have no "e" entry
                        if (field.getFieldName() == null) {
                            String fieldName = key.getRow().toString();
                            field.setLastUpdated(parseTimestamp(key.getTimestamp()));
                            if (reverseMapping.containsKey(fieldName)) {
                                field.setFieldName(reverseMapping.get(fieldName));
                                field.setInternalFieldName(fieldName);
                            } else {
                                field.setFieldName(fieldName);
                            }
                        }
                    }
                    
                }
            } catch (IOException e) {
                throw new IllegalStateException("Unable to decode row " + entry.getKey());
            } catch (MarkingFunctions.Exception e) {
                throw new IllegalStateException("Unable to decode visibility " + entry.getKey(), e);
            }
        }
        
        Collection<Map<String,MetadataFieldBase>> datatypesForField = fieldMap.values();
        LinkedList<MetadataFieldBase> fields = Lists.newLinkedList();
        for (Map<String,MetadataFieldBase> value : datatypesForField) {
            fields.addAll(value.values());
        }
        
        return fields;
    }
    
    private String parseTimestamp(long inMillis) {
        return DateHelper.formatToTimeExactToSeconds(inMillis);
    }
    
    private MetadataFieldBase getFieldForDatatype(String fieldName, String dataType, HashMap<String,Map<String,MetadataFieldBase>> fieldMap) {
        MetadataFieldBase field = null;
        Map<String,MetadataFieldBase> allTypesForField = fieldMap.get(fieldName);
        
        if (null == allTypesForField) {
            allTypesForField = Maps.newHashMap();
            fieldMap.put(fieldName, allTypesForField);
        }
        
        field = allTypesForField.get(dataType);
        
        if (field == null) {
            field = new DefaultMetadataField();
            field.setIndexOnly(true); // default fields to index-only, and we'll clear if we see an "e" entry for the field
            field.setDataType(dataType);
            allTypesForField.put(dataType, field);
        }
        
        return field;
    }
    
    public String translate(String normalizer) {
        if (normalizerMapping.containsKey(normalizer)) {
            String type = normalizerMapping.get(normalizer);
            if (null != type) {
                return type;
            }
        }
        
        return "Unknown";
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see datawave.webservice.datadictionary.DatawaveDataDictionary#setDescription(org.apache.accumulo.core.client.Connector, java.lang.String, java.util.Set,
     * java.lang.String, java.lang.String, datawave.webservice.datadictionary.FieldDescription)
     */
    @Override
    public void setDescription(Connector connector, String metadataTableName, Set<Authorizations> auths, String modelName, String modelTableName,
                    DictionaryFieldBase description) throws Exception {
        this.setDescription(connector, metadataTableName, auths, modelName, modelTableName, description.getFieldName(), description.getDatatype(),
                        description.getDescriptions());
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see datawave.webservice.datadictionary.DatawaveDataDictionary#setDescription(org.apache.accumulo.core.client.Connector, java.lang.String, java.util.Set,
     * java.lang.String, java.lang.String, java.lang.String, java.lang.String, datawave.webservice.results.datadictionary.Description)
     */
    @Override
    public void setDescription(Connector connector, String metadataTableName, Set<Authorizations> auths, String modelName, String modelTable, String fieldName,
                    String datatype, DescriptionBase desc) throws Exception {
        setDescription(connector, metadataTableName, auths, modelName, modelTable, fieldName, datatype, Collections.singleton(desc));
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see datawave.webservice.datadictionary.DatawaveDataDictionary#setDescription(org.apache.accumulo.core.client.Connector, java.lang.String, java.util.Set,
     * java.lang.String, java.lang.String, java.lang.String)
     */
    @Override
    public void setDescription(Connector connector, String metadataTableName, Set<Authorizations> auths, String modelName, String modelTable, String fieldName,
                    String datatype, Set<DescriptionBase> descs) throws Exception {
        MetadataHelperWithDescriptions helper = MetadataHelperWithDescriptions.getInstance(connector, metadataTableName, allMetadataAuths.iterator().next(),
                        auths);
        
        QueryModel model = helper.getQueryModel(modelTable, modelName);
        Map<String,String> mapping = reverseReverseMapping(model);
        String alias = mapping.get(fieldName);
        
        if (null != alias) {
            fieldName = alias;
        }
        
        // TODO The query model is effectively busted because it doesn't uniquely reference field+datatype
        MetadataEntry mentry = new MetadataEntry(fieldName, datatype);
        
        helper.setDescriptions(mentry, descs);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see datawave.webservice.datadictionary.DatawaveDataDictionary#getDescriptions(org.apache.accumulo.core.client.Connector, java.lang.String,
     * java.util.Set, int)
     */
    @Override
    public Multimap<Entry<String,String>,DescriptionBase> getDescriptions(Connector connector, String metadataTableName, Set<Authorizations> auths,
                    String modelName, String modelTable) throws Exception {
        MetadataHelperWithDescriptions helper = MetadataHelperWithDescriptions.getInstance(connector, metadataTableName, allMetadataAuths.iterator().next(),
                        auths);
        QueryModel model = helper.getQueryModel(modelTable, modelName);
        Map<String,String> mapping = model.getReverseQueryMapping();
        
        Multimap<MetadataEntry,DescriptionBase> descriptions = helper.getDescriptions((Set<String>) null);
        Multimap<Entry<String,String>,DescriptionBase> tformDescriptions = HashMultimap.create();
        
        for (Entry<MetadataEntry,DescriptionBase> entry : descriptions.entries()) {
            MetadataEntry mentry = entry.getKey();
            
            String alias = mapping.get(mentry.getFieldName());
            
            if (null == alias) {
                // TODO The query model is effectively busted because it doesn't uniquely reference field+datatype
                tformDescriptions.put(entry.getKey().toEntry(), entry.getValue());
            } else {
                tformDescriptions.put(Maps.immutableEntry(alias, mentry.getDatatype()), entry.getValue());
            }
        }
        
        return tformDescriptions;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see datawave.webservice.datadictionary.DatawaveDataDictionary#getDescriptions(org.apache.accumulo.core.client.Connector, java.lang.String,
     * java.util.Set, int, java.lang.String)
     */
    @Override
    public Multimap<Entry<String,String>,DescriptionBase> getDescriptions(Connector connector, String metadataTableName, Set<Authorizations> auths,
                    String modelName, String modelTable, String datatype) throws Exception {
        MetadataHelperWithDescriptions helper = MetadataHelperWithDescriptions.getInstance(connector, metadataTableName, allMetadataAuths.iterator().next(),
                        auths);
        QueryModel model = helper.getQueryModel(modelTable, modelName);
        Map<String,String> mapping = model.getReverseQueryMapping();
        
        Multimap<MetadataEntry,DescriptionBase> descriptions = helper.getDescriptions(datatype);
        Multimap<Entry<String,String>,DescriptionBase> tformDescriptions = HashMultimap.create();
        
        for (Entry<MetadataEntry,DescriptionBase> entry : descriptions.entries()) {
            MetadataEntry mentry = entry.getKey();
            
            String alias = mapping.get(mentry.getFieldName());
            
            if (null == alias) {
                // TODO The query model is effectively busted because it doesn't uniquely reference field+datatype
                tformDescriptions.put(entry.getKey().toEntry(), entry.getValue());
            } else {
                tformDescriptions.put(Maps.immutableEntry(alias, mentry.getDatatype()), entry.getValue());
            }
        }
        
        return tformDescriptions;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see datawave.webservice.datadictionary.DatawaveDataDictionary#getDescription(org.apache.accumulo.core.client.Connector, java.lang.String, java.util.Set,
     * int, java.lang.String, java.lang.String)
     */
    @Override
    public Set<? extends DescriptionBase> getDescriptions(Connector connector, String metadataTableName, Set<Authorizations> auths, String modelName,
                    String modelTable, String fieldName, String datatype) throws Exception {
        MetadataHelperWithDescriptions helper = MetadataHelperWithDescriptions.getInstance(connector, metadataTableName, allMetadataAuths.iterator().next(),
                        auths);
        QueryModel model = helper.getQueryModel(modelTable, modelName);
        Map<String,String> mapping = reverseReverseMapping(model);
        
        String alias = mapping.get(fieldName);
        
        if (null == alias) {
            return helper.getDescriptions(fieldName, datatype);
        } else {
            return helper.getDescriptions(alias, datatype);
        }
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see datawave.webservice.datadictionary.DatawaveDataDictionary#deleteDescription(org.apache.accumulo.core.client.Connector, java.lang.String,
     * java.util.Set, java.lang.String, java.lang.String)
     */
    @Override
    public void deleteDescription(Connector connector, String metadataTableName, Set<Authorizations> auths, String modelName, String modelTable,
                    String fieldName, String datatype, DescriptionBase desc) throws Exception {
        MetadataHelperWithDescriptions helper = MetadataHelperWithDescriptions.getInstance(connector, metadataTableName, allMetadataAuths.iterator().next(),
                        auths);
        QueryModel model = helper.getQueryModel(modelTable, modelName);
        Map<String,String> mapping = reverseReverseMapping(model);
        
        String alias = mapping.get(fieldName);
        
        if (null == alias) {
            helper.removeDescription(new MetadataEntry(fieldName, datatype), desc);
        } else {
            helper.removeDescription(new MetadataEntry(alias, datatype), desc);
        }
        
    }
    
    private Map<String,String> reverseReverseMapping(QueryModel model) {
        Map<String,String> reverseMapping = model.getReverseQueryMapping();
        Map<String,String> reversed = Maps.newHashMap();
        
        for (Entry<String,String> entry : reverseMapping.entrySet()) {
            reversed.put(entry.getValue(), entry.getKey());
        }
        return reversed;
    }
    
    private Map<String,String> getMarkings(Key k) throws MarkingFunctions.Exception {
        return getMarkings(k.getColumnVisibilityParsed());
    }
    
    private Map<String,String> getMarkings(ColumnVisibility visibility) throws MarkingFunctions.Exception {
        return markingFunctions.translateFromColumnVisibility(visibility);
    }
}
