package datawave.microservice.dictionary.data;

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

import com.google.common.collect.HashMultimap;
import datawave.data.ColumnFamilyConstants;
import datawave.marking.MarkingFunctions;
import datawave.microservice.dictionary.config.ResponseObjectFactory;
import datawave.microservice.metadata.MetadataDescriptionsHelper;
import datawave.microservice.metadata.MetadataDescriptionsHelperFactory;
import datawave.query.model.QueryModel;
import datawave.query.util.MetadataEntry;
import datawave.query.util.MetadataHelper;
import datawave.query.util.MetadataHelperFactory;
import datawave.security.util.ScannerHelper;
import datawave.webservice.query.result.metadata.DefaultMetadataField;
import datawave.webservice.results.datadictionary.DefaultDataDictionary;
import datawave.webservice.results.datadictionary.DefaultDescription;
import datawave.webservice.results.datadictionary.DefaultDictionaryField;

import datawave.webservice.results.datadictionary.DefaultFields;
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
import org.apache.hadoop.io.Text;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

public class DatawaveDataDictionaryImpl implements DatawaveDataDictionary<DefaultMetadataField,DefaultDescription,DefaultDictionaryField> {
    private static final Logger log = LoggerFactory.getLogger(DatawaveDataDictionaryImpl.class);
    
    private static final String DATE_FORMAT_STRING_TO_SECONDS = "yyyyMMddHHmmss";
    private static final DateTimeFormatter DTF_Seconds = DateTimeFormat.forPattern(DATE_FORMAT_STRING_TO_SECONDS);
    
    private Map<String,String> normalizerMapping = Maps.newHashMap();
    
    private final MarkingFunctions markingFunctions;
    private final ResponseObjectFactory<DefaultDescription,DefaultDataDictionary,DefaultMetadataField,DefaultDictionaryField,DefaultFields> responseObjectFactory;
    private final MetadataHelperFactory metadataHelperFactory;
    private final MetadataDescriptionsHelperFactory<DefaultDescription> metadataDescriptionsHelperFactory;
    
    public DatawaveDataDictionaryImpl(MarkingFunctions markingFunctions,
                    ResponseObjectFactory<DefaultDescription,DefaultDataDictionary,DefaultMetadataField,DefaultDictionaryField,DefaultFields> responseObjectFactory,
                    MetadataHelperFactory metadataHelperFactory, MetadataDescriptionsHelperFactory<DefaultDescription> metadataDescriptionsHelperFactory) {
        this.markingFunctions = markingFunctions;
        this.responseObjectFactory = responseObjectFactory;
        this.metadataHelperFactory = metadataHelperFactory;
        this.metadataDescriptionsHelperFactory = metadataDescriptionsHelperFactory;
    }
    
    @Override
    public Map<String,String> getNormalizerMapping() {
        return normalizerMapping;
    }
    
    @Override
    public void setNormalizerMapping(Map<String,String> normalizerMapping) {
        this.normalizerMapping = normalizerMapping;
    }
    
    /*
     * (non-Javadoc)
     *
     * Note: dataTypeFilters can be empty, which means all the fields will be returned
     */
    @Override
    public Collection<DefaultMetadataField> getFields(String modelName, String modelTableName, String metadataTableName, Collection<String> dataTypeFilters,
                    Connector connector, Set<Authorizations> auths, int numThreads) throws Exception {
        // Get a MetadataHelper
        MetadataHelper metadataHelper = metadataHelperFactory.createMetadataHelper(connector, metadataTableName, auths);
        // So we can get a QueryModel
        QueryModel queryModel = metadataHelper.getQueryModel(modelTableName, modelName, metadataHelper.getIndexOnlyFields(null));
        
        Map<String,String> reverseMapping;
        
        // So we can pull the reverse mapping for this model
        if (null != queryModel) {
            reverseMapping = queryModel.getReverseQueryMapping();
        } else {
            reverseMapping = Collections.emptyMap();
        }
        
        // Fetch the results from Accumulo
        BatchScanner bs = fetchResults(connector, metadataTableName, auths, numThreads);
        
        // Convert them into a response object
        Collection<DefaultMetadataField> fields = transformResults(bs.iterator(), reverseMapping, dataTypeFilters);
        
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
        bs.fetchColumnFamily(ColumnFamilyConstants.COLF_TF);
        
        return bs;
    }
    
    private Collection<DefaultMetadataField> transformResults(Iterator<Entry<Key,Value>> iterator, Map<String,String> reverseMapping,
                    Collection<String> dataTypeFilters) {
        HashMap<String,Map<String,DefaultMetadataField>> fieldMap = Maps.newHashMap();
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
                        DefaultMetadataField field = getFieldForDatatype(row.toString(), dataType, fieldMap);
                        
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
                            DefaultDescription desc = this.responseObjectFactory.getDescription();
                            desc.setDescription(value.toString());
                            desc.setMarkings(getMarkings(key));
                            field.getDescriptions().add(desc);
                        } else if (ColumnFamilyConstants.COLF_T.equals(holder)) {
                            field.addType(translate(cq.substring(nullPos + 1)));
                        } else if (ColumnFamilyConstants.COLF_TF.equals(holder)) {
                            field.setTokenized(true);
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
        
        Collection<Map<String,DefaultMetadataField>> datatypesForField = fieldMap.values();
        LinkedList<DefaultMetadataField> fields = Lists.newLinkedList();
        for (Map<String,DefaultMetadataField> value : datatypesForField) {
            fields.addAll(value.values());
        }
        
        return fields;
    }
    
    private String parseTimestamp(long inMillis) {
        return DTF_Seconds.print(inMillis);
    }
    
    private DefaultMetadataField getFieldForDatatype(String fieldName, String dataType, HashMap<String,Map<String,DefaultMetadataField>> fieldMap) {
        Map<String,DefaultMetadataField> allTypesForField = fieldMap.computeIfAbsent(fieldName, k -> Maps.newHashMap());
        
        DefaultMetadataField field = allTypesForField.get(dataType);
        
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
    
    @Override
    public void setDescription(Connector connector, String metadataTableName, Set<Authorizations> auths, String modelName, String modelTableName,
                    DefaultDictionaryField description) throws Exception {
        this.setDescription(connector, metadataTableName, auths, modelName, modelTableName, description.getFieldName(), description.getDatatype(),
                        description.getDescriptions());
    }
    
    @Override
    public void setDescription(Connector connector, String metadataTableName, Set<Authorizations> auths, String modelName, String modelTable, String fieldName,
                    String datatype, DefaultDescription desc) throws Exception {
        setDescription(connector, metadataTableName, auths, modelName, modelTable, fieldName, datatype, Collections.singleton(desc));
    }
    
    @Override
    public void setDescription(Connector connector, String metadataTableName, Set<Authorizations> auths, String modelName, String modelTable, String fieldName,
                    String datatype, Set<DefaultDescription> descs) throws Exception {
        MetadataHelper helper = metadataHelperFactory.createMetadataHelper(connector, metadataTableName, auths);
        
        QueryModel model = helper.getQueryModel(modelTable, modelName);
        Map<String,String> mapping = reverseReverseMapping(model);
        String alias = mapping.get(fieldName);
        
        if (null != alias) {
            fieldName = alias;
        }
        
        // TODO The query model is effectively busted because it doesn't uniquely reference field+datatype
        MetadataEntry mentry = new MetadataEntry(fieldName, datatype);
        
        MetadataDescriptionsHelper<DefaultDescription> descriptionsHelper = getInitializedDescriptionsHelper(connector, metadataTableName, auths);
        descriptionsHelper.setDescriptions(mentry, descs);
    }
    
    @Override
    public Multimap<Entry<String,String>,DefaultDescription> getDescriptions(Connector connector, String metadataTableName, Set<Authorizations> auths,
                    String modelName, String modelTable) throws Exception {
        MetadataHelper helper = metadataHelperFactory.createMetadataHelper(connector, metadataTableName, auths);
        QueryModel model = helper.getQueryModel(modelTable, modelName);
        Map<String,String> mapping = model.getReverseQueryMapping();
        
        MetadataDescriptionsHelper<DefaultDescription> descriptionsHelper = getInitializedDescriptionsHelper(connector, metadataTableName, auths);
        Multimap<MetadataEntry,DefaultDescription> descriptions = descriptionsHelper.getDescriptions((Set<String>) null);
        Multimap<Entry<String,String>,DefaultDescription> tformDescriptions = HashMultimap.create();
        
        for (Entry<MetadataEntry,DefaultDescription> entry : descriptions.entries()) {
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
    
    @Override
    public Multimap<Entry<String,String>,DefaultDescription> getDescriptions(Connector connector, String metadataTableName, Set<Authorizations> auths,
                    String modelName, String modelTable, String datatype) throws Exception {
        MetadataHelper helper = metadataHelperFactory.createMetadataHelper(connector, metadataTableName, auths);
        QueryModel model = helper.getQueryModel(modelTable, modelName);
        Map<String,String> mapping = model.getReverseQueryMapping();
        
        MetadataDescriptionsHelper<DefaultDescription> descriptionsHelper = getInitializedDescriptionsHelper(connector, metadataTableName, auths);
        Multimap<MetadataEntry,DefaultDescription> descriptions = descriptionsHelper.getDescriptions(datatype);
        Multimap<Entry<String,String>,DefaultDescription> tformDescriptions = HashMultimap.create();
        
        for (Entry<MetadataEntry,DefaultDescription> entry : descriptions.entries()) {
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
    
    @Override
    public Set<DefaultDescription> getDescriptions(Connector connector, String metadataTableName, Set<Authorizations> auths, String modelName,
                    String modelTable, String fieldName, String datatype) throws Exception {
        MetadataHelper helper = metadataHelperFactory.createMetadataHelper(connector, metadataTableName, auths);
        QueryModel model = helper.getQueryModel(modelTable, modelName);
        Map<String,String> mapping = reverseReverseMapping(model);
        
        String alias = mapping.get(fieldName);
        
        MetadataDescriptionsHelper<DefaultDescription> descriptionsHelper = getInitializedDescriptionsHelper(connector, metadataTableName, auths);
        if (null == alias) {
            return descriptionsHelper.getDescriptions(fieldName, datatype);
        } else {
            return descriptionsHelper.getDescriptions(alias, datatype);
        }
    }
    
    @Override
    public void deleteDescription(Connector connector, String metadataTableName, Set<Authorizations> auths, String modelName, String modelTable,
                    String fieldName, String datatype, DefaultDescription desc) throws Exception {
        MetadataHelper helper = metadataHelperFactory.createMetadataHelper(connector, metadataTableName, auths);
        QueryModel model = helper.getQueryModel(modelTable, modelName);
        Map<String,String> mapping = reverseReverseMapping(model);
        
        String alias = mapping.get(fieldName);
        
        MetadataDescriptionsHelper<DefaultDescription> descriptionsHelper = getInitializedDescriptionsHelper(connector, metadataTableName, auths);
        if (null == alias) {
            descriptionsHelper.removeDescription(new MetadataEntry(fieldName, datatype), desc);
        } else {
            descriptionsHelper.removeDescription(new MetadataEntry(alias, datatype), desc);
        }
    }
    
    private MetadataDescriptionsHelper<DefaultDescription> getInitializedDescriptionsHelper(Connector connector, String metadataTableName,
                    Set<Authorizations> auths) {
        MetadataDescriptionsHelper<DefaultDescription> helper = metadataDescriptionsHelperFactory.createMetadataDescriptionsHelper();
        helper.initialize(connector, metadataTableName, auths);
        return helper;
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
