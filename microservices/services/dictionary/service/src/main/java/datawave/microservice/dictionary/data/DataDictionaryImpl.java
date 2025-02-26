package datawave.microservice.dictionary.data;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.TableNotFoundException;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

import datawave.marking.MarkingFunctions;
import datawave.microservice.Connection;
import datawave.microservice.dictionary.config.ResponseObjectFactory;
import datawave.microservice.metadata.DefaultMetadataFieldScanner;
import datawave.microservice.metadata.MetadataDescriptionsHelper;
import datawave.microservice.metadata.MetadataDescriptionsHelperFactory;
import datawave.query.model.QueryModel;
import datawave.query.util.MetadataEntry;
import datawave.query.util.MetadataHelper;
import datawave.query.util.MetadataHelperFactory;
import datawave.webservice.dictionary.data.DefaultDataDictionary;
import datawave.webservice.dictionary.data.DefaultDescription;
import datawave.webservice.dictionary.data.DefaultDictionaryField;
import datawave.webservice.dictionary.data.DefaultFields;
import datawave.webservice.metadata.DefaultMetadataField;

public class DataDictionaryImpl implements DataDictionary<DefaultMetadataField,DefaultDescription,DefaultDictionaryField> {
    
    private final MarkingFunctions markingFunctions;
    private final ResponseObjectFactory<DefaultDescription,DefaultDataDictionary,DefaultMetadataField,DefaultDictionaryField,DefaultFields> responseObjectFactory;
    private final MetadataHelperFactory metadataHelperFactory;
    private final MetadataDescriptionsHelperFactory<DefaultDescription> metadataDescriptionsHelperFactory;
    private Map<String,String> normalizationMap = Maps.newHashMap();
    
    public DataDictionaryImpl(MarkingFunctions markingFunctions,
                    ResponseObjectFactory<DefaultDescription,DefaultDataDictionary,DefaultMetadataField,DefaultDictionaryField,DefaultFields> responseObjectFactory,
                    MetadataHelperFactory metadataHelperFactory, MetadataDescriptionsHelperFactory<DefaultDescription> metadataDescriptionsHelperFactory) {
        this.markingFunctions = markingFunctions;
        this.responseObjectFactory = responseObjectFactory;
        this.metadataHelperFactory = metadataHelperFactory;
        this.metadataDescriptionsHelperFactory = metadataDescriptionsHelperFactory;
    }
    
    @Override
    public Map<String,String> getNormalizationMap() {
        return normalizationMap;
    }
    
    @Override
    public void setNormalizationMap(Map<String,String> normalizationMap) {
        this.normalizationMap = normalizationMap;
    }
    
    /**
     * Retrieve metadata fields from the specified metadata table, aggregated by field name and data type.
     *
     * <p>
     *
     * If no data types are specified, then all metadata fields are returned. Otherwise, only metadata fields with one of the specified data types will be
     * returned.
     *
     * @param connectionConfig
     *            the connection configuration to use when connecting to accumulo
     * @param dataTypeFilters
     *            the set of data types to filter on
     * @param numThreads
     *            the number of threads to use when scanning the metadata table
     * @return a collection of metadata fields
     */
    @Override
    public Collection<DefaultMetadataField> getFields(Connection connectionConfig, Collection<String> dataTypeFilters, int numThreads) throws Exception {
        Map<String,String> aliases = getAliases(connectionConfig);
        DefaultMetadataFieldScanner scanner = new DefaultMetadataFieldScanner(markingFunctions, responseObjectFactory, normalizationMap, connectionConfig,
                        numThreads);
        return scanner.getFields(aliases, dataTypeFilters);
    }
    
    /**
     * Set the specified description to the metadata table for the field name and data type combination supplied by the description.
     *
     * <p>
     *
     * If an alias exists for the given field name, that alias will be used when adding the description.
     *
     * @param connectionConfig
     *            the connection configuration to use when connecting to accumulo
     * @param description
     *            the description to add
     */
    @Override
    public void setDescription(Connection connectionConfig, DefaultDictionaryField description) throws Exception {
        this.setDescriptions(connectionConfig, description.getFieldName(), description.getDatatype(), description.getDescriptions());
    }
    
    /**
     * Set the specified description to the metadata table for the specified field name and data type combination.
     *
     * <p>
     *
     * If an alias exists for the given field name, that alias will be used when adding the description.
     *
     * @param connectionConfig
     *            the connection configuration to use when connecting to accumulo
     * @param fieldName
     *            the field name
     * @param datatype
     *            the data type
     * @param description
     *            the description to add
     */
    @Override
    public void setDescription(Connection connectionConfig, String fieldName, String datatype, DefaultDescription description) throws Exception {
        setDescriptions(connectionConfig, fieldName, datatype, Collections.singleton(description));
    }
    
    /**
     * Set the specified descriptions to the metadata table for the specified field name and data type combination.
     *
     * <p>
     *
     * If an alias exists for the given field name, that alias will be used when adding the descriptions.
     *
     * @param connectionConfig
     *            the connection configuration to use when connecting to accumulo
     * @param fieldName
     *            the field name
     * @param datatype
     *            the data type
     * @param descriptions
     *            the descriptions to add
     */
    @Override
    public void setDescriptions(Connection connectionConfig, String fieldName, String datatype, Set<DefaultDescription> descriptions) throws Exception {
        // TODO - Consider skipping this. We check for an alias when retrieving descriptions or metadata fields anyway, rendering this unnecessary.
        String alias = getAlias(fieldName, connectionConfig);
        if (null != alias) {
            fieldName = alias;
        }
        
        // TODO The query model is effectively busted because it doesn't uniquely reference field+datatype
        MetadataEntry mentry = new MetadataEntry(fieldName, datatype);
        MetadataDescriptionsHelper<DefaultDescription> helper = getInitializedDescriptionsHelper(connectionConfig);
        helper.setDescriptions(mentry, descriptions);
    }
    
    /**
     * Retrieve all descriptions for metadata field entries.
     *
     * <p>
     *
     * If an alias exists for the given field name, that alias will be used when attempting to retrieve the descriptions.
     *
     * @param connectionConfig
     *            the connection configuration to use when connecting to accumulo
     * @return a {@link Multimap} with {@literal <fieldName, dataType>} entry keys mapped to their associated descriptions
     */
    @Override
    public Multimap<Entry<String,String>,DefaultDescription> getDescriptions(Connection connectionConfig) throws Exception {
        MetadataDescriptionsHelper<DefaultDescription> descriptionsHelper = getInitializedDescriptionsHelper(connectionConfig);
        Multimap<MetadataEntry,DefaultDescription> descriptions = descriptionsHelper.getDescriptions((Set<String>) null);
        return transformKeys(descriptions, connectionConfig);
    }
    
    /**
     * Retrieves all descriptions for metadata entries with the specified data type.
     *
     * <p>
     *
     * If an alias exists for the given field name, that alias will be used when attempting to retrieve the descriptions.
     *
     * @param connectionConfig
     *            the connection configuration to use when connecting to accumulo
     * @param datatype
     *            the data type
     * @return a {@link Multimap} with {@literal <fieldName, dataType>} entry keys mapped to their associated descriptions
     */
    @Override
    public Multimap<Entry<String,String>,DefaultDescription> getDescriptions(Connection connectionConfig, String datatype) throws Exception {
        MetadataDescriptionsHelper<DefaultDescription> helper = getInitializedDescriptionsHelper(connectionConfig);
        Multimap<MetadataEntry,DefaultDescription> descriptions = helper.getDescriptions(datatype);
        return transformKeys(descriptions, connectionConfig);
    }
    
    /**
     * Retrieves all descriptions for metadata entries with the specified field name and data type combination.
     *
     * <p>
     *
     * If an alias exists for the given field name, that alias will be used when attempting to retrieve the descriptions.
     *
     * @param connectionConfig
     *            the connection configuration to use when connecting to accumulo
     * @param fieldName
     *            the field name
     * @param datatype
     *            the data type
     * @return the descriptions
     */
    @Override
    public Set<DefaultDescription> getDescriptions(Connection connectionConfig, String fieldName, String datatype) throws Exception {
        String alias = getAlias(fieldName, connectionConfig);
        MetadataDescriptionsHelper<DefaultDescription> helper = getInitializedDescriptionsHelper(connectionConfig);
        return alias == null ? helper.getDescriptions(fieldName, datatype) : helper.getDescriptions(alias, datatype);
    }
    
    /**
     * Deletes the specified description for the specified field name and data type combination.
     *
     * <p>
     *
     * If an alias exists for the given field name, that alias will be used when attempting to delete the description.
     *
     * @param connectionConfig
     *            the connection configuration to use when connecting to accumulo
     * @param fieldName
     *            the field name
     * @param datatype
     *            the data type
     * @param description
     *            the description to delete
     */
    @Override
    public void deleteDescription(Connection connectionConfig, String fieldName, String datatype, DefaultDescription description) throws Exception {
        String alias = getAlias(fieldName, connectionConfig);
        if (alias != null) {
            fieldName = alias;
        }
        MetadataDescriptionsHelper<DefaultDescription> descriptionsHelper = getInitializedDescriptionsHelper(connectionConfig);
        descriptionsHelper.removeDescription(new MetadataEntry(fieldName, datatype), description);
    }
    
    // Transform the MetadataEntry key of the specified map into <fieldName,dataType> entries.
    // If an alias exists for a field name, that alias will be returned instead of the field name.
    private Multimap<Entry<String,String>,DefaultDescription> transformKeys(Multimap<MetadataEntry,DefaultDescription> descriptions,
                    Connection connectionConfig) throws ExecutionException, TableNotFoundException {
        Map<String,String> aliases = getAliases(connectionConfig);
        Multimap<Entry<String,String>,DefaultDescription> transformedDescriptions = HashMultimap.create();
        for (Entry<MetadataEntry,DefaultDescription> entry : descriptions.entries()) {
            MetadataEntry metadataEntry = entry.getKey();
            String alias = aliases.get(metadataEntry.getFieldName());
            if (alias == null) {
                // TODO The query model is effectively busted because it doesn't uniquely reference field+datatype
                transformedDescriptions.put(metadataEntry.toEntry(), entry.getValue());
            } else {
                transformedDescriptions.put(Maps.immutableEntry(alias, metadataEntry.getDatatype()), entry.getValue());
            }
        }
        return transformedDescriptions;
    }
    
    // Return a new, initialized metadata description helper.
    private MetadataDescriptionsHelper<DefaultDescription> getInitializedDescriptionsHelper(Connection connectionConfig) {
        MetadataDescriptionsHelper<DefaultDescription> helper = metadataDescriptionsHelperFactory.createMetadataDescriptionsHelper();
        helper.initialize(connectionConfig.getAccumuloClient(), connectionConfig.getMetadataTable(), connectionConfig.getAuths());
        return helper;
    }
    
    // Return the alias map for the query model in the specified connection config.
    private Map<String,String> getAliases(Connection connectionConfig) throws ExecutionException, TableNotFoundException {
        MetadataHelper helper = metadataHelperFactory.createMetadataHelper(connectionConfig.getAccumuloClient(), connectionConfig.getMetadataTable(),
                        connectionConfig.getAuths());
        QueryModel model = helper.getQueryModel(connectionConfig.getModelTable(), connectionConfig.getModelName());
        return model != null ? model.getReverseQueryMapping() : Collections.emptyMap();
    }
    
    // Retrieve the alias for the specified field name from the alias map for the specified field name.
    private String getAlias(String fieldName, Connection connectionConfig) throws ExecutionException, TableNotFoundException {
        Map<String,String> map = getAliases(connectionConfig);
        Map<String,String> reversedMap = map.entrySet().stream().collect(Collectors.toMap(Entry::getValue, Entry::getKey));
        return reversedMap.get(fieldName);
    }
}
