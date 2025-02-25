package datawave.microservice.dictionary;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.commons.lang.StringUtils;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

import datawave.microservice.AccumuloConnectionService;
import datawave.microservice.Connection;
import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.microservice.dictionary.config.DataDictionaryProperties;
import datawave.microservice.dictionary.config.ResponseObjectFactory;
import datawave.microservice.dictionary.data.DataDictionary;
import datawave.webservice.dictionary.data.DataDictionaryBase;
import datawave.webservice.dictionary.data.DescriptionBase;
import datawave.webservice.dictionary.data.DictionaryFieldBase;
import datawave.webservice.dictionary.data.FieldsBase;
import datawave.webservice.metadata.MetadataFieldBase;
import datawave.webservice.result.VoidResponse;

public class DataDictionaryControllerLogic<DESC extends DescriptionBase<DESC>,DICT extends DataDictionaryBase<DICT,META>,META extends MetadataFieldBase<META,DESC>,FIELD extends DictionaryFieldBase<FIELD,DESC>,FIELDS extends FieldsBase<FIELDS,FIELD,DESC>> {
    private final DataDictionaryProperties dataDictionaryConfiguration;
    private final DataDictionary<META,DESC,FIELD> dataDictionary;
    private final ResponseObjectFactory<DESC,DICT,META,FIELD,FIELDS> responseObjectFactory;
    private final AccumuloConnectionService accumuloConnectionService;
    
    private final Consumer<META> TRANSFORM_EMPTY_INTERNAL_FIELD_NAMES = meta -> {
        if (meta.getInternalFieldName() == null || meta.getInternalFieldName().isEmpty()) {
            meta.setInternalFieldName(meta.getFieldName());
        }
    };
    
    public DataDictionaryControllerLogic(DataDictionaryProperties dataDictionaryConfiguration, DataDictionary<META,DESC,FIELD> dataDictionary,
                    ResponseObjectFactory<DESC,DICT,META,FIELD,FIELDS> responseObjectFactory, AccumuloConnectionService accumloConnectionService) {
        this.dataDictionaryConfiguration = dataDictionaryConfiguration;
        this.dataDictionary = dataDictionary;
        this.responseObjectFactory = responseObjectFactory;
        this.accumuloConnectionService = accumloConnectionService;
        dataDictionary.setNormalizationMap(dataDictionaryConfiguration.getNormalizerMap());
    }
    
    /**
     * Returns the DataDictionary for the given parameters.
     *
     * @param modelName
     *            Optional model name
     * @param modelTableName
     *            Optional model table name
     * @param metadataTableName
     *            Optional metadata table name
     * @param queryAuthorizations
     *            Optional query authorizations
     * @param dataTypeFilters
     *            Optional data type filters
     * @param currentUser
     *            the current user
     * @return the DataDictionaryBase class (extended) that contains the data dictionary fields
     * @throws Exception
     *             if there is any problem fetching the entries
     */
    public DataDictionaryBase<DICT,META> get(String modelName, String modelTableName, String metadataTableName, String queryAuthorizations,
                    String dataTypeFilters, DatawaveUserDetails currentUser) throws Exception {
        Connection connection = accumuloConnectionService.getConnection(metadataTableName, modelTableName, modelName, currentUser);
        // If the user provides authorizations, intersect it with their actual authorizations
        connection.setAuths(accumuloConnectionService.getDowngradedAuthorizations(queryAuthorizations, currentUser));
        
        Collection<String> dataTypes = (StringUtils.isBlank(dataTypeFilters) ? Collections.emptyList() : Arrays.asList(dataTypeFilters.split(",")));
        
        Collection<META> fields = dataDictionary.getFields(connection, dataTypes, dataDictionaryConfiguration.getNumThreads());
        DICT dataDictionary = responseObjectFactory.getDataDictionary();
        dataDictionary.setFields(fields);
        // Ensure that empty internal field names will be set to the field name instead.
        dataDictionary.transformFields(TRANSFORM_EMPTY_INTERNAL_FIELD_NAMES);
        
        return dataDictionary;
    }
    
    /**
     * Upload a collection of descriptions to load into the database. Apply a query model to the provided FieldDescriptions before storing.
     *
     * @param fields
     *            a FieldDescriptions to load
     * @param modelName
     *            Optional model name
     * @param modelTable
     *            Optional model table name
     * @param currentUser
     *            The user sending the request
     * @return a VoidResponse
     * @throws Exception
     *             if there is any problem uploading the descriptions
     */
    public VoidResponse uploadDescriptions(FIELDS fields, String modelName, String modelTable, DatawaveUserDetails currentUser) throws Exception {
        Connection connection = accumuloConnectionService.getConnection(modelTable, modelName, currentUser);
        List<FIELD> list = fields.getFields();
        for (FIELD desc : list) {
            dataDictionary.setDescription(connection, desc);
        }
        
        // TODO: reload model table cache?
        // cache.reloadCache(modelTable);
        
        return new VoidResponse();
    }
    
    /**
     * Set a description for a field in a datatype, optionally applying a model to the field name.
     *
     * @param fieldName
     *            Name of field
     * @param datatype
     *            Name of datatype
     * @param description
     *            Description of field
     * @param modelName
     *            Optional model name
     * @param modelTable
     *            Optional model table name
     * @param columnVisibility
     *            ColumnVisibility of the description
     * @param currentUser
     *            The user sending the request
     * @return a VoidResponse
     * @throws Exception
     *             if there is any problem updating the description
     */
    public VoidResponse setDescriptionPut(String fieldName, String datatype, String description, String modelName, String modelTable, String columnVisibility,
                    DatawaveUserDetails currentUser) throws Exception {
        return setDescriptionPost(fieldName, datatype, description, modelName, modelTable, columnVisibility, currentUser);
    }
    
    /**
     * Set a description for a field in a datatype, optionally applying a model to the field name.
     *
     * @param fieldName
     *            Name of field
     * @param datatype
     *            Name of datatype
     * @param description
     *            Description of field
     * @param modelName
     *            Optional model name
     * @param modelTable
     *            Optional model table name
     * @param columnVisibility
     *            ColumnVisibility of the description
     * @param currentUser
     *            The user sending the request
     * @return Description of fields
     * @throws Exception
     *             if there is any problem updating the dictionary item description
     */
    public VoidResponse setDescriptionPost(String fieldName, String datatype, String description, String modelName, String modelTable, String columnVisibility,
                    DatawaveUserDetails currentUser) throws Exception {
        DESC desc = this.responseObjectFactory.getDescription();
        Map<String,String> markings = Maps.newHashMap();
        markings.put("columnVisibility", columnVisibility);
        desc.setMarkings(markings);
        desc.setDescription(description);
        
        Connection connection = accumuloConnectionService.getConnection(modelTable, modelName, currentUser);
        dataDictionary.setDescription(connection, fieldName, datatype, desc);
        
        // TODO: reload model table cache?
        // cache.reloadCache(modelTable);
        
        return new VoidResponse();
    }
    
    /**
     * Fetch all descriptions stored in the database, optionally applying a model.
     *
     * @param modelName
     *            Optional model name
     * @param modelTable
     *            Optional model table name
     * @param currentUser
     *            The user sending the request
     * @return the dictionary descriptions
     * @throws Exception
     *             if there is any problem retrieving the descriptions from Accumulo
     */
    public FIELDS allDescriptions(String modelName, String modelTable, DatawaveUserDetails currentUser) throws Exception {
        Connection connection = accumuloConnectionService.getConnection(modelTable, modelName, currentUser);
        Multimap<Map.Entry<String,String>,DESC> descriptions = dataDictionary.getDescriptions(connection);
        FIELDS response = this.responseObjectFactory.getFields();
        response.setDescriptions(descriptions);
        
        return response;
    }
    
    /**
     * Fetch all descriptions for a datatype, optionally applying a model to the field names.
     *
     * @param datatype
     *            Name of datatype
     * @param modelName
     *            Optional model name
     * @param modelTable
     *            Optional model table name
     * @param currentUser
     *            The user sending the request
     * @return the dictionary descriptions for {@code datatype}
     * @throws Exception
     *             if there is any problem retrieving the descriptions from Accumulo
     */
    public FIELDS datatypeDescriptions(String datatype, String modelName, String modelTable, DatawaveUserDetails currentUser) throws Exception {
        Connection connection = accumuloConnectionService.getConnection(modelTable, modelName, currentUser);
        Multimap<Map.Entry<String,String>,DESC> descriptions = dataDictionary.getDescriptions(connection, datatype);
        FIELDS response = this.responseObjectFactory.getFields();
        response.setDescriptions(descriptions);
        
        return response;
    }
    
    /**
     * Fetch the description for a field in a datatype, optionally applying a model.
     *
     * @param fieldName
     *            Name of field
     * @param datatype
     *            Name of datatype
     * @param modelName
     *            Optional model name
     * @param modelTable
     *            Optional model table name
     * @param currentUser
     *            The user sending the request
     * @return the dictionary descriptions for field {@code fieldName} in the type {@code dataType}
     * @throws Exception
     *             if there is any problem retrieving the descriptions from Accumulo
     */
    public FIELDS fieldNameDescription(String fieldName, String datatype, String modelName, String modelTable, DatawaveUserDetails currentUser)
                    throws Exception {
        Connection connection = accumuloConnectionService.getConnection(modelTable, modelName, currentUser);
        Set<DESC> descriptions = dataDictionary.getDescriptions(connection, fieldName, datatype);
        FIELDS response = responseObjectFactory.getFields();
        if (!descriptions.isEmpty()) {
            Multimap<Map.Entry<String,String>,DESC> mmap = HashMultimap.create();
            for (DESC desc : descriptions) {
                mmap.put(Maps.immutableEntry(fieldName, datatype), desc);
            }
            response.setDescriptions(mmap);
        }
        
        return response;
    }
    
    /**
     * Delete a description for a field in a datatype, optionally applying a model to the field name.
     *
     * @param fieldName
     *            Name of field
     * @param datatype
     *            Name of datatype
     * @param modelName
     *            Optional model name
     * @param modelTable
     *            Optional model table name
     * @param columnVisibility
     *            the column visibility
     * @param currentUser
     *            The user sending the request
     * @return a VoidResponse with operation time and error information
     * @throws Exception
     *             if there is any problem removing the description from Accumulo
     */
    public VoidResponse deleteDescription(String fieldName, String datatype, String modelName, String modelTable, String columnVisibility,
                    DatawaveUserDetails currentUser) throws Exception {
        Map<String,String> markings = Maps.newHashMap();
        markings.put("columnVisibility", columnVisibility);
        DESC desc = this.responseObjectFactory.getDescription();
        desc.setDescription("");
        desc.setMarkings(markings);
        
        Connection connection = accumuloConnectionService.getConnection(modelTable, modelName, currentUser);
        dataDictionary.deleteDescription(connection, fieldName, datatype, desc);
        
        // TODO: reload model table cache?
        // cache.reloadCache(modelTable);
        
        return new VoidResponse();
    }
    
    /**
     * Sets the Banner for the Data Dictionary.
     * 
     * @return the default banner for Data Dictionary
     */
    public DataDictionaryProperties.Banner retrieveBanner() {
        return dataDictionaryConfiguration.getBanner();
    }
}
