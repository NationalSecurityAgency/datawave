package datawave.microservice.dictionary;

import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import datawave.accumulo.util.security.UserAuthFunctions;
import datawave.microservice.authorization.user.ProxiedUserDetails;
import datawave.microservice.dictionary.config.DataDictionaryProperties;
import datawave.microservice.dictionary.config.ResponseObjectFactory;
import datawave.microservice.dictionary.data.DatawaveDataDictionary;
import datawave.security.authorization.DatawaveUser;
import datawave.webservice.query.result.metadata.MetadataFieldBase;
import datawave.webservice.result.VoidResponse;
import datawave.webservice.results.datadictionary.DataDictionaryBase;
import datawave.webservice.results.datadictionary.DescriptionBase;
import datawave.webservice.results.datadictionary.DictionaryFieldBase;
import datawave.webservice.results.datadictionary.FieldsBase;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.http.MediaType;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.security.PermitAll;
import javax.annotation.security.RolesAllowed;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import static datawave.microservice.http.converter.protostuff.ProtostuffHttpMessageConverter.PROTOSTUFF_VALUE;

@PermitAll
@RestController
@RequestMapping(path = "/data/v1", produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE, MediaType.TEXT_XML_VALUE, PROTOSTUFF_VALUE,
        MediaType.TEXT_HTML_VALUE, "text/x-yaml", "application/x-yaml"})
@EnableConfigurationProperties(DataDictionaryProperties.class)
public class DataDictionaryOperations<DESC extends DescriptionBase<DESC>,DICT extends DataDictionaryBase<DICT,META>,META extends MetadataFieldBase<META,DESC>,FIELD extends DictionaryFieldBase<FIELD,DESC>,FIELDS extends FieldsBase<FIELDS,FIELD,DESC>> {
    
    private final DataDictionaryProperties dataDictionaryConfiguration;
    private final DatawaveDataDictionary<META,DESC,FIELD> dataDictionary;
    private final ResponseObjectFactory<DESC,DICT,META,FIELD,FIELDS> responseObjectFactory;
    private final UserAuthFunctions userAuthFunctions;
    private final Connector accumuloConnector;
    
    public DataDictionaryOperations(DataDictionaryProperties dataDictionaryConfiguration, DatawaveDataDictionary<META,DESC,FIELD> dataDictionary,
                    ResponseObjectFactory<DESC,DICT,META,FIELD,FIELDS> responseObjectFactory, UserAuthFunctions userAuthFunctions,
                    @Qualifier("warehouse") Connector accumuloConnector) {
        this.dataDictionaryConfiguration = dataDictionaryConfiguration;
        this.dataDictionary = dataDictionary;
        this.responseObjectFactory = responseObjectFactory;
        this.userAuthFunctions = userAuthFunctions;
        this.accumuloConnector = accumuloConnector;
        dataDictionary.setNormalizerMapping(dataDictionaryConfiguration.getNormalizerMap());
    }
    
    @ResponseBody
    @RequestMapping(path = "/")
    @Timed(name = "dw.dictionary.data.get", absolute = true)
    public DataDictionaryBase<DICT,META> get(@RequestParam(required = false) String modelName, @RequestParam(required = false) String modelTableName,
                    @RequestParam(required = false) String metadataTableName, @RequestParam(name = "auths", required = false) String queryAuthorizations,
                    @RequestParam(defaultValue = "") String dataTypeFilters, @AuthenticationPrincipal ProxiedUserDetails currentUser) throws Exception {
        if (null == modelName || StringUtils.isBlank(modelName)) {
            modelName = this.dataDictionaryConfiguration.getModelName();
        }
        
        if (null == modelTableName || StringUtils.isBlank(modelTableName)) {
            modelTableName = this.dataDictionaryConfiguration.getModelTableName();
        }
        
        if (null == metadataTableName || StringUtils.isBlank(metadataTableName)) {
            metadataTableName = this.dataDictionaryConfiguration.getMetadataTableName();
        }
        
        Collection<String> dataTypes = (StringUtils.isBlank(dataTypeFilters) ? Collections.emptyList() : Arrays.asList(dataTypeFilters.split(",")));
        
        // If the user provides authorizations, intersect it with their actual authorizations
        Set<Authorizations> auths = getDowngradedAuthorizations(queryAuthorizations, currentUser);
        Collection<META> fields = dataDictionary.getFields(modelName, modelTableName, metadataTableName, dataTypes, accumuloConnector, auths,
                        dataDictionaryConfiguration.getNumThreads());
        DICT dataDictionary = responseObjectFactory.getDataDictionary();
        dataDictionary.setFields(fields);
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
     * @return a VoidResponse
     * @throws Exception
     *             if there is any problem uploading the descriptions
     */
    @ResponseBody
    @PostMapping(path = "/Descriptions", consumes = {MediaType.APPLICATION_XML_VALUE, MediaType.TEXT_XML_VALUE, MediaType.APPLICATION_JSON_VALUE})
    @Timed(name = "dw.dictionary.data.uploadDescriptions", absolute = true)
    public VoidResponse uploadDescriptions(FIELDS fields, @RequestParam String modelName, @RequestParam String modelTable,
                    @AuthenticationPrincipal ProxiedUserDetails currentUser) throws Exception {
        if (StringUtils.isBlank(modelName)) {
            modelName = this.dataDictionaryConfiguration.getModelName();
        }
        
        if (StringUtils.isBlank(modelTable)) {
            modelTable = this.dataDictionaryConfiguration.getModelTableName();
        }
        
        Set<Authorizations> auths = getAuths(currentUser);
        List<FIELD> list = fields.getFields();
        for (FIELD desc : list) {
            dataDictionary.setDescription(accumuloConnector, dataDictionaryConfiguration.getMetadataTableName(), auths, modelName, modelTable, desc);
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
     * @return a {@link VoidResponse}
     * @throws Exception
     *             if there is any problem updating the description
     */
    @RolesAllowed({"Administrator", "JBossAdministrator"})
    @PutMapping(path = "/Descriptions/{datatype}/{fieldName}/{description}", produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE,
            MediaType.TEXT_XML_VALUE, PROTOSTUFF_VALUE, "text/x-yaml", "application/x-yaml"})
    @Timed(name = "dw.dictionary.data.setDescriptionPut", absolute = true)
    public VoidResponse setDescriptionPut(@PathVariable String fieldName, @PathVariable String datatype, @PathVariable String description,
                    @RequestParam(required = false) String modelName, @RequestParam(required = false) String modelTable, @RequestParam String columnVisibility,
                    @AuthenticationPrincipal ProxiedUserDetails currentUser) throws Exception {
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
     * @return Description of fields
     * @throws Exception
     *             if there is any problem updating the dictionary item description
     */
    @RolesAllowed({"Administrator", "JBossAdministrator"})
    @PostMapping(path = "/Descriptions", produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE, MediaType.TEXT_XML_VALUE,
            PROTOSTUFF_VALUE, "text/x-yaml", "application/x-yaml"})
    @Timed(name = "dw.dictionary.data.setDescriptionPost", absolute = true)
    public VoidResponse setDescriptionPost(@RequestParam String fieldName, @RequestParam String datatype, @RequestParam String description,
                    @RequestParam(required = false) String modelName, @RequestParam(required = false) String modelTable, @RequestParam String columnVisibility,
                    @AuthenticationPrincipal ProxiedUserDetails currentUser) throws Exception {
        if (StringUtils.isBlank(modelName)) {
            modelName = this.dataDictionaryConfiguration.getModelName();
        }
        
        if (StringUtils.isBlank(modelTable)) {
            modelTable = this.dataDictionaryConfiguration.getModelTableName();
        }
        
        Set<Authorizations> auths = getAuths(currentUser);
        DESC desc = this.responseObjectFactory.getDescription();
        Map<String,String> markings = Maps.newHashMap();
        markings.put("columnVisibility", columnVisibility);
        desc.setMarkings(markings);
        desc.setDescription(description);
        dataDictionary.setDescription(accumuloConnector, dataDictionaryConfiguration.getMetadataTableName(), auths, modelName, modelTable, fieldName, datatype,
                        desc);
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
     * @return the dictionary descriptions
     * @throws Exception
     *             if there is any problem retrieving the descriptions from Accumulo
     */
    @RequestMapping(path = "/Descriptions")
    @ResponseBody
    @Timed(name = "dw.dictionary.data.allDescriptions", absolute = true)
    public FIELDS allDescriptions(@RequestParam(required = false) String modelName, @RequestParam(required = false) String modelTable,
                    @AuthenticationPrincipal ProxiedUserDetails currentUser) throws Exception {
        if (StringUtils.isBlank(modelName)) {
            modelName = this.dataDictionaryConfiguration.getModelName();
        }
        
        if (StringUtils.isBlank(modelTable)) {
            modelTable = this.dataDictionaryConfiguration.getModelTableName();
        }
        
        Set<Authorizations> auths = getAuths(currentUser);
        Multimap<Entry<String,String>,DESC> descriptions = dataDictionary.getDescriptions(accumuloConnector,
                        this.dataDictionaryConfiguration.getMetadataTableName(), auths, modelName, modelTable);
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
     * @return the dictionary descriptions for {@code datatype}
     * @throws Exception
     *             if there is any problem retrieving the descriptions from Accumulo
     */
    @RequestMapping(path = "/Descriptions/{datatype}")
    @Timed(name = "dw.dictionary.data.datatypeDescriptions", absolute = true)
    public FieldsBase datatypeDescriptions(@PathVariable("datatype") String datatype, @RequestParam(required = false) String modelName,
                    @RequestParam(required = false) String modelTable, @AuthenticationPrincipal ProxiedUserDetails currentUser) throws Exception {
        if (StringUtils.isBlank(modelName)) {
            modelName = this.dataDictionaryConfiguration.getModelName();
        }
        
        if (StringUtils.isBlank(modelTable)) {
            modelTable = this.dataDictionaryConfiguration.getModelTableName();
        }
        
        Set<Authorizations> auths = getAuths(currentUser);
        Multimap<Entry<String,String>,DESC> descriptions = dataDictionary.getDescriptions(accumuloConnector,
                        this.dataDictionaryConfiguration.getMetadataTableName(), auths, modelName, modelTable, datatype);
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
     * @return the dictionary descriptions for field {@code fieldName} in the type {@code dataType}
     * @throws Exception
     *             if there is any problem retrieving the descriptions from Accumulo
     */
    @RequestMapping(path = "/Descriptions/{datatype}/{fieldName}")
    @Timed(name = "dw.dictionary.data.fieldNameDescription", absolute = true)
    public FieldsBase fieldNameDescription(@PathVariable String fieldName, @PathVariable String datatype, @RequestParam(required = false) String modelName,
                    @RequestParam(required = false) String modelTable, @AuthenticationPrincipal ProxiedUserDetails currentUser) throws Exception {
        if (StringUtils.isBlank(modelName)) {
            modelName = this.dataDictionaryConfiguration.getModelName();
        }
        
        if (StringUtils.isBlank(modelTable)) {
            modelTable = this.dataDictionaryConfiguration.getModelTableName();
        }
        
        Set<Authorizations> auths = getAuths(currentUser);
        Set<DESC> descriptions = dataDictionary.getDescriptions(accumuloConnector, this.dataDictionaryConfiguration.getMetadataTableName(), auths, modelName,
                        modelTable, fieldName, datatype);
        FIELDS response = responseObjectFactory.getFields();
        if (!descriptions.isEmpty()) {
            Multimap<Entry<String,String>,DESC> mmap = HashMultimap.create();
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
     * @return a {@link VoidResponse} with operation time and error information
     * @throws Exception
     *             if there is any problem removing the description from Accumulo
     */
    @RolesAllowed({"Administrator", "JBossAdministrator"})
    @DeleteMapping(path = "/Descriptions/{datatype}/{fieldName}", produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE,
            MediaType.TEXT_XML_VALUE, PROTOSTUFF_VALUE, "text/x-yaml", "application/x-yaml"})
    @Timed(name = "dw.dictionary.data.deleteDescription", absolute = true)
    public VoidResponse deleteDescription(@PathVariable String fieldName, @PathVariable String datatype, @RequestParam(required = false) String modelName,
                    @RequestParam(required = false) String modelTable, @RequestParam String columnVisibility,
                    @AuthenticationPrincipal ProxiedUserDetails currentUser) throws Exception {
        if (StringUtils.isBlank(modelName)) {
            modelName = this.dataDictionaryConfiguration.getModelName();
        }
        
        if (StringUtils.isBlank(modelTable)) {
            modelTable = this.dataDictionaryConfiguration.getModelTableName();
        }
        
        Set<Authorizations> auths = getAuths(currentUser);
        Map<String,String> markings = Maps.newHashMap();
        markings.put("columnVisibility", columnVisibility);
        DESC desc = this.responseObjectFactory.getDescription();
        desc.setDescription("");
        desc.setMarkings(markings);
        
        dataDictionary.deleteDescription(accumuloConnector, this.dataDictionaryConfiguration.getMetadataTableName(), auths, modelName, modelTable, fieldName,
                        datatype, desc);
        // TODO: reload model table cache?
        // cache.reloadCache(modelTable);
        return new VoidResponse();
    }
    
    private Set<Authorizations> getAuths(ProxiedUserDetails currentUser) {
        //@formatter:off
        return currentUser.getProxiedUsers().stream()
                .map(DatawaveUser::getAuths)
                .map(a -> new Authorizations(a.toArray(new String[0])))
                .collect(Collectors.toSet());
        //@formatter:on
    }
    
    private Set<Authorizations> getDowngradedAuthorizations(String requestedAuthorizations, ProxiedUserDetails currentUser) {
        DatawaveUser primaryUser = currentUser.getPrimaryUser();
        return userAuthFunctions.mergeAuthorizations(userAuthFunctions.getRequestedAuthorizations(requestedAuthorizations, currentUser.getPrimaryUser()),
                        currentUser.getProxiedUsers(), u -> u != primaryUser);
    }
}
