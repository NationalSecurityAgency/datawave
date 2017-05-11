package datawave.webservice.datadictionary;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import datawave.annotation.Required;
import datawave.configuration.spring.SpringBean;
import datawave.interceptor.RequiredInterceptor;
import datawave.interceptor.ResponseInterceptor;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.util.AuthorizationsUtil;
import datawave.webservice.common.cache.AccumuloTableCache;
import datawave.webservice.common.connection.AccumuloConnectionFactory;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.query.result.metadata.MetadataFieldBase;
import datawave.webservice.result.VoidResponse;
import datawave.webservice.results.datadictionary.DataDictionaryBase;
import datawave.webservice.results.datadictionary.DefaultFields;
import datawave.webservice.results.datadictionary.DescriptionBase;
import datawave.webservice.results.datadictionary.DictionaryFieldBase;
import datawave.webservice.results.datadictionary.FieldsBase;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.jboss.resteasy.annotations.GZIP;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.annotation.security.PermitAll;
import javax.annotation.security.RolesAllowed;
import javax.ejb.EJBContext;
import javax.ejb.LocalBean;
import javax.ejb.Stateless;
import javax.inject.Inject;
import javax.interceptor.Interceptors;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.xml.bind.annotation.XmlSeeAlso;

import java.security.Principal;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * 
 */
@Path("/DataDictionary")
@GZIP
@Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
        "application/x-protostuff", "text/html"})
@LocalBean
@Stateless
@PermitAll
@XmlSeeAlso(DefaultFields.class)
public class DataDictionaryBean {
    
    private static final Logger log = Logger.getLogger(DataDictionaryBean.class);
    
    @Resource
    private EJBContext ctx;
    
    @Inject
    private AccumuloConnectionFactory connectionFactory;
    
    @Inject
    private AccumuloTableCache cache;
    
    @Inject
    private DatawaveDataDictionary datawaveDataDictionary;
    
    @Inject
    @SpringBean(refreshable = true)
    private DataDictionaryConfiguration dataDictionaryConfiguration;
    
    @Inject
    private ResponseObjectFactory responseObjectFactory;
    
    @PostConstruct
    public void init() {
        this.datawaveDataDictionary.setNormalizerMapping(this.dataDictionaryConfiguration.getNormalizerMap());
    }
    
    Connector getConnector() throws Exception {
        Map<String,String> trackingMap = connectionFactory.getTrackingMap(Thread.currentThread().getStackTrace());
        return connectionFactory.getConnection(AccumuloConnectionFactory.Priority.NORMAL, trackingMap);
    }
    
    private void returnConnector(Connector connector) throws Exception {
        connectionFactory.returnConnection(connector);
    }
    
    private Set<Authorizations> getAuths() {
        // Find out who/what called this method
        Principal p = ctx.getCallerPrincipal();
        String sid = p.getName();
        Collection<Collection<String>> cbAuths = new HashSet<>();
        if (p instanceof DatawavePrincipal) {
            DatawavePrincipal cp = (DatawavePrincipal) p;
            sid = cp.getShortName();
            cbAuths.addAll(cp.getAuthorizations());
        }
        log.trace(sid + " has authorizations " + cbAuths.toString());
        
        return AuthorizationsUtil.buildAuthorizations(cbAuths);
    }
    
    /**
     * Returns the DataDictionary given a model name and table, metadata table and authorizations
     *
     * @param modelName
     *            Name of query model to apply (Optional)
     * @param modelTableName
     *            Name of query model table (Optional)
     * @param metadataTableName
     *            Name of metadata table (Optional)
     * @param queryAuthorizations
     *            Authorizations to use
     * @param dataTypeFilters
     *            Comma separated list of dataTypeFilters. DataDictionary will contain the union of fields that are found with those data types (Optional,
     *            returns all if no filter provided)
     * @return
     * @throws Exception
     */
    @GET
    @Path("/")
    @Interceptors({ResponseInterceptor.class})
    public DataDictionaryBase get(@QueryParam("modelName") String modelName, @QueryParam("modelTableName") String modelTableName,
                    @QueryParam("metadataTableName") String metadataTableName, @QueryParam("auths") String queryAuthorizations,
                    @QueryParam("dataTypeFilters") @DefaultValue("") String dataTypeFilters) throws Exception {
        if (null == modelName || StringUtils.isBlank(modelName)) {
            modelName = this.dataDictionaryConfiguration.getModelName();
        }
        
        if (null == modelTableName || StringUtils.isBlank(modelTableName)) {
            modelTableName = this.dataDictionaryConfiguration.getModelTableName();
        }
        
        if (null == metadataTableName || StringUtils.isBlank(metadataTableName)) {
            metadataTableName = this.dataDictionaryConfiguration.getMetadataTableName();
        }
        
        Collection<String> dataTypes = (StringUtils.isBlank(dataTypeFilters) ? Collections.<String> emptyList() : Arrays.asList(dataTypeFilters.split(",")));
        
        Connector connector = null;
        try {
            connector = getConnector();
            // If the user provides authorizations, intersect it with their actual authorizations
            Set<Authorizations> auths = AuthorizationsUtil.getDowngradedAuthorizations(queryAuthorizations, ctx.getCallerPrincipal());
            Collection<MetadataFieldBase> fields = this.datawaveDataDictionary.getFields(modelName, modelTableName, metadataTableName, dataTypes, connector,
                            auths, this.dataDictionaryConfiguration.getNumThreads());
            DataDictionaryBase dataDictionary = this.responseObjectFactory.getDataDictionary();
            dataDictionary.setFields(fields);
            return dataDictionary;
        } finally {
            if (null != connector)
                returnConnector(connector);
        }
        
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
     * @return
     * @throws Exception
     */
    @POST
    @Consumes({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml"})
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "application/x-protostuff"})
    @Path("/Descriptions")
    public VoidResponse uploadDescriptions(FieldsBase fields, @QueryParam("modelName") String modelName, @QueryParam("modelTable") String modelTable)
                    throws Exception {
        if (StringUtils.isBlank(modelName)) {
            modelName = this.dataDictionaryConfiguration.getModelName();
        }
        
        if (StringUtils.isBlank(modelTable)) {
            modelTable = this.dataDictionaryConfiguration.getModelTableName();
        }
        
        Connector connector = null;
        try {
            connector = getConnector();
            Set<Authorizations> auths = getAuths();
            List<DictionaryFieldBase> list = fields.getFields();
            for (DictionaryFieldBase desc : list) {
                this.datawaveDataDictionary.setDescription(connector, this.dataDictionaryConfiguration.getMetadataTableName(), auths, modelName, modelTable,
                                desc);
            }
            cache.reloadCache(modelTable);
            return new VoidResponse();
        } finally {
            if (null != connector)
                returnConnector(connector);
        }
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
     * @return
     * @throws Exception
     */
    @RolesAllowed({"Administrator", "JBossAdministrator"})
    @Interceptors({RequiredInterceptor.class, ResponseInterceptor.class})
    @PUT
    @Path("/Descriptions/{datatype}/{fieldName}/{description}")
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "application/x-protostuff"})
    public VoidResponse setDescriptionPut(@Required("fieldName") @PathParam("fieldName") String fieldName,
                    @Required("datatype") @PathParam("datatype") String datatype, @Required("description") @PathParam("description") String description,
                    @QueryParam("modelName") String modelName, @QueryParam("modelTable") String modelTable,
                    @Required("columnVisibility") @QueryParam("columnVisibility") String columnVisibility) throws Exception {
        if (StringUtils.isBlank(modelName)) {
            modelName = this.dataDictionaryConfiguration.getModelName();
        }
        
        if (StringUtils.isBlank(modelTable)) {
            modelTable = this.dataDictionaryConfiguration.getModelTableName();
        }
        
        Connector connector = null;
        try {
            connector = getConnector();
            Set<Authorizations> auths = getAuths();
            Map<String,String> markings = Maps.newHashMap();
            markings.put("columnVisibility", columnVisibility);
            DescriptionBase desc = this.responseObjectFactory.getDescription();
            desc.setMarkings(markings);
            desc.setDescription(description);
            this.datawaveDataDictionary.setDescription(connector, this.dataDictionaryConfiguration.getMetadataTableName(), auths, modelName, modelTable,
                            fieldName, datatype, desc);
            cache.reloadCache(modelTable);
            return new VoidResponse();
        } finally {
            if (null != connector)
                returnConnector(connector);
        }
        
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
     */
    @RolesAllowed({"Administrator", "JBossAdministrator"})
    @Interceptors({RequiredInterceptor.class, ResponseInterceptor.class})
    @POST
    @Path("/Descriptions")
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "application/x-protostuff"})
    public VoidResponse setDescriptionPost(@Required("fieldName") @FormParam("fieldName") String fieldName,
                    @Required("datatype") @FormParam("datatype") String datatype, @Required("description") @FormParam("description") String description,
                    @FormParam("modelName") String modelName, @FormParam("modelTable") String modelTable,
                    @Required("columnVisibility") @FormParam("columnVisibility") String columnVisibility) throws Exception {
        if (StringUtils.isBlank(modelName)) {
            modelName = this.dataDictionaryConfiguration.getModelName();
        }
        
        if (StringUtils.isBlank(modelTable)) {
            modelTable = this.dataDictionaryConfiguration.getModelTableName();
        }
        
        Connector connector = null;
        try {
            connector = getConnector();
            Set<Authorizations> auths = getAuths();
            DescriptionBase desc = this.responseObjectFactory.getDescription();
            Map<String,String> markings = Maps.newHashMap();
            markings.put("columnVisibility", columnVisibility);
            desc.setMarkings(markings);
            desc.setDescription(description);
            this.datawaveDataDictionary.setDescription(connector, this.dataDictionaryConfiguration.getMetadataTableName(), auths, modelName, modelTable,
                            fieldName, datatype, desc);
            cache.reloadCache(modelTable);
            return new VoidResponse();
        } finally {
            if (null != connector)
                returnConnector(connector);
        }
    }
    
    /**
     * Fetch all descriptions stored in the database, optionally applying a model.
     *
     * @param modelName
     *            Optional model name
     * @param modelTable
     *            Optional model table name
     * @return
     * @throws Exception
     */
    @Interceptors({ResponseInterceptor.class})
    @GET
    @Path("/Descriptions")
    public FieldsBase allDescriptions(@QueryParam("modelName") String modelName, @QueryParam("modelTable") String modelTable) throws Exception {
        if (StringUtils.isBlank(modelName)) {
            modelName = this.dataDictionaryConfiguration.getModelName();
        }
        
        if (StringUtils.isBlank(modelTable)) {
            modelTable = this.dataDictionaryConfiguration.getModelTableName();
        }
        
        Connector connector = null;
        try {
            connector = getConnector();
            Set<Authorizations> auths = getAuths();
            Multimap<Entry<String,String>,? extends DescriptionBase> descriptions = this.datawaveDataDictionary.getDescriptions(connector,
                            this.dataDictionaryConfiguration.getMetadataTableName(), auths, modelName, modelTable);
            FieldsBase response = this.responseObjectFactory.getFields();
            response.setDescriptions(descriptions);
            return response;
        } finally {
            if (null != connector)
                returnConnector(connector);
        }
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
     * @return
     * @throws Exception
     */
    @Interceptors({RequiredInterceptor.class, ResponseInterceptor.class})
    @GET
    @Path("/Descriptions/{datatype}")
    public FieldsBase datatypeDescriptions(@Required("datatype") @PathParam("datatype") String datatype, @QueryParam("modelName") String modelName,
                    @QueryParam("modelTable") String modelTable) throws Exception {
        if (StringUtils.isBlank(modelName)) {
            modelName = this.dataDictionaryConfiguration.getModelName();
        }
        
        if (StringUtils.isBlank(modelTable)) {
            modelTable = this.dataDictionaryConfiguration.getModelTableName();
        }
        
        Connector connector = null;
        try {
            connector = getConnector();
            Set<Authorizations> auths = getAuths();
            Multimap<Entry<String,String>,? extends DescriptionBase> descriptions = this.datawaveDataDictionary.getDescriptions(connector,
                            this.dataDictionaryConfiguration.getMetadataTableName(), auths, modelName, modelTable, datatype);
            FieldsBase response = this.responseObjectFactory.getFields();
            response.setDescriptions(descriptions);
            return response;
        } finally {
            if (null != connector)
                returnConnector(connector);
        }
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
     * @return
     * @throws Exception
     */
    @Interceptors({RequiredInterceptor.class, ResponseInterceptor.class})
    @GET
    @Path("/Descriptions/{datatype}/{fieldName}")
    public FieldsBase fieldNameDescription(@Required("fieldName") @PathParam("fieldName") String fieldName,
                    @Required("datatype") @PathParam("datatype") String datatype, @QueryParam("modelName") String modelName,
                    @QueryParam("modelTable") String modelTable) throws Exception {
        if (StringUtils.isBlank(modelName)) {
            modelName = this.dataDictionaryConfiguration.getModelName();
        }
        
        if (StringUtils.isBlank(modelTable)) {
            modelTable = this.dataDictionaryConfiguration.getModelTableName();
        }
        
        Connector connector = null;
        try {
            connector = getConnector();
            Set<Authorizations> auths = getAuths();
            Set<? extends DescriptionBase> descriptions = this.datawaveDataDictionary.getDescriptions(connector,
                            this.dataDictionaryConfiguration.getMetadataTableName(), auths, modelName, modelTable, fieldName, datatype);
            FieldsBase response;
            if (descriptions.isEmpty()) {
                response = this.responseObjectFactory.getFields();
            } else {
                Multimap<Entry<String,String>,DescriptionBase> mmap = HashMultimap.create();
                for (DescriptionBase desc : descriptions) {
                    mmap.put(Maps.immutableEntry(fieldName, datatype), desc);
                }
                response = this.responseObjectFactory.getFields();
                response.setDescriptions(mmap);
            }
            return response;
        } finally {
            if (null != connector)
                returnConnector(connector);
        }
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
     * @return
     * @throws Exception
     */
    @RolesAllowed({"Administrator", "JBossAdministrator"})
    @Interceptors({RequiredInterceptor.class, ResponseInterceptor.class})
    @DELETE
    @Path("/Descriptions/{datatype}/{fieldName}")
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "application/x-protostuff"})
    public VoidResponse deleteDescription(@Required("fieldName") @PathParam("fieldName") String fieldName,
                    @Required("datatype") @PathParam("datatype") String datatype, @QueryParam("modelName") String modelName,
                    @QueryParam("modelTable") String modelTable, @Required("columnVisibility") @QueryParam("columnVisibility") String columnVisibility)
                    throws Exception {
        if (StringUtils.isBlank(modelName)) {
            modelName = this.dataDictionaryConfiguration.getModelName();
        }
        
        if (StringUtils.isBlank(modelTable)) {
            modelTable = this.dataDictionaryConfiguration.getModelTableName();
        }
        
        Connector connector = null;
        try {
            connector = getConnector();
            Set<Authorizations> auths = getAuths();
            Map<String,String> markings = Maps.newHashMap();
            markings.put("columnVisibility", columnVisibility);
            DescriptionBase desc = this.responseObjectFactory.getDescription();
            desc.setDescription("");
            desc.setMarkings(markings);
            
            this.datawaveDataDictionary.deleteDescription(connector, this.dataDictionaryConfiguration.getMetadataTableName(), auths, modelName, modelTable,
                            fieldName, datatype, desc);
            cache.reloadCache(modelTable);
            return new VoidResponse();
        } finally {
            if (null != connector)
                returnConnector(connector);
        }
        
    }
    
}
