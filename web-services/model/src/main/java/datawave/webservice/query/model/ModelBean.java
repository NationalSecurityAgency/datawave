package datawave.webservice.query.model;

import com.google.common.collect.Sets;
import datawave.annotation.Required;
import datawave.interceptor.RequiredInterceptor;
import datawave.interceptor.ResponseInterceptor;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.util.ScannerHelper;
import datawave.webservice.common.cache.AccumuloTableCache;
import datawave.webservice.common.connection.AccumuloConnectionFactory;
import datawave.webservice.common.exception.DatawaveWebApplicationException;
import datawave.webservice.common.exception.NotFoundException;
import datawave.webservice.common.exception.PreConditionFailedException;
import datawave.webservice.model.FieldMapping;
import datawave.webservice.model.ModelList;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.result.VoidResponse;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang.StringUtils;
import org.apache.deltaspike.core.api.config.ConfigProperty;
import org.apache.log4j.Logger;
import org.jboss.resteasy.annotations.GZIP;

import javax.annotation.Resource;
import javax.annotation.security.DeclareRoles;
import javax.annotation.security.RolesAllowed;
import javax.ejb.EJB;
import javax.ejb.EJBContext;
import javax.ejb.LocalBean;
import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.ejb.TransactionManagement;
import javax.ejb.TransactionManagementType;
import javax.inject.Inject;
import javax.interceptor.Interceptors;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import java.security.Principal;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Service that supports manipulation of models. The models are contained in the data dictionary table.
 */
@Path("/Model")
@RolesAllowed({"AuthorizedUser", "AuthorizedQueryServer", "InternalUser", "Administrator", "JBossAdministrator"})
@DeclareRoles({"AuthorizedUser", "AuthorizedQueryServer", "InternalUser", "Administrator", "JBossAdministrator"})
@Stateless
@LocalBean
@TransactionAttribute(TransactionAttributeType.NOT_SUPPORTED)
@TransactionManagement(TransactionManagementType.BEAN)
public class ModelBean {
    
    private Logger log = Logger.getLogger(this.getClass());
    
    public static final String DEFAULT_MODEL_TABLE_NAME = "DatawaveMetadata";
    
    private static final long BATCH_WRITER_MAX_LATENCY = 1000L;
    private static final long BATCH_WRITER_MAX_MEMORY = 10845760;
    private static final int BATCH_WRITER_MAX_THREADS = 2;
    
    private static final HashSet<String> RESERVED_COLF_VALUES = Sets.newHashSet("e", "i", "ri", "f", "tf", "m", "desc", "edge", "t", "n", "h");
    
    @Inject
    @ConfigProperty(name = "dw.model.defaultTableName", defaultValue = DEFAULT_MODEL_TABLE_NAME)
    private String defaultModelTableName;
    
    @Inject
    @ConfigProperty(name = "dw.cdn.jquery.uri", defaultValue = "/jquery.min.js")
    private String jqueryUri;
    
    @Inject
    @ConfigProperty(name = "dw.cdn.dataTables.uri", defaultValue = "/jquery.dataTables.min.js")
    private String dataTablesUri;
    
    @EJB
    private AccumuloConnectionFactory connectionFactory;
    
    @EJB
    private AccumuloTableCache cache;
    
    @Resource
    private EJBContext ctx;
    
    /**
     * Get the names of the models
     *
     * @param modelTableName
     *            name of the table that contains the model
     * @return datawave.webservice.model.ModelList
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user
     *
     * @HTTP 200 success
     * @HTTP 500 internal server error
     */
    @GET
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "application/x-protostuff", "text/html"})
    @Path("/list")
    @GZIP
    @Interceptors(ResponseInterceptor.class)
    public ModelList listModelNames(@QueryParam("modelTableName") String modelTableName) {
        
        if (modelTableName == null) {
            modelTableName = defaultModelTableName;
        }
        
        ModelList response = new ModelList(jqueryUri, dataTablesUri, modelTableName);
        
        // Find out who/what called this method
        Principal p = ctx.getCallerPrincipal();
        String user = p.getName();
        Set<Authorizations> cbAuths = new HashSet<>();
        if (p instanceof DatawavePrincipal) {
            DatawavePrincipal cp = (DatawavePrincipal) p;
            user = cp.getShortName();
            for (Collection<String> auths : cp.getAuthorizations()) {
                cbAuths.add(new Authorizations(auths.toArray(new String[auths.size()])));
            }
        }
        log.trace(user + " has authorizations " + cbAuths);
        
        AccumuloClient client = null;
        HashSet<String> modelNames = new HashSet<>();
        try {
            Map<String,String> trackingMap = connectionFactory.getTrackingMap(Thread.currentThread().getStackTrace());
            client = connectionFactory.getClient(AccumuloConnectionFactory.Priority.LOW, trackingMap);
            try (Scanner scanner = ScannerHelper.createScanner(client, this.checkModelTableName(modelTableName), cbAuths)) {
                for (Entry<Key,Value> entry : scanner) {
                    String colf = entry.getKey().getColumnFamily().toString();
                    if (!RESERVED_COLF_VALUES.contains(colf) && !modelNames.contains(colf)) {
                        String[] parts = colf.split(ModelKeyParser.NULL_BYTE);
                        if (parts.length == 1)
                            modelNames.add(colf);
                        else if (parts.length == 2)
                            modelNames.add(parts[0]);
                    }
                }
            }
            
        } catch (Exception e) {
            QueryException qe = new QueryException(DatawaveErrorCode.MODEL_NAME_LIST_ERROR, e);
            log.error(qe);
            response.addException(qe.getBottomQueryException());
            throw new DatawaveWebApplicationException(qe, response);
        } finally {
            if (null != client) {
                try {
                    connectionFactory.returnClient(client);
                } catch (Exception e) {
                    log.error("Error returning connection to factory", e);
                }
            }
        }
        response.setNames(modelNames);
        return response;
    }
    
    /**
     * <strong>Administrator credentials required.</strong> Insert a new model
     *
     * @param model
     *            the model
     * @param modelTableName
     *            name of the table that contains the model
     * @return datawave.webservice.result.VoidResponse
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user
     *
     * @HTTP 200 success
     * @HTTP 412 if model already exists with this name, delete it first
     * @HTTP 500 internal server error
     */
    @POST
    @Consumes({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml"})
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "application/x-protostuff"})
    @Path("/import")
    @GZIP
    @RolesAllowed({"Administrator", "JBossAdministrator"})
    @Interceptors(ResponseInterceptor.class)
    public VoidResponse importModel(datawave.webservice.model.Model model, @QueryParam("modelTableName") String modelTableName) {
        
        if (modelTableName == null) {
            modelTableName = defaultModelTableName;
        }
        
        if (log.isDebugEnabled()) {
            log.debug("modelTableName: " + (null == modelTableName ? "" : modelTableName));
        }
        VoidResponse response = new VoidResponse();
        
        ModelList models = listModelNames(modelTableName);
        if (models.getNames().contains(model.getName()))
            throw new PreConditionFailedException(null, response);
        
        insertMapping(model, modelTableName);
        
        return response;
    }
    
    /**
     * <strong>Administrator credentials required.</strong> Delete a model with the supplied name
     *
     * @param name
     *            model name to delete
     * @param modelTableName
     *            name of the table that contains the model
     * @return datawave.webservice.result.VoidResponse
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user
     *
     * @HTTP 200 success
     * @HTTP 404 model not found
     * @HTTP 500 internal server error
     */
    @DELETE
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "application/x-protostuff"})
    @Path("/{name}")
    @GZIP
    @RolesAllowed({"Administrator", "JBossAdministrator"})
    @Interceptors({RequiredInterceptor.class, ResponseInterceptor.class})
    public VoidResponse deleteModel(@Required("name") @PathParam("name") String name, @QueryParam("modelTableName") String modelTableName) {
        
        if (modelTableName == null) {
            modelTableName = defaultModelTableName;
        }
        
        return deleteModel(name, modelTableName, true);
    }
    
    private VoidResponse deleteModel(@Required("name") String name, String modelTableName, boolean reloadCache) {
        if (log.isDebugEnabled()) {
            log.debug("model name: " + name);
            log.debug("modelTableName: " + (null == modelTableName ? "" : modelTableName));
        }
        VoidResponse response = new VoidResponse();
        
        ModelList models = listModelNames(modelTableName);
        if (!models.getNames().contains(name))
            throw new NotFoundException(null, response);
        
        datawave.webservice.model.Model model = getModel(name, modelTableName);
        deleteMapping(model, modelTableName, reloadCache);
        
        return response;
    }
    
    /**
     * <strong>Administrator credentials required.</strong> Copy a model
     *
     * @param name
     *            model to copy
     * @param newName
     *            name of copied model
     * @param modelTableName
     *            name of the table that contains the model
     * @return datawave.webservice.result.VoidResponse
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user
     *
     * @HTTP 200 success
     * @HTTP 204 model not found
     * @HTTP 500 internal server error
     */
    @POST
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "application/x-protostuff"})
    @Path("/clone")
    @GZIP
    @RolesAllowed({"Administrator", "JBossAdministrator"})
    @Interceptors({RequiredInterceptor.class, ResponseInterceptor.class})
    public VoidResponse cloneModel(@Required("name") @FormParam("name") String name, @Required("newName") @FormParam("newName") String newName,
                    @FormParam("modelTableName") String modelTableName) {
        VoidResponse response = new VoidResponse();
        
        if (modelTableName == null) {
            modelTableName = defaultModelTableName;
        }
        
        datawave.webservice.model.Model model = getModel(name, modelTableName);
        // Set the new name
        model.setName(newName);
        importModel(model, modelTableName);
        return response;
    }
    
    /**
     * Retrieve the model and all of its mappings
     *
     * @param name
     *            model name
     * @param modelTableName
     *            name of the table that contains the model
     * @return datawave.webservice.model.Model
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user
     *
     * @HTTP 200 success
     * @HTTP 404 model not found
     * @HTTP 500 internal server error
     */
    @GET
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "application/x-protostuff", "text/html"})
    @Path("/{name}")
    @GZIP
    @Interceptors({RequiredInterceptor.class, ResponseInterceptor.class})
    public datawave.webservice.model.Model getModel(@Required("name") @PathParam("name") String name, @QueryParam("modelTableName") String modelTableName) {
        
        if (modelTableName == null) {
            modelTableName = defaultModelTableName;
        }
        
        datawave.webservice.model.Model response = new datawave.webservice.model.Model(jqueryUri, dataTablesUri);
        
        // Find out who/what called this method
        Principal p = ctx.getCallerPrincipal();
        String user = p.getName();
        Set<Authorizations> cbAuths = new HashSet<>();
        if (p instanceof DatawavePrincipal) {
            DatawavePrincipal cp = (DatawavePrincipal) p;
            user = cp.getShortName();
            for (Collection<String> auths : cp.getAuthorizations()) {
                cbAuths.add(new Authorizations(auths.toArray(new String[auths.size()])));
            }
        }
        log.trace(user + " has authorizations " + cbAuths);
        
        AccumuloClient client = null;
        try {
            Map<String,String> trackingMap = connectionFactory.getTrackingMap(Thread.currentThread().getStackTrace());
            client = connectionFactory.getClient(AccumuloConnectionFactory.Priority.LOW, trackingMap);
            try (Scanner scanner = ScannerHelper.createScanner(client, this.checkModelTableName(modelTableName), cbAuths)) {
                IteratorSetting cfg = new IteratorSetting(21, "colfRegex", RegExFilter.class.getName());
                cfg.addOption(RegExFilter.COLF_REGEX, "^" + name + "(\\x00.*)?");
                scanner.addScanIterator(cfg);
                for (Entry<Key,Value> entry : scanner) {
                    FieldMapping mapping = ModelKeyParser.parseKey(entry.getKey(), cbAuths);
                    response.getFields().add(mapping);
                }
            }
        } catch (Exception e) {
            QueryException qe = new QueryException(DatawaveErrorCode.MODEL_FETCH_ERROR, e);
            log.error(qe);
            response.addException(qe.getBottomQueryException());
            throw new DatawaveWebApplicationException(qe, response);
        } finally {
            if (null != client) {
                try {
                    connectionFactory.returnClient(client);
                } catch (Exception e) {
                    log.error("Error returning connection to factory", e);
                }
            }
        }
        
        // return 404 if model not found
        if (response.getFields().isEmpty()) {
            throw new NotFoundException(null, response);
        }
        
        response.setName(name);
        return response;
    }
    
    /**
     * <strong>Administrator credentials required.</strong> Insert a new field mapping into an existing model
     *
     * @param model
     *            list of new field mappings to insert
     * @param modelTableName
     *            name of the table that contains the model
     * @return datawave.webservice.result.VoidResponse
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user
     *
     * @HTTP 200 success
     * @HTTP 500 internal server error
     */
    @POST
    @Consumes({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml"})
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "application/x-protostuff"})
    @Path("/insert")
    @GZIP
    @RolesAllowed({"Administrator", "JBossAdministrator"})
    @Interceptors(ResponseInterceptor.class)
    public VoidResponse insertMapping(datawave.webservice.model.Model model, @QueryParam("modelTableName") String modelTableName) {
        
        if (modelTableName == null) {
            modelTableName = defaultModelTableName;
        }
        
        VoidResponse response = new VoidResponse();
        
        AccumuloClient client = null;
        BatchWriter writer = null;
        String tableName = this.checkModelTableName(modelTableName);
        try {
            Map<String,String> trackingMap = connectionFactory.getTrackingMap(Thread.currentThread().getStackTrace());
            client = connectionFactory.getClient(AccumuloConnectionFactory.Priority.LOW, trackingMap);
            writer = client.createBatchWriter(tableName,
                            new BatchWriterConfig().setMaxLatency(BATCH_WRITER_MAX_LATENCY, TimeUnit.MILLISECONDS).setMaxMemory(BATCH_WRITER_MAX_MEMORY)
                                            .setMaxWriteThreads(BATCH_WRITER_MAX_THREADS));
            for (FieldMapping mapping : model.getFields()) {
                Mutation m = ModelKeyParser.createMutation(mapping, model.getName());
                writer.addMutation(m);
            }
        } catch (Exception e) {
            log.error("Could not insert mapping.", e);
            QueryException qe = new QueryException(DatawaveErrorCode.INSERT_MAPPING_ERROR, e);
            response.addException(qe.getBottomQueryException());
            throw new DatawaveWebApplicationException(qe, response);
        } finally {
            if (null != writer) {
                try {
                    writer.close();
                } catch (MutationsRejectedException e1) {
                    QueryException qe = new QueryException(DatawaveErrorCode.WRITER_CLOSE_ERROR, e1);
                    log.error(qe);
                    response.addException(qe);
                    throw new DatawaveWebApplicationException(qe, response);
                }
            }
            if (null != client) {
                try {
                    connectionFactory.returnClient(client);
                } catch (Exception e) {
                    log.error("Error returning connection to factory", e);
                }
            }
        }
        cache.reloadCache(tableName);
        return response;
    }
    
    /**
     * <strong>Administrator credentials required.</strong> Delete field mappings from an existing model
     *
     * @param model
     *            list of field mappings to delete
     * @param modelTableName
     *            name of the table that contains the model
     * @return datawave.webservice.result.VoidResponse
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user
     *
     * @HTTP 200 success
     * @HTTP 500 internal server error
     */
    @DELETE
    @Consumes({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml"})
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "application/x-protostuff"})
    @Path("/delete")
    @GZIP
    @RolesAllowed({"Administrator", "JBossAdministrator"})
    @Interceptors(ResponseInterceptor.class)
    public VoidResponse deleteMapping(datawave.webservice.model.Model model, @QueryParam("modelTableName") String modelTableName) {
        
        if (modelTableName == null) {
            modelTableName = defaultModelTableName;
        }
        
        return deleteMapping(model, modelTableName, true);
    }
    
    private VoidResponse deleteMapping(datawave.webservice.model.Model model, String modelTableName, boolean reloadCache) {
        VoidResponse response = new VoidResponse();
        
        AccumuloClient client = null;
        BatchWriter writer = null;
        String tableName = this.checkModelTableName(modelTableName);
        try {
            Map<String,String> trackingMap = connectionFactory.getTrackingMap(Thread.currentThread().getStackTrace());
            client = connectionFactory.getClient(AccumuloConnectionFactory.Priority.LOW, trackingMap);
            writer = client.createBatchWriter(tableName,
                            new BatchWriterConfig().setMaxLatency(BATCH_WRITER_MAX_LATENCY, TimeUnit.MILLISECONDS).setMaxMemory(BATCH_WRITER_MAX_MEMORY)
                                            .setMaxWriteThreads(BATCH_WRITER_MAX_THREADS));
            for (FieldMapping mapping : model.getFields()) {
                Mutation m = ModelKeyParser.createDeleteMutation(mapping, model.getName());
                writer.addMutation(m);
            }
        } catch (Exception e) {
            log.error("Could not delete mapping.", e);
            QueryException qe = new QueryException(DatawaveErrorCode.MAPPING_DELETION_ERROR, e);
            response.addException(qe.getBottomQueryException());
            throw new DatawaveWebApplicationException(qe, response);
        } finally {
            if (null != writer) {
                try {
                    writer.close();
                } catch (MutationsRejectedException e1) {
                    QueryException qe = new QueryException(DatawaveErrorCode.WRITER_CLOSE_ERROR, e1);
                    log.error(qe);
                    response.addException(qe);
                    throw new DatawaveWebApplicationException(qe, response);
                }
            }
            if (null != client) {
                try {
                    connectionFactory.returnClient(client);
                } catch (Exception e) {
                    log.error("Error returning connection to factory", e);
                }
            }
        }
        if (reloadCache)
            cache.reloadCache(tableName);
        return response;
    }
    
    /**
     * 
     * @param tableName
     *            the table name
     * @return default table name if param is null or empty, else return the input.
     */
    private String checkModelTableName(String tableName) {
        if (StringUtils.isEmpty(tableName))
            return DEFAULT_MODEL_TABLE_NAME;
        else
            return tableName;
    }
}
