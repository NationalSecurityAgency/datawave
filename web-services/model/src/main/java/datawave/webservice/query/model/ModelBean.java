package datawave.webservice.query.model;

import com.google.common.collect.ImmutableSet;
import datawave.annotation.Required;
import datawave.interceptor.RequiredInterceptor;
import datawave.interceptor.ResponseInterceptor;
import datawave.iterators.filter.EntryRegexFilter;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.util.ScannerHelper;
import datawave.webservice.common.cache.AccumuloTableCache;
import datawave.webservice.common.connection.AccumuloConnectionFactory;
import datawave.webservice.common.exception.DatawaveWebApplicationException;
import datawave.webservice.common.exception.NotFoundException;
import datawave.webservice.common.exception.PreConditionFailedException;
import datawave.webservice.model.FieldMapping;
import datawave.webservice.model.Model;
import datawave.webservice.model.ModelList;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.result.BaseResponse;
import datawave.webservice.result.VoidResponse;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.FirstEntryInRowIterator;
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
import javax.ws.rs.DefaultValue;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import java.security.Principal;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
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
    
    protected static final String DEFAULT_MODEL_TABLE_NAME = "DatawaveMetadata";
    
    private static final Logger log = Logger.getLogger(ModelBean.class);
    private static final String VISIBILITY = "visibility";
    private static final String FIELD_NAME = "fieldname";
    private static final String DATA_TYPE = "datatype";
    private static final String MODEL_FIELD_NAME = "modelfieldname";
    private static final String DIRECTION = "direction";
    private static final String ASC = "asc";
    private static final String DESC = "desc";
    
    private static final long BATCH_WRITER_MAX_LATENCY = 1000L;
    private static final long BATCH_WRITER_MAX_MEMORY = 10845760;
    private static final int BATCH_WRITER_MAX_THREADS = 2;
    private static final Set<String> RESERVED_COLF_VALUES = ImmutableSet.of("e", "i", "ri", "f", "tf", "m", "desc", "edge", "t", "n", "h");
    
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
    private AccumuloTableCache tableCache;
    
    @Resource
    private EJBContext context;
    
    /**
     * Get the names of the models
     *
     * @param tableName
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
    public ModelList listModelNames(@QueryParam("modelTableName") String tableName) {
        tableName = getDefaultModelTableNameIfBlank(tableName);
        
        ModelList response = new ModelList(jqueryUri, dataTablesUri, tableName);
        Set<Authorizations> authorizations = getCurrentUserAuthorizations();
        HashSet<String> modelNames = new HashSet<>();
        
        Connector connector = null;
        try {
            connector = getConnector();
            try (Scanner scanner = ScannerHelper.createScanner(connector, tableName, authorizations)) {
                for (Entry<Key,Value> entry : scanner) {
                    String colf = entry.getKey().getColumnFamily().toString();
                    // Do not extract the model name for reserved data types or already found model names.
                    if (!RESERVED_COLF_VALUES.contains(colf) && !modelNames.contains(colf)) {
                        // Extract the model name.
                        String[] parts = colf.split(ModelKeyParser.NULL_BYTE);
                        if (parts.length == 1) {
                            modelNames.add(colf);
                        } else if (parts.length == 2) {
                            modelNames.add(parts[0]);
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error(e);
            throwWebApplicationException(response, DatawaveErrorCode.MODEL_NAME_LIST_ERROR, e);
        } finally {
            returnConnectorToFactory(connector);
        }
        response.setNames(modelNames);
        return response;
    }
    
    /**
     * <strong>Administrator credentials required.</strong> Insert a new model
     *
     * @param model
     * @param tableName
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
    public VoidResponse importModel(Model model, @QueryParam("modelTableName") String tableName) {
        tableName = getDefaultModelTableNameIfBlank(tableName);
        log.debug("Importing model '" + model.getName() + "' to table '" + tableName + "'");
        
        // Verify that a model does not already exist in the table with the same name.
        VoidResponse response = new VoidResponse();
        ModelList models = listModelNames(tableName);
        if (models.getNames().contains(model.getName())) {
            throw new PreConditionFailedException(null, response);
        }
        
        // Insert each mapping for the model.
        insertMapping(model, tableName);
        return response;
    }
    
    /**
     * <strong>Administrator credentials required.</strong> Delete a model with the supplied name
     *
     * @param name
     *            model name to delete
     * @param tableName
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
    public VoidResponse deleteModel(@Required("name") @PathParam("name") String name, @QueryParam("modelTableName") String tableName) {
        tableName = getDefaultModelTableNameIfBlank(tableName);
        log.debug("Deleting model " + name + " from table " + tableName);
        
        // Retrieve all the field mappings for the model and delete them.
        Model model = getModel(name, tableName);
        deleteMappings(model, tableName);
        return new VoidResponse();
    }
    
    /**
     * <strong>Administrator credentials required.</strong> Copy a model
     *
     * @param name
     *            model to copy
     * @param newName
     *            name of copied model
     * @param tableName
     *            name of the table that contains the model
     * @return datawave.webservice.result.VoidResponse
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user
     *
     * @HTTP 200 success
     * @HTTP 404 model not found
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
                    @FormParam("modelTableName") String tableName) {
        tableName = getDefaultModelTableNameIfBlank(tableName);
        
        // Retrieve the model to be cloned.
        Model model = getModel(name, tableName);
        
        // Set a new name and re-import it to create the clone.
        model.setName(newName);
        importModel(model, tableName);
        return new VoidResponse();
    }
    
    /**
     * Retrieve the model and all of its mappings
     *
     * @param name
     *            model name
     * @param tableName
     *            name of the table that contains the model
     * @param limit
     *            the maximum number of field mappings to include in the returned model. Defaults to -1 for no limit.
     * @param offset
     *            the number of initial field mappings to skip and not include in the returned model. Defaults to 0 for no offset.
     * @param sort
     *            the sort order to apply to the field mappings. Defaults to the natural field mapping sort order if not specified. Case-insensitive, supported
     *            fields: 'fieldName', 'modelFieldName', 'dataType', 'direction', 'visibility'. Usage: 'sort=asc:field' to sort by the specified field in
     *            ascending order, 'sort=desc:field' to sort in descending order. Example: 'sort=asc:fieldName'. Omit the field to sort either ascending or
     *            descending with the natural order. Example: 'sort=desc'. Multiple sort orders not supported.
     * @param search
     *            a search term to match (case-insensitive) against the dataType, fieldName, modelFieldName, direction, or visibility of the field mappings.
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
    public Model getModel(@Required("name") @PathParam("name") String name, @QueryParam("modelTableName") String tableName,
                    @QueryParam("limit") @DefaultValue("-1") int limit, @QueryParam("offset") @DefaultValue("0") int offset, @QueryParam("sort") String sort,
                    @QueryParam("search") String search) {
        tableName = getDefaultModelTableNameIfBlank(tableName);
        
        Model response = new Model(jqueryUri, dataTablesUri);
        Set<Authorizations> authorizations = getCurrentUserAuthorizations();
        Connector connector = null;
        boolean modelExists = false;
        try {
            connector = getConnector();
            try (Scanner scanner = ScannerHelper.createScanner(connector, tableName, authorizations)) {
                // Add a filter to only retrieve rows with the model name in the column family.
                IteratorSetting modelNameFilter = new IteratorSetting(21, "modelNameFilter", EntryRegexFilter.class.getName());
                modelNameFilter.addOption(EntryRegexFilter.COLUMN_FAMILY_REGEX, "^" + name + "(\\x00.*)?");
                scanner.addScanIterator(modelNameFilter);
                
                // First check if the model exists. This is true if at least one row for the model exists.
                scanner.addScanIterator(new IteratorSetting(22, "firstEntryOnly", FirstEntryInRowIterator.class.getName()));
                
                if (scanner.iterator().hasNext()) {
                    // Reset the scanner and apply the model name and search term filters.
                    modelExists = true;
                    scanner.clearScanIterators();
                    scanner.addScanIterator(modelNameFilter);
                    
                    // Apply a search filter if needed.
                    applySearchFilter(scanner, search);
                    
                    // Parse the field mappings from each row.
                    TreeSet<FieldMapping> mappings = new TreeSet<>(getSortComparator(sort));
                    for (Entry<Key,Value> entry : scanner) {
                        FieldMapping mapping = ModelKeyParser.parseKey(entry.getKey());
                        mappings.add(mapping);
                    }
                    
                    // Apply the offset and limit to get the desired subset.
                    mappings = applyOffsetAndLimit(mappings, offset, limit);
                    response.setFields(mappings);
                }
            }
        } catch (Exception e) {
            log.error(e);
            throwWebApplicationException(response, DatawaveErrorCode.MODEL_FETCH_ERROR, e);
        } finally {
            returnConnectorToFactory(connector);
        }
        
        // Return 404 if the model is not found.
        if (!modelExists) {
            throw new NotFoundException(null, response);
        }
        
        response.setName(name);
        return response;
    }
    
    // Add a regex filter for the specified search term to the scanner.
    private void applySearchFilter(Scanner scanner, String search) {
        if (StringUtils.isNotBlank(search)) {
            IteratorSetting setting = new IteratorSetting(22, "searchTermFilter", EntryRegexFilter.class.getName());
            EntryRegexFilter.configureOptions(setting).allRegex(search).orMatches().matchSubstrings().caseInsensitive();
            scanner.addScanIterator(setting);
        }
    }
    
    // Return the comparator(s) to be used for sorting the field mappings. The sort may be provided as 'asc:field' or 'desc:field'.
    private Comparator<FieldMapping> getSortComparator(String sort) {
        if (StringUtils.isBlank(sort)) {
            return FieldMapping::compareTo;
        } else {
            // The sort may be in the format 'direction:field' or 'direction'. Ensure that case is ignored.
            String[] parts = StringUtils.deleteWhitespace(sort).toLowerCase().split(":");
            Comparator<FieldMapping> comparator = FieldMapping::compareTo;
            boolean usingNaturalOrder = true;
            
            // If specified, parse the field and establish the comparator to use.
            if (parts.length > 1 && !parts[1].isEmpty()) {
                usingNaturalOrder = false;
                String sortField = parts[1];
                switch (sortField) {
                    case VISIBILITY:
                        comparator = Comparator.comparing(FieldMapping::getColumnVisibility);
                        break;
                    case FIELD_NAME:
                        comparator = Comparator.comparing(FieldMapping::getFieldName);
                        break;
                    case DATA_TYPE:
                        comparator = Comparator.comparing(FieldMapping::getDatatype);
                        break;
                    case MODEL_FIELD_NAME:
                        comparator = Comparator.comparing(FieldMapping::getModelFieldName);
                        break;
                    case DIRECTION:
                        comparator = Comparator.comparing(FieldMapping::getDirection);
                        break;
                    default:
                        throw new IllegalArgumentException("Invalid sort field specified. Valid fields: " + VISIBILITY + " " + FIELD_NAME + " " + DATA_TYPE
                                        + " " + MODEL_FIELD_NAME + " " + DIRECTION);
                }
            }
            
            // Parse the direction.
            String direction = parts[0];
            if (direction.equals(DESC)) {
                comparator = comparator.reversed();
            } else if (!direction.equals(ASC)) {
                throw new IllegalArgumentException("Invalid sort direction specified. Valid sort directions: " + ASC + " " + DESC);
            }
            
            // If the comparator is for a specific field, ensure that any subsequently considered equal fields are sorted naturally.
            if (!usingNaturalOrder) {
                comparator = comparator.thenComparing(FieldMapping::compareTo);
            }
            
            return comparator;
        }
    }
    
    // Return a subset of the given sorted set that contains at most <limit> elements, and does not include the first <offset> elements. A limit of -1 indicates
    // no limit.
    private TreeSet<FieldMapping> applyOffsetAndLimit(SortedSet<FieldMapping> set, int offset, int limit) {
        // If the set size is less than the offset, simply return an empty set and skip all results.
        if (set.size() <= offset) {
            return new TreeSet<>();
        } else {
            TreeSet<FieldMapping> subset = new TreeSet<>(set.comparator());
            Iterator<FieldMapping> iterator = set.iterator();
            long totalSkipped = 0;
            long totalIncluded = 0;
            while (iterator.hasNext()) {
                // Skip the initial desired results.
                if (totalSkipped < offset) {
                    iterator.next();
                    totalSkipped++;
                    continue;
                }
                // If there is a limit and we have reached it, do not include any more results.
                if (limit != -1 && totalIncluded == limit) {
                    break;
                }
                subset.add(iterator.next());
                totalIncluded++;
            }
            return subset;
        }
    }
    
    /**
     * <strong>Administrator credentials required.</strong> Insert a new field mapping into an existing model
     *
     * @param model
     *            list of new field mappings to insert
     * @param tableName
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
    public VoidResponse insertMapping(Model model, @QueryParam("modelTableName") String tableName) {
        VoidResponse response = new VoidResponse();
        
        Connector connector = null;
        BatchWriter writer = null;
        tableName = getDefaultModelTableNameIfBlank(tableName);
        try {
            connector = getConnector();
            writer = createBatchWriter(connector, tableName);
            // Create a corresponding mutation for each field mapping in the model.
            for (FieldMapping mapping : model.getFields()) {
                writer.addMutation(ModelKeyParser.createMutation(mapping, model.getName()));
            }
        } catch (Exception e) {
            log.error("Could not insert mappings for model " + model.getName(), e);
            throwWebApplicationException(response, DatawaveErrorCode.INSERT_MAPPING_ERROR, e);
        } finally {
            closeWriter(writer, response);
            returnConnectorToFactory(connector);
        }
        
        // Reload the cached version of the table since it's been modified.
        tableCache.reloadCache(tableName);
        return response;
    }
    
    /**
     * <strong>Administrator credentials required.</strong> Delete field mappings from an existing model
     *
     * @param model
     *            list of field mappings to delete
     * @param tableName
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
    public VoidResponse deleteMappings(Model model, @QueryParam("modelTableName") String tableName) {
        tableName = getDefaultModelTableNameIfBlank(tableName);
        
        VoidResponse response = new VoidResponse();
        Connector connector = null;
        BatchWriter writer = null;
        try {
            connector = getConnector();
            writer = createBatchWriter(connector, tableName);
            // Create a corresponding deletion mutation for each field mapping in the model.
            for (FieldMapping mapping : model.getFields()) {
                writer.addMutation(ModelKeyParser.createDeleteMutation(mapping, model.getName()));
            }
        } catch (Exception e) {
            log.error("Could not delete mappings for model " + model.getName(), e);
            throwWebApplicationException(response, DatawaveErrorCode.MAPPING_DELETION_ERROR, e);
        } finally {
            closeWriter(writer, response);
            returnConnectorToFactory(connector);
        }
        
        // Reload the cached version of the table since it's been modified.
        tableCache.reloadCache(tableName);
        return response;
    }
    
    // Retrieve a model without limiting or sorting its field mappings.
    private Model getModel(String name, String tableName) {
        return getModel(name, tableName, -1, 0, null, null);
    }
    
    // Get the authorizations for the current user.
    private Set<Authorizations> getCurrentUserAuthorizations() {
        Principal principal = context.getCallerPrincipal();
        String user = principal.getName();
        Set<Authorizations> authorizations = new HashSet<>();
        if (principal instanceof DatawavePrincipal) {
            DatawavePrincipal datawavePrincipal = (DatawavePrincipal) principal;
            user = datawavePrincipal.getShortName();
            datawavePrincipal.getAuthorizations().forEach(auths -> authorizations.add(new Authorizations(auths.toArray(new String[0]))));
        }
        log.trace(user + " has authorizations " + authorizations);
        return authorizations;
    }
    
    // Retrieve a new connector from the factory.
    private Connector getConnector() throws Exception {
        Map<String,String> trackingMap = connectionFactory.getTrackingMap(Thread.currentThread().getStackTrace());
        return connectionFactory.getConnection(AccumuloConnectionFactory.Priority.LOW, trackingMap);
    }
    
    // Return the given connector to the connector pool.
    private void returnConnectorToFactory(Connector connector) {
        try {
            if (connector != null) {
                connectionFactory.returnConnection(connector);
            }
        } catch (Exception e) {
            log.error("Error returning connection to factory", e);
        }
    }
    
    // Attempt to close the given writer, and throw an exception with the response if an error occurs.
    private void closeWriter(BatchWriter writer, BaseResponse response) {
        if (writer != null) {
            try {
                writer.close();
            } catch (MutationsRejectedException e) {
                log.error(e);
                throwWebApplicationException(response, DatawaveErrorCode.WRITER_CLOSE_ERROR, e);
            }
        }
    }
    
    // Return the default model table name if the given value is blank, or the original value otherwise.
    private String getDefaultModelTableNameIfBlank(String tableName) {
        return StringUtils.isBlank(tableName) ? defaultModelTableName : tableName;
    }
    
    // Construct and throw a web application exception.
    private void throwWebApplicationException(BaseResponse response, DatawaveErrorCode errorCode, Throwable cause) {
        QueryException exception = new QueryException(errorCode, cause);
        response.addException(exception);
        throw new DatawaveWebApplicationException(exception, response);
    }
    
    // Create and return for the given connector and table name.
    private BatchWriter createBatchWriter(Connector connector, String tableName) throws TableNotFoundException {
        return connector.createBatchWriter(tableName,
                        new BatchWriterConfig().setMaxLatency(BATCH_WRITER_MAX_LATENCY, TimeUnit.MILLISECONDS).setMaxMemory(BATCH_WRITER_MAX_MEMORY)
                                        .setMaxWriteThreads(BATCH_WRITER_MAX_THREADS));
    }
}
