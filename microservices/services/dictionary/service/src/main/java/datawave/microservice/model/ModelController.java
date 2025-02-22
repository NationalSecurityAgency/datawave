package datawave.microservice.model;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.Authorizations;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.http.MediaType;
import org.springframework.security.access.annotation.Secured;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.google.common.collect.Sets;

import datawave.microservice.AccumuloConnectionService;
import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.microservice.http.converter.protostuff.ProtostuffHttpMessageConverter;
import datawave.microservice.model.config.ModelProperties;
import datawave.query.model.FieldMapping;
import datawave.query.model.ModelKeyParser;
import datawave.webservice.model.Model;
import datawave.webservice.model.ModelList;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.result.VoidResponse;
import io.swagger.v3.oas.annotations.ExternalDocumentation;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;

/**
 * Service that supports manipulation of models. The models are contained in the data dictionary table.
 */
@Tag(name = "Model Controller /v1", description = "DataWave Model Operations",
                externalDocs = @ExternalDocumentation(description = "Dictionary Service Documentation",
                                url = "https://github.com/NationalSecurityAgency/datawave-dictionary-service"))
@Slf4j
@RestController
@RequestMapping(path = "/model/v1",
                produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE, MediaType.TEXT_XML_VALUE,
                        ProtostuffHttpMessageConverter.PROTOSTUFF_VALUE, MediaType.TEXT_HTML_VALUE, "text/x-yaml", "application/x-yaml"})
@Secured({"AuthorizedUser", "AuthorizedQueryServer", "InternalUser", "Administrator", "JBossAdministrator"})
@EnableConfigurationProperties(ModelProperties.class)
public class ModelController {
    
    private final String dataTablesUri;
    private final String jqueryUri;
    private final AccumuloConnectionService accumloConnectionService;
    
    public static final String DEFAULT_MODEL_TABLE_NAME = "DatawaveMetadata";
    private static final HashSet<String> RESERVED_COLF_VALUES = Sets.newHashSet("e", "i", "ri", "f", "tf", "m", "desc", "edge", "t", "n", "h");
    
    public ModelController(ModelProperties modelProperties, AccumuloConnectionService accumloConnectionService) {
        this.dataTablesUri = modelProperties.getDataTablesUri();
        this.jqueryUri = modelProperties.getJqueryUri();
        this.accumloConnectionService = accumloConnectionService;
    }
    
    /**
     * Get the names of the models
     *
     * @param modelTableName
     *            name of the table that contains the model
     * @return the ModelList
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user
     *            
     * @HTTP 200 success
     * @HTTP 500 internal server error
     */
    @GetMapping("/list") // If we get to change to follow true REST standard, this would just be / and remain a GET
    public ModelList listModelNames(@RequestParam(defaultValue = DEFAULT_MODEL_TABLE_NAME) String modelTableName,
                    @AuthenticationPrincipal DatawaveUserDetails currentUser) {
        
        ModelList response = new ModelList(jqueryUri, dataTablesUri, modelTableName);
        HashSet<String> modelNames = new HashSet<>();
        List<Key> keys;
        try {
            keys = accumloConnectionService.getKeys(modelTableName, currentUser, "");
        } catch (TableNotFoundException e) {
            QueryException qe = new QueryException(DatawaveErrorCode.MODEL_NAME_LIST_ERROR, e);
            log.error(qe.getMessage());
            response.addException(qe.getBottomQueryException());
            return response;
        }
        
        Set<String> colFamilies = keys.stream().map(k -> k.getColumnFamily().toString()).filter(colFamily -> !RESERVED_COLF_VALUES.contains(colFamily))
                        .collect(Collectors.toSet());
        
        for (String colf : colFamilies) {
            String[] parts = colf.split(ModelKeyParser.NULL_BYTE);
            if (parts.length == 1) {
                modelNames.add(colf);
            } else if (parts.length == 2) {
                modelNames.add(parts[0]);
            }
        }
        
        response.setNames(modelNames);
        return response;
    }
    
    /**
     * <strong>Administrator credentials required.</strong> Delete a model with the supplied name
     *
     * @param name
     *            model name to delete
     * @param modelTableName
     *            name of the table that contains the model
     * @return a VoidResponse
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user
     *            
     * @HTTP 200 success
     * @HTTP 404 model not found
     * @HTTP 500 internal server error
     */
    @Operation(summary = "Delete a model with the supplied name.")
    @DeleteMapping("/{name}")
    @Secured({"Administrator", "JBossAdministrator"})
    public VoidResponse deleteModel(@PathVariable String name, @RequestParam(defaultValue = DEFAULT_MODEL_TABLE_NAME) String modelTableName,
                    @AuthenticationPrincipal DatawaveUserDetails currentUser) {
        VoidResponse response = new VoidResponse();
        ModelList models = listModelNames(modelTableName, currentUser);
        if (models.getNames().contains(name)) {
            // the specified model exists, so we can proceed with deleting it
            Model model = getModel(name, modelTableName, currentUser);
            deleteMapping(model, modelTableName, currentUser);
        }
        
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
     * @return a VoidResponse
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user
     *            
     * @HTTP 200 success
     * @HTTP 204 model not found
     * @HTTP 500 internal server error
     */
    @Operation(summary = "Copy a model.")
    
    @PostMapping("/clone")
    @Secured({"Administrator", "JBossAdministrator"})
    public VoidResponse cloneModel(@RequestParam String name, @RequestParam String newName,
                    @RequestParam(defaultValue = DEFAULT_MODEL_TABLE_NAME) String modelTableName, @AuthenticationPrincipal DatawaveUserDetails currentUser) {
        VoidResponse response = new VoidResponse();
        
        Model model = getModel(name, modelTableName, currentUser);
        // Set the new name
        model.setName(newName);
        insertMapping(model, modelTableName, currentUser);
        return response;
    }
    
    /**
     * Retrieve the model and all of its mappings
     *
     * @param name
     *            model name
     * @param modelTableName
     *            name of the table that contains the model
     * @return the Model
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user
     *            
     * @HTTP 200 success
     * @HTTP 404 model not found
     * @HTTP 500 internal server error
     */
    @GetMapping("/{name}")
    public Model getModel(@PathVariable String name, @RequestParam(defaultValue = DEFAULT_MODEL_TABLE_NAME) String modelTableName,
                    @AuthenticationPrincipal DatawaveUserDetails currentUser) {
        Model response = new Model(jqueryUri, dataTablesUri);
        List<Key> keys;
        try {
            keys = accumloConnectionService.getKeys(modelTableName, currentUser, name);
        } catch (TableNotFoundException e) {
            QueryException qe = new QueryException(DatawaveErrorCode.MODEL_NAME_LIST_ERROR, e);
            log.error(qe.getMessage());
            response.addException(qe.getBottomQueryException());
            return response;
        }
        
        TreeSet<FieldMapping> fields = response.getFields();
        keys.forEach(k -> fields.add(ModelKeyParser.parseKey(k)));
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
     * @return a VoidResponse
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user
     *            
     * @HTTP 200 success
     * @HTTP 500 internal server error
     */
    @Operation(summary = "Insert a new field mapping into an existing model.")
    @PostMapping(value = {"/insert", "/import"})
    @Secured({"Administrator", "JBossAdministrator"})
    public VoidResponse insertMapping(@RequestBody Model model, @RequestParam(defaultValue = DEFAULT_MODEL_TABLE_NAME) String modelTableName,
                    @AuthenticationPrincipal DatawaveUserDetails currentUser) {
        if (log.isDebugEnabled()) {
            log.debug("modelTableName: " + (null == modelTableName ? "" : modelTableName));
        }
        
        VoidResponse response = new VoidResponse();
        List<Mutation> mutations = model.getFields().stream().map(mapping -> ModelKeyParser.createMutation(mapping, model.getName()))
                        .collect(Collectors.toList());
        
        QueryException exception = accumloConnectionService.modifyMappings(mutations, modelTableName, model.getName(), currentUser);
        if (exception != null) {
            response.addException(exception.getBottomQueryException());
        }
        
        // Do we already have an AccumuloTableCache bean somewhere?
        // cache.reloadTableCache(tableName);
        return response;
    }
    
    /**
     * <strong>Administrator credentials required.</strong> Delete field mappings from an existing model
     *
     * @param model
     *            list of field mappings to delete
     * @param modelTableName
     *            name of the table that contains the model
     * @return a VoidResponse
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user
     *            
     * @HTTP 200 success
     * @HTTP 500 internal server error
     */
    @Operation(summary = "Delete field mappings from an existing model.")
    @DeleteMapping("/delete")
    @Secured({"Administrator", "JBossAdministrator"})
    public VoidResponse deleteMapping(@RequestBody Model model, @RequestParam(defaultValue = DEFAULT_MODEL_TABLE_NAME) String modelTableName,
                    @AuthenticationPrincipal DatawaveUserDetails currentUser) {
        if (log.isDebugEnabled()) {
            log.debug("Deleting model name: " + model.getName() + "from modelTableName " + modelTableName);
        }
        VoidResponse response = new VoidResponse();
        
        List<Mutation> mutations = model.getFields().stream().map(mapping -> ModelKeyParser.createDeleteMutation(mapping, model.getName()))
                        .collect(Collectors.toList());
        
        QueryException exception = accumloConnectionService.modifyMappings(mutations, modelTableName, model.getName(), currentUser);
        if (exception != null) {
            response.addException(exception.getBottomQueryException());
        }
        
        return response;
    }
}
