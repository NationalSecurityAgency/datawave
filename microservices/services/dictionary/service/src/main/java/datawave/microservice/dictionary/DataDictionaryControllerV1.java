package datawave.microservice.dictionary;

import static datawave.microservice.http.converter.protostuff.ProtostuffHttpMessageConverter.PROTOSTUFF_VALUE;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.http.MediaType;
import org.springframework.security.access.annotation.Secured;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.codahale.metrics.annotation.Timed;

import datawave.microservice.AccumuloConnectionService;
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
import io.swagger.v3.oas.annotations.ExternalDocumentation;
import io.swagger.v3.oas.annotations.tags.Tag;

@Tag(name = "Data Dictionary Controller /v1", description = "DataWave Dictionary Operations",
                externalDocs = @ExternalDocumentation(description = "Dictionary Service Documentation",
                                url = "https://github.com/NationalSecurityAgency/datawave-dictionary-service"))
@RestController
@RequestMapping(path = "/data/v1",
                produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE, MediaType.TEXT_XML_VALUE, PROTOSTUFF_VALUE,
                        MediaType.TEXT_HTML_VALUE, "text/x-yaml", "application/x-yaml"})
@EnableConfigurationProperties(DataDictionaryProperties.class)
public class DataDictionaryControllerV1<DESC extends DescriptionBase<DESC>,DICT extends DataDictionaryBase<DICT,META>,META extends MetadataFieldBase<META,DESC>,FIELD extends DictionaryFieldBase<FIELD,DESC>,FIELDS extends FieldsBase<FIELDS,FIELD,DESC>> {
    
    private DataDictionaryControllerLogic dataDictionaryControllerLogic;
    
    public DataDictionaryControllerV1(DataDictionaryProperties dataDictionaryConfiguration, DataDictionary<META,DESC,FIELD> dataDictionary,
                    ResponseObjectFactory<DESC,DICT,META,FIELD,FIELDS> responseObjectFactory, AccumuloConnectionService accumloConnectionService) {
        dataDictionaryControllerLogic = new DataDictionaryControllerLogic<>(dataDictionaryConfiguration, dataDictionary, responseObjectFactory,
                        accumloConnectionService);
    }
    
    @GetMapping("/")
    @Timed(name = "dw.dictionary.data.get", absolute = true)
    public DataDictionaryBase<DICT,META> get(@RequestParam(required = false) String modelName, @RequestParam(required = false) String modelTableName,
                    @RequestParam(required = false) String metadataTableName, @RequestParam(name = "auths", required = false) String queryAuthorizations,
                    @RequestParam(defaultValue = "") String dataTypeFilters, @AuthenticationPrincipal DatawaveUserDetails currentUser) throws Exception {
        return dataDictionaryControllerLogic.get(modelName, modelTableName, metadataTableName, queryAuthorizations, dataTypeFilters, currentUser);
    }
    
    @PostMapping(path = "/Descriptions", consumes = {MediaType.APPLICATION_XML_VALUE, MediaType.TEXT_XML_VALUE, MediaType.APPLICATION_JSON_VALUE})
    @Timed(name = "dw.dictionary.data.uploadDescriptions", absolute = true)
    public VoidResponse uploadDescriptions(@RequestBody FIELDS fields, @RequestParam(required = false) String modelName,
                    @RequestParam(required = false) String modelTable, @AuthenticationPrincipal DatawaveUserDetails currentUser) throws Exception {
        return dataDictionaryControllerLogic.uploadDescriptions(fields, modelName, modelTable, currentUser);
    }
    
    @Secured({"Administrator", "JBossAdministrator"})
    @PutMapping("/Descriptions/{datatype}/{fieldName}/{description}")
    @Timed(name = "dw.dictionary.data.setDescriptionPut", absolute = true)
    public VoidResponse setDescriptionPut(@PathVariable String fieldName, @PathVariable String datatype, @PathVariable String description,
                    @RequestParam(required = false) String modelName, @RequestParam(required = false) String modelTable, @RequestParam String columnVisibility,
                    @AuthenticationPrincipal DatawaveUserDetails currentUser) throws Exception {
        return dataDictionaryControllerLogic.setDescriptionPost(fieldName, datatype, description, modelName, modelTable, columnVisibility, currentUser);
    }
    
    @Secured({"Administrator", "JBossAdministrator"})
    @PostMapping("/Descriptions")
    @Timed(name = "dw.dictionary.data.setDescriptionPost", absolute = true)
    public VoidResponse setDescriptionPost(@RequestParam String fieldName, @RequestParam String datatype, @RequestParam String description,
                    @RequestParam(required = false) String modelName, @RequestParam(required = false) String modelTable, @RequestParam String columnVisibility,
                    @AuthenticationPrincipal DatawaveUserDetails currentUser) throws Exception {
        return dataDictionaryControllerLogic.setDescriptionPost(fieldName, datatype, description, modelName, modelTable, columnVisibility, currentUser);
    }
    
    @GetMapping("/Descriptions")
    @Timed(name = "dw.dictionary.data.allDescriptions", absolute = true)
    public FIELDS allDescriptions(@RequestParam(required = false) String modelName, @RequestParam(required = false) String modelTable,
                    @AuthenticationPrincipal DatawaveUserDetails currentUser) throws Exception {
        return (FIELDS) dataDictionaryControllerLogic.allDescriptions(modelName, modelTable, currentUser);
    }
    
    @GetMapping("/Descriptions/{datatype}")
    @Timed(name = "dw.dictionary.data.datatypeDescriptions", absolute = true)
    public FIELDS datatypeDescriptions(@PathVariable("datatype") String datatype, @RequestParam(required = false) String modelName,
                    @RequestParam(required = false) String modelTable, @AuthenticationPrincipal DatawaveUserDetails currentUser) throws Exception {
        return (FIELDS) dataDictionaryControllerLogic.datatypeDescriptions(datatype, modelName, modelTable, currentUser);
    }
    
    @GetMapping("/Descriptions/{datatype}/{fieldName}")
    @Timed(name = "dw.dictionary.data.fieldNameDescription", absolute = true)
    public FIELDS fieldNameDescription(@PathVariable String fieldName, @PathVariable String datatype, @RequestParam(required = false) String modelName,
                    @RequestParam(required = false) String modelTable, @AuthenticationPrincipal DatawaveUserDetails currentUser) throws Exception {
        return (FIELDS) dataDictionaryControllerLogic.fieldNameDescription(fieldName, datatype, modelName, modelTable, currentUser);
    }
    
    @Secured({"Administrator", "JBossAdministrator"})
    @DeleteMapping("/Descriptions/{datatype}/{fieldName}")
    @Timed(name = "dw.dictionary.data.deleteDescription", absolute = true)
    public VoidResponse deleteDescription(@PathVariable String fieldName, @PathVariable String datatype, @RequestParam(required = false) String modelName,
                    @RequestParam(required = false) String modelTable, @RequestParam String columnVisibility,
                    @AuthenticationPrincipal DatawaveUserDetails currentUser) throws Exception {
        return dataDictionaryControllerLogic.deleteDescription(fieldName, datatype, modelName, modelTable, columnVisibility, currentUser);
    }
}
