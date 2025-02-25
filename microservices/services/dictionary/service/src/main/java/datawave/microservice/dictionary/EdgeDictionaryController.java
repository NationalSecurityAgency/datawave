package datawave.microservice.dictionary;

import static datawave.microservice.http.converter.protostuff.ProtostuffHttpMessageConverter.PROTOSTUFF_VALUE;

import org.apache.commons.lang.StringUtils;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.http.MediaType;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.codahale.metrics.annotation.Timed;

import datawave.accumulo.util.security.UserAuthFunctions;
import datawave.microservice.AccumuloConnectionService;
import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.microservice.dictionary.config.EdgeDictionaryProperties;
import datawave.microservice.dictionary.edge.EdgeDictionary;
import datawave.webservice.dictionary.edge.EdgeDictionaryBase;
import datawave.webservice.dictionary.edge.MetadataBase;
import io.swagger.v3.oas.annotations.ExternalDocumentation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;

@Tag(name = "Edge Dictionary Controller /v1", description = "DataWave Edge Dictionary Operations",
                externalDocs = @ExternalDocumentation(description = "Dictionary Service Documentation",
                                url = "https://github.com/NationalSecurityAgency/datawave-dictionary-service"))
@Slf4j
@RestController
@RequestMapping(path = "/edge/v1",
                produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE, MediaType.TEXT_XML_VALUE, PROTOSTUFF_VALUE,
                        MediaType.TEXT_HTML_VALUE, "text/x-yaml", "application/x-yaml"})
@EnableConfigurationProperties(EdgeDictionaryProperties.class)
public class EdgeDictionaryController<EDGE extends EdgeDictionaryBase<EDGE,META>,META extends MetadataBase<META>> {
    
    private final EdgeDictionaryProperties edgeDictionaryProperties;
    private final EdgeDictionary<EDGE,META> edgeDictionary;
    private final UserAuthFunctions userAuthFunctions;
    private final AccumuloConnectionService accumuloConnectionService;
    
    public EdgeDictionaryController(EdgeDictionaryProperties edgeDictionaryProperties, EdgeDictionary<EDGE,META> edgeDictionary,
                    UserAuthFunctions userAuthFunctions, AccumuloConnectionService accumloConnectionService) {
        this.edgeDictionaryProperties = edgeDictionaryProperties;
        this.edgeDictionary = edgeDictionary;
        this.userAuthFunctions = userAuthFunctions;
        this.accumuloConnectionService = accumloConnectionService;
    }
    
    /**
     * Returns the EdgeDictionary given a metadata table and authorizations
     *
     * @param metadataTableName
     *            Name of metadata table (Optional)
     * @param queryAuthorizations
     *            Authorizations to use
     * @return the EdgeDictionaryBase class (extended) that contains the edge dictionary fields
     * @throws Exception
     *             if there is any problem retrieving the edge dictionary from Accumulo
     */
    @GetMapping("/")
    @Timed(name = "dw.dictionary.edge.get", absolute = true)
    public EdgeDictionaryBase<EDGE,META> get(@RequestParam(required = false) String metadataTableName,
                    @RequestParam(name = "auths", required = false) String queryAuthorizations, @AuthenticationPrincipal DatawaveUserDetails currentUser)
                    throws Exception {
        log.info("EDGEDICTIONARY: entered rest endpoint");
        if (null == metadataTableName || StringUtils.isBlank(metadataTableName)) {
            metadataTableName = edgeDictionaryProperties.getMetadataTableName();
        }
        
        EDGE edgeDict = edgeDictionary.getEdgeDictionary(metadataTableName, accumuloConnectionService.getConnection().getAccumuloClient(),
                        accumuloConnectionService.getDowngradedAuthorizations(queryAuthorizations, currentUser), edgeDictionaryProperties.getNumThreads());
        
        log.info("EDGEDICTIONARY: returning edge dictionary");
        return edgeDict;
    }
    
}
