package datawave.microservice.dictionary;

import com.codahale.metrics.annotation.Timed;
import datawave.accumulo.util.security.UserAuthFunctions;
import datawave.microservice.authorization.user.ProxiedUserDetails;
import datawave.microservice.dictionary.config.EdgeDictionaryProperties;
import datawave.microservice.dictionary.edge.DatawaveEdgeDictionary;
import datawave.security.authorization.DatawaveUser;
import datawave.webservice.results.edgedictionary.EdgeDictionaryBase;
import datawave.webservice.results.edgedictionary.MetadataBase;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.http.MediaType;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.security.PermitAll;
import java.util.Set;

import static datawave.microservice.http.converter.protostuff.ProtostuffHttpMessageConverter.PROTOSTUFF_VALUE;

@PermitAll
@RestController
@RequestMapping(path = "/edge/v1", produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE, MediaType.TEXT_XML_VALUE, PROTOSTUFF_VALUE,
        MediaType.TEXT_HTML_VALUE, "text/x-yaml", "application/x-yaml"})
@EnableConfigurationProperties(EdgeDictionaryProperties.class)
public class EdgeDictionaryOperations<EDGE extends EdgeDictionaryBase<EDGE,META>,META extends MetadataBase<META>> {
    private Logger log = LoggerFactory.getLogger(getClass());
    
    private final EdgeDictionaryProperties edgeDictionaryProperties;
    private final DatawaveEdgeDictionary<EDGE,META> edgeDictionary;
    private final UserAuthFunctions userAuthFunctions;
    private final Connector accumuloConnector;
    
    public EdgeDictionaryOperations(EdgeDictionaryProperties edgeDictionaryProperties, DatawaveEdgeDictionary<EDGE,META> edgeDictionary,
                    UserAuthFunctions userAuthFunctions, @Qualifier("warehouse") Connector accumuloConnector) {
        this.edgeDictionaryProperties = edgeDictionaryProperties;
        this.edgeDictionary = edgeDictionary;
        this.userAuthFunctions = userAuthFunctions;
        this.accumuloConnector = accumuloConnector;
    }
    
    /**
     * Returns the EdgeDictionary given a metadata table and authorizations
     *
     * @param metadataTableName
     *            Name of metadata table (Optional)
     * @param queryAuthorizations
     *            Authorizations to use
     * @return the {@link EdgeDictionaryBase} class (extended) that contains the edge dictionary fields
     * @throws Exception
     *             if there is any problem retrieving the edge dictionary from Accumulo
     */
    @ResponseBody
    @RequestMapping(path = "/")
    @Timed(name = "dw.dictionary.edge.get", absolute = true)
    public EdgeDictionaryBase<EDGE,META> get(@RequestParam(required = false) String metadataTableName,
                    @RequestParam(name = "auths", required = false) String queryAuthorizations, @AuthenticationPrincipal ProxiedUserDetails currentUser)
                    throws Exception {
        log.info("EDGEDICTIONARY: entered rest endpoint");
        if (null == metadataTableName || StringUtils.isBlank(metadataTableName)) {
            metadataTableName = edgeDictionaryProperties.getMetadataTableName();
        }
        
        // If the user provides authorizations, intersect it with their actual authorizations
        Set<Authorizations> auths = getDowngradedAuthorizations(queryAuthorizations, currentUser);
        
        EDGE edgeDict = edgeDictionary.getEdgeDictionary(metadataTableName, accumuloConnector, auths, edgeDictionaryProperties.getNumThreads());
        
        log.info("EDGEDICTIONARY: returning edge dictionary");
        return edgeDict;
    }
    
    private Set<Authorizations> getDowngradedAuthorizations(String requestedAuthorizations, ProxiedUserDetails currentUser) {
        DatawaveUser primaryUser = currentUser.getPrimaryUser();
        return userAuthFunctions.mergeAuthorizations(userAuthFunctions.getRequestedAuthorizations(requestedAuthorizations, currentUser.getPrimaryUser()),
                        currentUser.getProxiedUsers(), u -> u != primaryUser);
    }
}
