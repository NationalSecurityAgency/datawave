package datawave.microservice.query.edge;

import datawave.microservice.authorization.user.ProxiedUserDetails;
import datawave.microservice.query.edge.config.EdgeDictionaryProviderProperties;
import datawave.microservice.query.storage.QueryStorageCache;
import datawave.security.authorization.JWTTokenHandler;
import datawave.services.common.edgedictionary.EdgeDictionaryProvider;
import datawave.webservice.dictionary.edge.EdgeDictionaryBase;
import datawave.webservice.dictionary.edge.MetadataBase;
import datawave.webservice.query.Query;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

@Service("edgeDictionaryProvider")
public class DefaultEdgeDictionaryProvider implements EdgeDictionaryProvider {
    
    protected final QueryStorageCache queryStorageCache;
    
    private final WebClient webClient;
    private final JWTTokenHandler jwtTokenHandler;
    
    public DefaultEdgeDictionaryProvider(EdgeDictionaryProviderProperties edgeDictionaryProperties, QueryStorageCache queryStorageCache,
                    WebClient.Builder webClientBuilder, JWTTokenHandler jwtTokenHandler) {
        this.queryStorageCache = queryStorageCache;
        this.webClient = webClientBuilder.baseUrl(edgeDictionaryProperties.getUri()).build();
        this.jwtTokenHandler = jwtTokenHandler;
    }
    
    @Override
    public EdgeDictionaryBase<?,? extends MetadataBase<?>> getEdgeDictionary(Query settings, String metadataTableName) {
        final ProxiedUserDetails currentUser = queryStorageCache.getQueryStatus(settings.getId().toString()).getCurrentUser();
        
        final String bearerHeader = "Bearer " + jwtTokenHandler.createTokenFromUsers(currentUser.getPrimaryUser().getName(), currentUser.getProxiedUsers());
        
        // @formatter:off
        return (EdgeDictionaryBase<?,? extends MetadataBase<?>>) webClient.get()
                .uri(uriBuilder -> uriBuilder
                        .queryParam("metadataTableName", metadataTableName)
                        .queryParam("auths", settings.getQueryAuthorizations())
                        .build())
                .header("Authorization", bearerHeader)
                .retrieve()
                .bodyToMono(EdgeDictionaryBase.class) // probably need to be more specific about the class
                .block();
        // @formatter:on
    }
}
