package datawave.microservice.query.edge;

import org.springframework.web.reactive.function.client.WebClient;

import datawave.core.common.edgedictionary.EdgeDictionaryProvider;
import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.microservice.query.Query;
import datawave.microservice.query.edge.config.EdgeDictionaryProviderProperties;
import datawave.microservice.query.storage.QueryStorageCache;
import datawave.security.authorization.JWTTokenHandler;
import datawave.webservice.dictionary.edge.EdgeDictionaryBase;
import datawave.webservice.dictionary.edge.MetadataBase;

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
        final DatawaveUserDetails currentUser = queryStorageCache.getQueryStatus(settings.getId().toString()).getCurrentUser();
        
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
