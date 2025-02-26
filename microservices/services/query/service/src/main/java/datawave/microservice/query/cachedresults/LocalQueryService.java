package datawave.microservice.query.cachedresults;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;

import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.microservice.query.QueryManagementService;
import datawave.security.authorization.ProxiedUserDetails;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.GenericResponse;
import datawave.webservice.result.VoidResponse;

@Primary
@Service
@ConditionalOnProperty(name = "datawave.query.cached-results.enabled", havingValue = "true", matchIfMissing = true)
public class LocalQueryService implements QueryService {
    
    private static final Logger log = LoggerFactory.getLogger(LocalQueryService.class);
    
    final private QueryManagementService queryManagementService;
    
    public LocalQueryService(QueryManagementService queryManagementService) {
        this.queryManagementService = queryManagementService;
    }
    
    @Override
    public GenericResponse<String> duplicate(String queryId, ProxiedUserDetails currentUser) throws QueryException {
        log.info("LocalQueryService duplicate {} for {}", queryId, currentUser.getPrimaryUser());
        
        return queryManagementService.duplicate(queryId, new LinkedMultiValueMap<>(), (DatawaveUserDetails) currentUser);
    }
    
    @Override
    public BaseQueryResponse next(String queryId, ProxiedUserDetails currentUser) throws QueryException {
        log.info("LocalQueryService next {} for {}", queryId, currentUser.getPrimaryUser());
        
        return queryManagementService.next(queryId, (DatawaveUserDetails) currentUser);
    }
    
    @Override
    public VoidResponse close(String queryId, ProxiedUserDetails currentUser) throws QueryException {
        log.info("LocalQueryService close {} for {}", queryId, currentUser.getPrimaryUser());
        
        return queryManagementService.close(queryId, (DatawaveUserDetails) currentUser);
    }
    
    @Override
    public VoidResponse cancel(String queryId, ProxiedUserDetails currentUser) throws QueryException {
        log.info("LocalQueryService cancel {} for {}", queryId, currentUser.getPrimaryUser());
        
        return queryManagementService.cancel(queryId, (DatawaveUserDetails) currentUser);
    }
    
    @Override
    public VoidResponse remove(String queryId, ProxiedUserDetails currentUser) throws QueryException {
        log.info("LocalQueryService remove {} for {}", queryId, currentUser.getPrimaryUser());
        
        return queryManagementService.remove(queryId, (DatawaveUserDetails) currentUser);
    }
}
