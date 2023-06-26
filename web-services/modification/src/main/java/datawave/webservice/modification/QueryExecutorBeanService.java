package datawave.webservice.modification;

import datawave.modification.query.ModificationQueryService;
import datawave.security.authorization.DatawaveUser;
import datawave.webservice.common.audit.AuditParameterBuilder;
import datawave.webservice.query.runner.QueryExecutorBean;
import datawave.webservice.query.util.MapUtils;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.GenericResponse;
import org.springframework.util.MultiValueMap;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public class QueryExecutorBeanService implements ModificationQueryService {
    private final QueryExecutorBean queryService;

    public QueryExecutorBeanService(QueryExecutorBean queryService) {
        this.queryService = queryService;
    }

    @Override
    public GenericResponse<String> createQuery(String logicName, Map<String,List<String>> paramsToMap) {
        return queryService.createQuery(logicName, MapUtils.toMultivaluedMap(paramsToMap));
    }

    @Override
    public BaseQueryResponse next(String id) {
        return queryService.next(id);
    }

    @Override
    public void close(String id) {
        queryService.close(id);
    }

    public ModificationQueryServiceFactory getFactory() {
        return new ModificationQueryServiceFactory() {
            @Override
            public ModificationQueryService createService(Collection<? extends DatawaveUser> proxiedUsers) {
                return QueryExecutorBeanService.this;
            }
        };
    }
}
