package datawave.modification.query;

import datawave.query.exceptions.DatawaveQueryException;
import datawave.security.authorization.DatawaveUser;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.GenericResponse;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface ModificationQueryService {
    GenericResponse<String> createQuery(String logicName, Map<String,List<String>> paramsToMap) throws DatawaveQueryException;

    BaseQueryResponse next(String id) throws DatawaveQueryException;

    void close(String id) throws DatawaveQueryException;

    public interface ModificationQueryServiceFactory {
        ModificationQueryService createService(Collection<? extends DatawaveUser> proxiedUsers);
    }
}
