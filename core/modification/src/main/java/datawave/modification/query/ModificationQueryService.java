package datawave.modification.query;

import java.util.List;
import java.util.Map;

import datawave.query.exceptions.DatawaveQueryException;
import datawave.security.authorization.ProxiedUserDetails;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.GenericResponse;

public interface ModificationQueryService {
    GenericResponse<String> createQuery(String logicName, Map<String,List<String>> paramsToMap) throws DatawaveQueryException;

    BaseQueryResponse next(String id) throws DatawaveQueryException;

    void close(String id) throws DatawaveQueryException;

    public interface ModificationQueryServiceFactory {
        ModificationQueryService createService(ProxiedUserDetails userDetails);
    }
}
