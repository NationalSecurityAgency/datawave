package datawave.core.query.remote;

import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.GenericResponse;
import datawave.webservice.result.VoidResponse;

import java.util.List;
import java.util.Map;

public interface RemoteQueryService {
    
    public GenericResponse<String> createQuery(String queryLogicName, Map<String,List<String>> queryParameters, Object callerObject);
    
    public BaseQueryResponse next(String id, Object callerObject);
    
    public VoidResponse close(String id, Object callerObject);
    
    public GenericResponse<String> planQuery(String queryLogicName, Map<String,List<String>> queryParameters, Object callerObject);
    
    public GenericResponse<String> planQuery(String id, Object callerObject);
}
