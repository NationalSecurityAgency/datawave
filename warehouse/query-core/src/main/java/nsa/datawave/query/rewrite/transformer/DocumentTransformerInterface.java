package nsa.datawave.query.rewrite.transformer;

import java.util.List;

import nsa.datawave.query.transformer.EventQueryTransformerInterface;
import nsa.datawave.webservice.query.Query;
import nsa.datawave.webservice.query.logic.QueryLogic;
import nsa.datawave.webservice.result.BaseQueryResponse;

public interface DocumentTransformerInterface<Q> extends EventQueryTransformerInterface<Q> {
    
    BaseQueryResponse createResponse(List<Object> resultList);
    
    void initialize(String tableName, Query settings, boolean reducedResponse);
    
    void initialize(QueryLogic<Q> logic, Query settings, boolean reducedResponse);
    
}
