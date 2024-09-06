package datawave.query.transformer;

import java.util.List;

import datawave.core.query.logic.QueryLogic;
import datawave.microservice.query.Query;
import datawave.webservice.result.BaseQueryResponse;

public interface DocumentTransformerInterface<Q> extends EventQueryTransformerInterface<Q> {

    BaseQueryResponse createResponse(List<Object> resultList);

    void initialize(String tableName, Query settings, boolean reducedResponse);

    void initialize(QueryLogic<Q> logic, Query settings, boolean reducedResponse);

}
