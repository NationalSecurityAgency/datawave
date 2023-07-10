package datawave.query.transformer;

import java.util.List;

import org.apache.commons.collections4.Transformer;

import datawave.query.model.QueryModel;
import datawave.webservice.query.Query;
import datawave.webservice.query.cachedresults.CacheableQueryRow;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.logic.QueryLogic;
import datawave.webservice.query.logic.QueryLogicTransformer;

public interface EventQueryTransformerInterface<Q> extends QueryLogicTransformer {

    void initialize(String tableName, Query settings);

    void initialize(QueryLogic<Q> logic, Query settings);

    Object transform(Object input);

    List<CacheableQueryRow> writeToCache(Object o) throws QueryException;

    List<Object> readFromCache(List<CacheableQueryRow> cacheableQueryRowList);

    Transformer getEventQueryDataDecoratorTransformer();

    void setEventQueryDataDecoratorTransformer(Transformer eventQueryDataDecoratorTransformer);

    List<String> getContentFieldNames();

    void setContentFieldNames(List<String> contentFieldNames);

    QueryModel getQm();

    void setQm(QueryModel qm);

}
