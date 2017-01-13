package nsa.datawave.query.transformer;

import java.util.List;

import nsa.datawave.query.model.QueryModel;
import nsa.datawave.webservice.query.Query;
import nsa.datawave.webservice.query.cachedresults.CacheableQueryRow;
import nsa.datawave.webservice.query.exception.QueryException;
import nsa.datawave.webservice.query.logic.QueryLogic;
import nsa.datawave.webservice.query.logic.QueryLogicTransformer;

import org.apache.commons.collections.Transformer;

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
