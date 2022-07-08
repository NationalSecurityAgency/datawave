package datawave.core.query.logic.composite;

import datawave.core.query.cache.ResultsPage;
import datawave.core.query.cachedresults.CacheableLogic;
import datawave.core.query.logic.AbstractQueryLogicTransformer;
import datawave.core.query.logic.QueryLogicTransformer;
import datawave.webservice.query.cachedresults.CacheableQueryRow;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.result.BaseQueryResponse;
import org.apache.log4j.Logger;

import java.util.List;

public class CompositeQueryLogicTransformer<I,O> extends AbstractQueryLogicTransformer<I,O> implements CacheableLogic {
    
    protected static final Logger log = Logger.getLogger(CompositeQueryLogicTransformer.class);
    
    private List<QueryLogicTransformer<I,O>> delegates = null;
    
    public CompositeQueryLogicTransformer(List<QueryLogicTransformer<I,O>> delegates) {
        this.delegates = delegates;
    }
    
    @Override
    public O transform(I input) {
        O result = null;
        Exception ex = null;
        for (QueryLogicTransformer<I,O> t : delegates) {
            try {
                log.trace("transform");
                result = t.transform(input);
            } catch (Exception e) {
                log.warn("Error calling transform on delegate, continuing...", e);
                ex = e;
            }
        }
        if (null == result && null != ex) {
            throw new RuntimeException("Unable to transform result", ex);
        }
        return result;
    }
    
    @Override
    public List<CacheableQueryRow> writeToCache(Object o) throws QueryException {
        List<CacheableQueryRow> result = null;
        for (QueryLogicTransformer t : delegates) {
            if (t instanceof CacheableLogic) {
                CacheableLogic c = (CacheableLogic) t;
                try {
                    result = c.writeToCache(o);
                } catch (Exception e) {
                    log.warn("Error calling writeToCache on delegate, continuing...", e);
                }
            }
        }
        return result;
    }
    
    @Override
    public List<Object> readFromCache(List<CacheableQueryRow> row) {
        List<Object> result = null;
        for (QueryLogicTransformer t : delegates) {
            if (t instanceof CacheableLogic) {
                CacheableLogic c = (CacheableLogic) t;
                try {
                    result = c.readFromCache(row);
                } catch (Exception e) {
                    log.warn("Error calling writeToCache on delegate, continuing...", e);
                }
            }
        }
        return result;
    }
    
    @Override
    public BaseQueryResponse createResponse(ResultsPage resultList) {
        BaseQueryResponse result = null;
        for (QueryLogicTransformer t : delegates) {
            try {
                log.trace("createResponse ResultsPage");
                result = t.createResponse(resultList);
            } catch (Exception e) {
                log.warn("Error calling createResponse on delegate, continuing...", e);
            }
        }
        return result;
    }
    
    @Override
    public BaseQueryResponse createResponse(List<Object> resultList) {
        BaseQueryResponse result = null;
        for (QueryLogicTransformer t : delegates) {
            if (t instanceof AbstractQueryLogicTransformer) {
                AbstractQueryLogicTransformer a = (AbstractQueryLogicTransformer) t;
                try {
                    log.trace("createResponse List<Object>");
                    result = a.createResponse(resultList);
                } catch (Exception e) {
                    log.warn("Error calling createResponse on delegate, continuing...", e);
                }
            }
        }
        return result;
    }
    
}
