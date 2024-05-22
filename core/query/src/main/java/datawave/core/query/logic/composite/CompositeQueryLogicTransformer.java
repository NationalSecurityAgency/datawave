package datawave.core.query.logic.composite;

import java.util.List;

import org.apache.log4j.Logger;

import com.google.common.base.Throwables;

import datawave.core.query.cachedresults.CacheableLogic;
import datawave.core.query.logic.AbstractQueryLogicTransformer;
import datawave.core.query.logic.QueryLogicTransformer;
import datawave.webservice.query.cachedresults.CacheableQueryRow;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.result.BaseQueryResponse;

public class CompositeQueryLogicTransformer<I,O> extends AbstractQueryLogicTransformer<I,O> implements CacheableLogic {

    protected static final Logger log = Logger.getLogger(CompositeQueryLogicTransformer.class);

    private List<QueryLogicTransformer<I,O>> delegates = null;

    public CompositeQueryLogicTransformer(List<QueryLogicTransformer<I,O>> delegates) {
        this.delegates = delegates;
    }

    @Override
    public O transform(I input) {
        // The objects put into the pageQueue have already been transformed, so no transformation required here.
        return (O) input;
    }

    @Override
    public CacheableQueryRow writeToCache(Object o) throws QueryException {
        CacheableQueryRow result = null;
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
    public Object readFromCache(CacheableQueryRow cacheableQueryRow) {
        Object result = null;
        for (QueryLogicTransformer t : delegates) {
            if (t instanceof CacheableLogic) {
                CacheableLogic c = (CacheableLogic) t;
                try {
                    result = c.readFromCache(cacheableQueryRow);
                } catch (Exception e) {
                    log.warn("Error calling writeToCache on delegate, continuing...", e);
                }
            }
        }
        return result;
    }

    @Override
    public BaseQueryResponse createResponse(List<Object> resultList) {
        Exception lastFailure = null;
        for (QueryLogicTransformer t : delegates) {
            if (t instanceof AbstractQueryLogicTransformer) {
                AbstractQueryLogicTransformer a = (AbstractQueryLogicTransformer) t;
                try {
                    log.trace("createResponse List<Object>");
                    return a.createResponse(resultList);
                } catch (Exception e) {
                    log.warn("Error calling createResponse on delegate, trying the next one", e);
                    lastFailure = e;
                }
            }
        }
        if (lastFailure != null) {
            Throwables.propagate(lastFailure);
        }
        return null;
    }

}
