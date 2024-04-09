package datawave.webservice.query.logic.composite;

import java.util.List;

import org.apache.log4j.Logger;

import com.google.common.base.Throwables;

import datawave.core.query.cache.ResultsPage;
import datawave.webservice.query.cachedresults.CacheableLogic;
import datawave.webservice.query.cachedresults.CacheableQueryRow;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.logic.AbstractQueryLogicTransformer;
import datawave.webservice.query.logic.QueryLogicTransformer;
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
