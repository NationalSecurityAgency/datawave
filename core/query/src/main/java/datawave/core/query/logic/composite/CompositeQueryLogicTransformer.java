package datawave.core.query.logic.composite;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;

import datawave.core.query.cachedresults.CacheableLogic;
import datawave.core.query.logic.AbstractQueryLogicTransformer;
import datawave.core.query.logic.QueryLogicTransformer;
import datawave.core.query.logic.WritesQueryMetrics;
import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.webservice.query.cachedresults.CacheableQueryRow;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.result.BaseQueryResponse;

public class CompositeQueryLogicTransformer<I,O> extends AbstractQueryLogicTransformer<I,O> implements CacheableLogic, WritesQueryMetrics {

    protected static final Logger log = LoggerFactory.getLogger(CompositeQueryLogicTransformer.class);

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

    @Override
    public boolean hasMetrics() {
        for (QueryLogicTransformer d : delegates) {
            if (d instanceof WritesQueryMetrics) {
                if (((WritesQueryMetrics) d).hasMetrics()) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public void writeQueryMetrics(BaseQueryMetric metric) {
        // if any timing details have been returned, add metrics
        if (hasMetrics()) {
            metric.setSourceCount(getSourceCount());
            metric.setNextCount(getNextCount());
            metric.setSeekCount(getSeekCount());
            metric.setYieldCount(getYieldCount());
            metric.setDocRanges(getDocRanges());
            metric.setFiRanges(getFiRanges());
        }
    }

    @Override
    public void resetMetrics() {
        for (QueryLogicTransformer d : delegates) {
            if (d instanceof WritesQueryMetrics) {
                ((WritesQueryMetrics) d).resetMetrics();
            }
        }
    }

    @Override
    public long getFiRanges() {
        long total = 0;
        for (QueryLogicTransformer d : delegates) {
            if (d instanceof WritesQueryMetrics) {
                total += ((WritesQueryMetrics) d).getFiRanges();
            }
        }
        return total;
    }

    @Override
    public long getDocRanges() {
        long total = 0;
        for (QueryLogicTransformer d : delegates) {
            if (d instanceof WritesQueryMetrics) {
                total += ((WritesQueryMetrics) d).getDocRanges();
            }
        }
        return total;
    }

    @Override
    public long getSourceCount() {
        long total = 0;
        for (QueryLogicTransformer d : delegates) {
            if (d instanceof WritesQueryMetrics) {
                total += ((WritesQueryMetrics) d).getSourceCount();
            }
        }
        return total;
    }

    @Override
    public long getSeekCount() {
        long total = 0;
        for (QueryLogicTransformer d : delegates) {
            if (d instanceof WritesQueryMetrics) {
                total += ((WritesQueryMetrics) d).getSeekCount();
            }
        }
        return total;
    }

    @Override
    public long getNextCount() {
        long total = 0;
        for (QueryLogicTransformer d : delegates) {
            if (d instanceof WritesQueryMetrics) {
                total += ((WritesQueryMetrics) d).getNextCount();
            }
        }
        return total;
    }

    @Override
    public long getYieldCount() {
        long total = 0;
        for (QueryLogicTransformer d : delegates) {
            if (d instanceof WritesQueryMetrics) {
                total += ((WritesQueryMetrics) d).getYieldCount();
            }
        }
        return total;
    }
}
