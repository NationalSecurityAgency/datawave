package datawave.query.dashboard;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.collections4.iterators.TransformIterator;

import datawave.query.tables.ShardQueryLogic;
import datawave.webservice.common.extjs.ExtJsResponse;
import datawave.webservice.query.Query;
import datawave.webservice.query.cache.ResultsPage;
import datawave.webservice.query.dashboard.DashboardFields;
import datawave.webservice.query.dashboard.DashboardSummary;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.logic.QueryLogicTransformer;
import datawave.webservice.query.logic.ResponseEnricher;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.result.BaseQueryResponse;

/**
 * Aggregate a range of query metrics into a single DashboardSummary object.
 */
public class DashboardQueryLogic extends ShardQueryLogic implements QueryLogicTransformer {

    private ResponseEnricher responseEnricher;

    protected long queryExecutionForCurrentPageStartTime;

    public DashboardQueryLogic() {}

    public DashboardQueryLogic(DashboardQueryLogic logic) {
        super(logic);
        this.responseEnricher = logic.responseEnricher;
        this.queryExecutionForCurrentPageStartTime = logic.queryExecutionForCurrentPageStartTime;
    }

    @Override
    public DashboardQueryLogic clone() {
        return new DashboardQueryLogic(this);
    }

    @Override
    public QueryLogicTransformer getTransformer(Query settings) {
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public TransformIterator getTransformIterator(Query settings) {
        TransformIterator origIter = super.getTransformIterator(settings);
        DashboardSummary summary = new DashboardSummary(settings.getEndDate());
        while (origIter.hasNext()) {
            EventBase event = (EventBase) origIter.next();
            DashboardFields.addEvent(summary, event);
        }
        return new TransformIterator(Arrays.asList(summary).iterator(), this);
    }

    @Override
    public void setResponseEnricher(ResponseEnricher responseEnricher) {
        this.responseEnricher = responseEnricher;
    }

    @Override
    public BaseQueryResponse createResponse(ResultsPage resultList) {
        final List<Object> results = resultList.getResults();
        final List<DashboardSummary> list = new ArrayList<>(results.size());

        for (Object o : results) {
            list.add((DashboardSummary) o);
        }

        ExtJsResponse response = new ExtJsResponse<>(list);
        if (responseEnricher != null) {
            return responseEnricher.enrichResponse(response);
        } else {
            return response;
        }
    }

    @Override
    public Object transform(Object input) {
        return input;
    }

    @Override
    public void setQueryExecutionForPageStartTime(long queryExecutionForCurrentPageStartTime) {
        this.queryExecutionForCurrentPageStartTime = queryExecutionForCurrentPageStartTime;
    }

}
