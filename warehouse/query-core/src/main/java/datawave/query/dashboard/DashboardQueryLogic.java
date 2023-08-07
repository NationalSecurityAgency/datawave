package datawave.query.dashboard;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.collections4.iterators.TransformIterator;

import datawave.core.common.extjs.ExtJsResponse;
import datawave.core.query.cache.ResultsPage;
import datawave.core.query.dashboard.DashboardFields;
import datawave.core.query.dashboard.DashboardSummary;
import datawave.core.query.logic.QueryLogicTransformer;
import datawave.core.query.logic.ResponseEnricher;
import datawave.microservice.query.Query;
import datawave.query.tables.ShardQueryLogic;
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
