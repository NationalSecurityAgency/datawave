package datawave.query.dashboard;

import datawave.query.tables.ShardQueryLogic;
import datawave.services.common.extjs.ExtJsResponse;
import datawave.services.query.cache.ResultsPage;
import datawave.services.query.dashboard.DashboardFields;
import datawave.services.query.dashboard.DashboardSummary;
import datawave.services.query.logic.QueryLogicTransformer;
import datawave.webservice.query.Query;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.result.BaseQueryResponse;
import org.apache.commons.collections4.iterators.TransformIterator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Aggregate a range of query metrics into a single DashboardSummary object.
 */
public class DashboardQueryLogic extends ShardQueryLogic implements QueryLogicTransformer {
    
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
        QueryLogicTransformer transformer = super.getTransformer(settings);
        DashboardSummary summary = new DashboardSummary(settings.getEndDate());
        while (origIter.hasNext()) {
            EventBase event = (EventBase) transformer.transform(origIter.next());
            DashboardFields.addEvent(summary, event);
        }
        
        return new TransformIterator(Arrays.asList(summary).iterator(), this);
    }
    
    @Override
    public BaseQueryResponse createResponse(ResultsPage resultList) {
        final List<Object> results = resultList.getResults();
        final List<DashboardSummary> list = new ArrayList<>(results.size());
        
        for (Object o : results) {
            list.add((DashboardSummary) o);
        }
        
        return new ExtJsResponse<>(list);
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
