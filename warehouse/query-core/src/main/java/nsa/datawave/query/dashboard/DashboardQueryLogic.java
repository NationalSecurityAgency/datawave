package nsa.datawave.query.dashboard;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import nsa.datawave.query.rewrite.tables.RefactoredShardQueryLogic;
import nsa.datawave.webservice.common.extjs.ExtJsResponse;
import nsa.datawave.webservice.query.Query;
import nsa.datawave.webservice.query.cache.ResultsPage;
import nsa.datawave.webservice.query.dashboard.DashboardFields;
import nsa.datawave.webservice.query.dashboard.DashboardSummary;
import nsa.datawave.webservice.query.logic.QueryLogicTransformer;
import nsa.datawave.webservice.query.result.event.DefaultEvent;
import nsa.datawave.webservice.result.BaseQueryResponse;

import org.apache.commons.collections.iterators.TransformIterator;

/**
 * Aggregate a range of query metrics into a single DashboardSummary object.
 */
public class DashboardQueryLogic extends RefactoredShardQueryLogic implements QueryLogicTransformer {
    
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
            DefaultEvent event = (DefaultEvent) transformer.transform(origIter.next());
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
    
}
