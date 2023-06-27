package datawave.core.query.logic;

import java.util.List;

import datawave.core.query.cache.ResultsPage;
import datawave.webservice.result.BaseQueryResponse;

public abstract class AbstractQueryLogicTransformer<I,O> implements QueryLogicTransformer<I,O> {

    public static final String PARTIAL_RESULTS = "Partial/incomplete page of results returned probably due to memory constraints";

    public abstract BaseQueryResponse createResponse(List<Object> resultList);

    protected long queryExecutionForCurrentPageStartTime;

    protected ResponseEnricher enricher;

    @Override
    public void setResponseEnricher(ResponseEnricher enricher) {
        this.enricher = enricher;
    }

    public ResponseEnricher getResponseEnricher() {
        return enricher;
    }

    @Override
    public BaseQueryResponse createResponse(ResultsPage page) {
        BaseQueryResponse response = createResponse(page.getResults());
        if (page.getStatus() == ResultsPage.Status.PARTIAL) {
            response.addMessage(PARTIAL_RESULTS);
            response.setPartialResults(true);
        }
        ResponseEnricher enricher = getResponseEnricher();
        return (enricher == null ? response : enricher.enrichResponse(response));
    }

    @Override
    public void setQueryExecutionForPageStartTime(long queryExecutionForCurrentPageStartTime) {
        this.queryExecutionForCurrentPageStartTime = queryExecutionForCurrentPageStartTime;
    }
}
