package datawave.microservice.query.logic;

import datawave.webservice.query.cache.ResultsPage;
import datawave.webservice.result.BaseQueryResponse;

import java.util.List;

public abstract class AbstractQueryLogicTransformer<I,O> implements QueryLogicTransformer<I,O> {
    public static final String PARTIAL_RESULTS = "Partial/incomplete page of results returned probably due to memory constraints";
    
    public abstract BaseQueryResponse createResponse(List<Object> resultList);
    
    @Override
    public BaseQueryResponse createResponse(ResultsPage page) {
        BaseQueryResponse response = createResponse(page.getResults());
        if (page.getStatus() == ResultsPage.Status.PARTIAL) {
            response.addMessage(PARTIAL_RESULTS);
            response.setPartialResults(true);
        }
        return response;
    }
}
