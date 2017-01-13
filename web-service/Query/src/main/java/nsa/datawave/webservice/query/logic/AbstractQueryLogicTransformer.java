package nsa.datawave.webservice.query.logic;

import java.util.List;

import nsa.datawave.webservice.query.cache.ResultsPage;
import nsa.datawave.webservice.result.BaseQueryResponse;

public abstract class AbstractQueryLogicTransformer implements QueryLogicTransformer {
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
