package datawave.webservice.query.logic;

import datawave.webservice.query.cache.ResultsPage;
import datawave.webservice.result.BaseQueryResponse;
import org.apache.commons.collections4.Transformer;

public interface QueryLogicTransformer extends Transformer {
    
    /*
     * @return a jaxb response object that is specific to this QueryLogic
     */
    public BaseQueryResponse createResponse(ResultsPage resultList);
}
