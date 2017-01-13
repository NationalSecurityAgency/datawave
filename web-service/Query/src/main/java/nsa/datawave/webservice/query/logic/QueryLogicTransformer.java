package nsa.datawave.webservice.query.logic;

import nsa.datawave.webservice.query.cache.ResultsPage;
import nsa.datawave.webservice.result.BaseQueryResponse;

import org.apache.commons.collections.Transformer;

public interface QueryLogicTransformer extends Transformer {
    
    /*
     * @return a jaxb response object that is specific to this QueryLogic
     */
    public BaseQueryResponse createResponse(ResultsPage resultList);
}
