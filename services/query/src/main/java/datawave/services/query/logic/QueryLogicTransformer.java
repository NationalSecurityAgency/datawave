package datawave.services.query.logic;

import datawave.services.query.exception.EmptyObjectException;
import datawave.services.query.cache.ResultsPage;
import datawave.webservice.result.BaseQueryResponse;
import org.apache.commons.collections4.Transformer;

public interface QueryLogicTransformer<I,O> extends Transformer<I,O> {
    
    /*
     * @return a jaxb response object that is specific to this QueryLogic
     */
    BaseQueryResponse createResponse(ResultsPage resultList);
    
    /**
     * Transforms the input object (leaving it unchanged) into some output object.
     *
     * @param input
     *            the object to be transformed, should be left unchanged
     * @return a transformed object
     * @throws EmptyObjectException
     *             if the result is empty
     */
    @Override
    O transform(I input) throws EmptyObjectException;
}
