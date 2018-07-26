package datawave.webservice.query.logic;

import datawave.webservice.query.cache.ResultsPage;
import datawave.webservice.query.exception.EmptyObjectException;
import datawave.webservice.result.BaseQueryResponse;

import org.apache.commons.collections.Transformer;

/**
 * NOTE: The transform method may throw an EmptyObjectException when the TransformIterator is to call next instead of returning null.
 */
public interface QueryLogicTransformer extends Transformer {
    
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
    Object transform(Object input) throws EmptyObjectException;
}
