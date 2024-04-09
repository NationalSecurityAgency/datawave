package datawave.webservice.query.logic;

import org.apache.commons.collections4.Transformer;

import datawave.core.query.cache.ResultsPage;
import datawave.webservice.query.exception.EmptyObjectException;
import datawave.webservice.result.BaseQueryResponse;

public interface QueryLogicTransformer<I,O> extends Transformer<I,O> {

    /**
     * Set a response transform to be applied in the createResponse method.
     *
     * @param responseTransform
     *            a response transform
     */
    void setResponseEnricher(ResponseEnricher responseTransform);

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

    void setQueryExecutionForPageStartTime(long queryExecutionForCurrentPageStartTime);
}
