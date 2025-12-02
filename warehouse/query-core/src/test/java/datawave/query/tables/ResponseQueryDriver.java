package datawave.query.tables;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections4.iterators.TransformIterator;

import datawave.core.query.cache.ResultsPage;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.logic.BaseQueryLogic;
import datawave.core.query.logic.QueryLogicTransformer;
import datawave.webservice.result.BaseQueryResponse;

public class ResponseQueryDriver<K> {
    private BaseQueryLogic<K> logic;

    public ResponseQueryDriver(BaseQueryLogic<K> logic) {
        this.logic = logic;
    }

    public BaseQueryResponse drive(GenericQueryConfiguration config) {
        List<K> results = new ArrayList<>();
        TransformIterator transformerIterator = logic.getTransformIterator(config.getQuery());
        while (transformerIterator.hasNext()) {
            K o = (K) transformerIterator.next();
            results.add(o);
        }
        ResultsPage<K> resultsPage = new ResultsPage<>(results);
        QueryLogicTransformer transformer = logic.getTransformer(config.getQuery());
        return transformer.createResponse(resultsPage);
    }
}
