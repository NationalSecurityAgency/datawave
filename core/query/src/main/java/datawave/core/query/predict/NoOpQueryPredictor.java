package datawave.core.query.predict;

import java.util.Set;

import datawave.microservice.querymetric.BaseQueryMetric;

public class NoOpQueryPredictor implements QueryPredictor {

    @Override
    public Set<BaseQueryMetric.Prediction> predict(BaseQueryMetric query) throws PredictionException {
        return null;
    }
}
