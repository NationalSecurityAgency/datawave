package datawave.core.query.predict;

import datawave.microservice.querymetric.BaseQueryMetric;

import java.util.Set;

public class NoOpQueryPredictor implements QueryPredictor {

    @Override
    public Set<BaseQueryMetric.Prediction> predict(BaseQueryMetric query) throws PredictionException {
        return null;
    }
}
