package datawave.webservice.query.runner;

import java.util.Set;

import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.BaseQueryMetric.Prediction;

public class NoOpQueryPredictor implements QueryPredictor {
    
    @Override
    public Set<Prediction> predict(BaseQueryMetric query) throws PredictionException {
        return null;
    }
}
