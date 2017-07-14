package datawave.webservice.query.runner;

import java.util.Set;

import datawave.webservice.query.metric.BaseQueryMetric;
import datawave.webservice.query.metric.BaseQueryMetric.Prediction;

public class NoOpQueryPredictor implements QueryPredictor {
    
    @Override
    public Set<Prediction> predict(BaseQueryMetric query) throws PredictionException {
        return null;
    }
}
