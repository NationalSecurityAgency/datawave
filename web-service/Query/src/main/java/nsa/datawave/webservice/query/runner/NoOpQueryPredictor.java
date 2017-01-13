package nsa.datawave.webservice.query.runner;

import java.util.Set;

import nsa.datawave.webservice.query.metric.BaseQueryMetric;
import nsa.datawave.webservice.query.metric.BaseQueryMetric.Prediction;

public class NoOpQueryPredictor implements QueryPredictor {
    
    @Override
    public Set<Prediction> predict(BaseQueryMetric query) throws PredictionException {
        return null;
    }
}
