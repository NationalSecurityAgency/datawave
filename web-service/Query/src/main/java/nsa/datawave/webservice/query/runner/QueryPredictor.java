package nsa.datawave.webservice.query.runner;

import java.util.Set;

import nsa.datawave.webservice.query.metric.BaseQueryMetric;
import nsa.datawave.webservice.query.metric.BaseQueryMetric.Prediction;

import java.io.Serializable;

/**
 * Created on 7/6/16.
 */
public interface QueryPredictor<T extends BaseQueryMetric> {
    
    public Set<Prediction> predict(T query) throws PredictionException;
    
    public static class PredictionException extends Exception implements Serializable {
        
        public PredictionException() {
            super();
        }
        
        public PredictionException(String message) {
            super(message);
        }
        
        public PredictionException(String message, Throwable cause) {
            super(message, cause);
        }
        
        public PredictionException(Throwable cause) {
            super(cause);
        }
    }
    
}
