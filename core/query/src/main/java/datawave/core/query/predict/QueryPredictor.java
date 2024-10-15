package datawave.core.query.predict;

import java.io.Serializable;
import java.util.Set;

import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.BaseQueryMetric.Prediction;

/**
 * Created on 7/6/16.
 */
public interface QueryPredictor<T extends BaseQueryMetric> {

    Set<Prediction> predict(T query) throws PredictionException;

    class PredictionException extends Exception implements Serializable {
        private static final long serialVersionUID = 61150377750239886L;

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
