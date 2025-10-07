package datawave.core.query.configuration;

import org.apache.accumulo.core.data.Key;

public interface ResultContext {
    /**
     * Set the last result returned. Setting a result of null denotes this scan is finished.
     *
     * @param result
     *            The last result
     */
    void setLastResult(Key result);

    boolean isFinished();
}
