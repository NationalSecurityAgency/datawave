package datawave.core.query.cache;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class ResultsPage<T> {
    public enum Status {
        NONE, PARTIAL, COMPLETE
    }

    private List<T> results;
    private Status status;

    public ResultsPage() {
        this(new ArrayList<>());
    }

    public ResultsPage(List<T> results) {
        this(results, Status.COMPLETE);
    }

    public ResultsPage(List<T> results, Status status) {
        this.results = results;
        this.status = status;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public List<T> getResults() {
        return results;
    }

    public void setResults(List<T> results) {
        this.results = results;
    }
}
