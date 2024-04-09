package datawave.core.query.cache;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class ResultsPage {
    public enum Status {
        NONE, PARTIAL, COMPLETE
    };

    private List<Object> results = null;
    private Status status = null;

    public ResultsPage() {
        this(new ArrayList<>());
    }

    public ResultsPage(List<Object> c) {
        this(c, (c.isEmpty() ? Status.NONE : Status.COMPLETE));
    }

    public ResultsPage(List<Object> c, Status s) {
        setResults(c);
        setStatus(s);
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public List<Object> getResults() {
        return results;
    }

    public void setResults(List<Object> results) {
        this.results = results;
    }
}
