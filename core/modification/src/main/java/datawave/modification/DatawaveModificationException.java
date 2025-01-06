package datawave.modification;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import datawave.webservice.query.exception.QueryException;

public class DatawaveModificationException extends RuntimeException {

    private List<QueryException> exceptions = new ArrayList<>();

    public DatawaveModificationException(QueryException qe) {
        super(qe);
        exceptions.add(qe);
    }

    public DatawaveModificationException(String msg, QueryException qe) {
        super(msg, qe);
        exceptions.add(qe);
    }

    public void addException(QueryException e) {
        exceptions.add(e);
    }

    public List<QueryException> getExceptions() {
        return Collections.unmodifiableList(exceptions);
    }
}
