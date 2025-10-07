package datawave.query.iterator.errors;

import java.io.IOException;

public class UnindexedException extends IOException {

    /**
     *
     */
    private static final long serialVersionUID = -7369895799596098161L;

    public UnindexedException(String message) {
        super(message);
    }
}
