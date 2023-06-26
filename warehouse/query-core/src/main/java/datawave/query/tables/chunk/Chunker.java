package datawave.query.tables.chunk;

import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.webservice.query.Query;

import java.util.Iterator;

/**
 * A <code>java.util.Iterator</code> interface for splitting a query into smaller chunks to be executed as separate queries.
 */
public abstract class Chunker implements Iterator<Query>, Cloneable {
    public abstract void setBaseQuery(Query query);

    public void remove() {
        throw new UnsupportedOperationException();
    }

    public boolean preInitializeQueryLogic() {
        return false;
    }

    /**
     * Do any extra chunker initialization. If you need extra initialization you probably also need to override preInitializeQueryLogic to have it return true.
     *
     * @param config
     *            a query configuration
     */
    public void initialize(GenericQueryConfiguration config) {}

    public abstract Chunker clone();
}
