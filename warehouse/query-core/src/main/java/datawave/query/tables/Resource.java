package datawave.query.tables;

import com.google.common.base.Preconditions;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

public abstract class Resource<T> implements Closeable, Iterable<T> {


    /**
     * Our connector.
     */
    protected AccumuloClient client;

    public Resource(final AccumuloClient client) {
        Preconditions.checkNotNull(client);

        this.client = client;
    }

    public Resource(final datawave.query.tables.AccumuloResource other) {
        // deep copy
    }

    protected AccumuloClient getClient() {
        return client;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.io.Closeable#close()
     */
    @Override
    public void close() throws IOException {
        // nothing to close.
    }

    protected void init(final String tableName, final Set<Authorizations> auths, Collection<Range> currentRange) throws TableNotFoundException {
        // do nothing.
    }

    /**
     * Sets the option on this currently running resource.
     *
     * @param options
     *            options to set
     * @return the resource
     */
    public Resource setOptions(SessionOptions options) {
        return this;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Iterable#iterator()
     */
    @Override
    public Iterator<T> iterator() {
        return Collections.emptyIterator();
    }


}
