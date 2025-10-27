package datawave.query.index.day;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;

/**
 * Enforce a standard {@link #convert(AccumuloClient, Authorizations, String, String)} method for all implementing classes
 */
public abstract class AbstractIndexIngest {

    /**
     * Create and configure the destination table
     *
     * @param client
     *            the AccumuloClient
     * @param destination
     *            the destination table name
     */
    protected abstract void configureDestination(AccumuloClient client, String destination);

    /**
     * Given keys in a source table, convert keys and write them to a destination table
     *
     * @param client
     *            the AccumuloClient
     * @param auths
     *            the Authorizations
     * @param source
     *            the source table name
     * @param destination
     *            the destination table name
     */
    public abstract void convert(AccumuloClient client, Authorizations auths, String source, String destination);
}
