package datawave.query.testframework;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.security.Authorizations;

public interface AbstractAccumuloSetupHelper {
    public void printTables(final AccumuloClient client, final Authorizations authorizations) throws TableNotFoundException;
}
