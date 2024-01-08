package datawave.query.tables.facets;

import datawave.helpers.PrintUtility;
import datawave.query.testframework.AbstractAccumuloSetupHelper;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.security.Authorizations;

public class FacetedQuerySetupHelper implements AbstractAccumuloSetupHelper {
    public static final String FACET_TABLE_NAME = "facet";
    public static final String FACET_HASH_TABLE_NAME = "facetHash";
    public static final String FACET_METADATA_TABLE_NAME = "facetMetadata";
    @Override
    public void printTables(AccumuloClient client, Authorizations auths) throws TableNotFoundException {
        PrintUtility.printTable(client, auths, FACET_TABLE_NAME);
        PrintUtility.printTable(client, auths, FACET_METADATA_TABLE_NAME);
        PrintUtility.printTable(client, auths, FACET_HASH_TABLE_NAME);
    }
}
