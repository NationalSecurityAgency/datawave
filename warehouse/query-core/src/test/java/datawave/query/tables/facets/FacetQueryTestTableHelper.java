package datawave.query.tables.facets;

import datawave.helpers.PrintUtility;
import datawave.ingest.mapreduce.handler.facet.FacetHandler;
import datawave.ingest.table.config.FacetTableConfigHelper;
import datawave.query.MockAccumuloRecordWriter;
import datawave.query.QueryTestTableHelper;
import datawave.query.RebuildingScannerTestHelper;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;

public class FacetQueryTestTableHelper extends QueryTestTableHelper {
    public static final String FACET_TABLE_NAME = "facet";
    public static final String FACET_HASH_TABLE_NAME = "facetHash";
    public static final String FACET_METADATA_TABLE_NAME = "facetMetadata";

    public FacetQueryTestTableHelper(AccumuloClient client, Logger log) throws AccumuloSecurityException, AccumuloException, TableExistsException,
            TableNotFoundException {
        super(client, log);
    }

    public FacetQueryTestTableHelper(String instanceName, Logger log) throws AccumuloSecurityException, AccumuloException, TableExistsException,
            TableNotFoundException {
        this(instanceName, log, RebuildingScannerTestHelper.TEARDOWN.EVERY_OTHER, RebuildingScannerTestHelper.INTERRUPT.EVERY_OTHER);
    }

    public FacetQueryTestTableHelper(String instanceName, Logger log, RebuildingScannerTestHelper.TEARDOWN teardown, RebuildingScannerTestHelper.INTERRUPT interrupt)
            throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {
        super(instanceName, log, teardown, interrupt);
    }

    @Override
    public void printTables(Authorizations auths) throws TableNotFoundException {
        super.printTables(auths);
        PrintUtility.printTable(client, auths, FACET_TABLE_NAME);
        PrintUtility.printTable(client, auths, FACET_METADATA_TABLE_NAME);
        PrintUtility.printTable(client, auths, FACET_HASH_TABLE_NAME);
    }

    @Override
    public void dumpTables(Authorizations auths) {
        super.dumpTables(auths);

        try {
            dumpTable(FACET_TABLE_NAME, auths);
            dumpTable(FACET_HASH_TABLE_NAME, auths);
            dumpTable(FACET_METADATA_TABLE_NAME, auths);
        } catch (TableNotFoundException e) {
            // should not happen
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    protected void createTables() throws AccumuloSecurityException, AccumuloException, TableNotFoundException, TableExistsException {
        super.createTables();

        TableOperations tops = client.tableOperations();
        deleteAndCreateTable(tops, FACET_TABLE_NAME);
        deleteAndCreateTable(tops, FACET_HASH_TABLE_NAME);
        deleteAndCreateTable(tops, FACET_METADATA_TABLE_NAME);
    }

    @Override
    public void configureTables(MockAccumuloRecordWriter writer) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        super.configureTables(writer);

        configureAShardRelatedTable(writer, new FacetTableConfigHelper(), FacetHandler.FACET_TABLE_NAME, FACET_TABLE_NAME);
        configureAShardRelatedTable(writer, new FacetTableConfigHelper(), FacetHandler.FACET_METADATA_TABLE_NAME, FACET_METADATA_TABLE_NAME);
        configureAShardRelatedTable(writer, new FacetTableConfigHelper(), FacetHandler.FACET_HASH_TABLE_NAME, FACET_HASH_TABLE_NAME);
    }
}
