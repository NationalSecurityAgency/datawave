package datawave.query.tables.facets;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import datawave.helpers.PrintUtility;
import datawave.ingest.mapreduce.handler.facet.FacetHandler;
import datawave.ingest.table.config.FacetTableConfigHelper;
import datawave.query.MockAccumuloRecordWriter;
import datawave.query.QueryTestTableHelper;
import datawave.query.RebuildingScannerTestHelper;

public class FacetQueryTestTableHelper extends QueryTestTableHelper {
    public static final String FACET_TABLE_NAME = "facet";
    public static final String FACET_HASH_TABLE_NAME = "facetHash";
    public static final String FACET_METADATA_TABLE_NAME = "facetMetadata";

    public FacetQueryTestTableHelper(String instanceName, Logger log, RebuildingScannerTestHelper.TEARDOWN teardown,
                    RebuildingScannerTestHelper.INTERRUPT interrupt)
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
    public void dumpTables(Authorizations auths) throws TableNotFoundException {
        super.dumpTables(auths);
        dumpTable(FACET_TABLE_NAME, auths);
        dumpTable(FACET_HASH_TABLE_NAME, auths);
        dumpTable(FACET_METADATA_TABLE_NAME, auths);
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
