package datawave.query.tables.ssdeep.testframework;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;

import datawave.helpers.PrintUtility;
import datawave.ingest.mapreduce.handler.ssdeep.SSDeepIndexHandler;
import datawave.ingest.table.config.SSDeepIndexTableConfigHelper;
import datawave.query.MockAccumuloRecordWriter;
import datawave.query.QueryTestTableHelper;
import datawave.query.RebuildingScannerTestHelper;

public class SSDeepQueryTestTableHelper extends QueryTestTableHelper {
    public static final String SSDEEP_INDEX_TABLE_NAME = SSDeepIndexHandler.DEFAULT_SSDEEP_INDEX_TABLE_NAME;

    public SSDeepQueryTestTableHelper(AccumuloClient client, Logger log)
                    throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {
        super(client, log);
    }

    public SSDeepQueryTestTableHelper(String instanceName, Logger log)
                    throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {
        this(instanceName, log, RebuildingScannerTestHelper.TEARDOWN.EVERY_OTHER, RebuildingScannerTestHelper.INTERRUPT.EVERY_OTHER);
    }

    public SSDeepQueryTestTableHelper(String instanceName, Logger log, RebuildingScannerTestHelper.TEARDOWN teardown,
                    RebuildingScannerTestHelper.INTERRUPT interrupt)
                    throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {
        super(instanceName, log, teardown, interrupt);
    }

    @Override
    public void printTables(Authorizations auths) throws TableNotFoundException {
        super.printTables(auths);
        PrintUtility.printTable(client, auths, SSDEEP_INDEX_TABLE_NAME);
    }

    @Override
    public void dumpTables(Authorizations auths) throws TableNotFoundException {
        super.dumpTables(auths);
        dumpTable(SSDEEP_INDEX_TABLE_NAME, auths);
    }

    @Override
    protected void createTables() throws AccumuloSecurityException, AccumuloException, TableNotFoundException, TableExistsException {
        super.createTables();

        TableOperations tops = client.tableOperations();
        deleteAndCreateTable(tops, SSDEEP_INDEX_TABLE_NAME);
    }

    @Override
    public void configureTables(MockAccumuloRecordWriter writer) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        super.configureTables(writer);

        configureAShardRelatedTable(writer, new SSDeepIndexTableConfigHelper(), SSDeepIndexHandler.SSDEEP_INDEX_TABLE_NAME, SSDEEP_INDEX_TABLE_NAME);
    }
}
