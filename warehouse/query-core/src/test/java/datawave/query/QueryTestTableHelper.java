package datawave.query;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.helpers.PrintUtility;
import datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler;
import datawave.ingest.table.config.MetadataTableConfigHelper;
import datawave.ingest.table.config.ShardTableConfigHelper;
import datawave.ingest.table.config.TableConfigHelper;
import datawave.query.tables.ShardQueryLogic;
import datawave.util.TableName;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import datawave.accumulo.inmemory.InMemoryInstance;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Creates and configures tables that are commonly needed for queries
 */
public class QueryTestTableHelper {
    
    public static final String METADATA_TABLE_NAME = "metadata";
    public static final String SHARD_DICT_INDEX_NAME = "shardTermDictionary";
    public static final String MODEL_TABLE_NAME = "DatawaveMetadata";
    
    private static final BatchWriterConfig bwCfg = new BatchWriterConfig().setMaxLatency(1, TimeUnit.SECONDS).setMaxMemory(1000L).setMaxWriteThreads(1);
    
    public final AccumuloClient client;
    protected final Logger log; // passed in for context when debugging
    
    public QueryTestTableHelper(AccumuloClient client, Logger log) throws AccumuloSecurityException, AccumuloException, TableExistsException,
                    TableNotFoundException {
        // create mock instance and connector
        this.client = client;
        this.log = log;
        createTables();
    }
    
    public QueryTestTableHelper(String instanceName, Logger log) throws AccumuloSecurityException, AccumuloException, TableExistsException,
                    TableNotFoundException {
        this(instanceName, log, RebuildingScannerTestHelper.TEARDOWN.EVERY_OTHER, RebuildingScannerTestHelper.INTERRUPT.EVERY_OTHER);
    }
    
    public QueryTestTableHelper(String instanceName, Logger log, RebuildingScannerTestHelper.TEARDOWN teardown, RebuildingScannerTestHelper.INTERRUPT interrupt)
                    throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {
        // create mock instance and connector
        InMemoryInstance i = new InMemoryInstance(instanceName);
        this.client = new InMemoryAccumuloClient("root", i);
        this.log = log;
        
        createTables();
    }
    
    public void dumpTables(Authorizations auths) {
        try {
            dumpTable(METADATA_TABLE_NAME, auths);
            dumpTable(TableName.DATE_INDEX, auths);
            dumpTable(TableName.LOAD_DATES, auths);
            dumpTable(TableName.SHARD, auths);
            dumpTable(TableName.SHARD_INDEX, auths);
            dumpTable(TableName.SHARD_RINDEX, auths);
            dumpTable(SHARD_DICT_INDEX_NAME, auths);
            dumpTable(MODEL_TABLE_NAME, auths);
        } catch (TableNotFoundException e) {
            // should not happen
            throw new IllegalArgumentException(e);
        }
    }
    
    public void dumpTable(String table, Authorizations auths) throws TableNotFoundException {
        TableOperations tops = client.tableOperations();
        Scanner scanner = client.createScanner(table, auths);
        Iterator<Map.Entry<Key,Value>> iterator = scanner.iterator();
        System.out.println("*************** " + table + " ********************");
        while (iterator.hasNext()) {
            Map.Entry<Key,Value> entry = iterator.next();
            System.out.println(entry);
        }
        scanner.close();
    }
    
    public void printTables(Authorizations auths) throws TableNotFoundException {
        PrintUtility.printTable(client, auths, QueryTestTableHelper.METADATA_TABLE_NAME);
        PrintUtility.printTable(client, auths, TableName.SHARD);
        PrintUtility.printTable(client, auths, TableName.SHARD_INDEX);
        PrintUtility.printTable(client, auths, TableName.SHARD_RINDEX);
    }
    
    protected void createTables() throws AccumuloSecurityException, AccumuloException, TableNotFoundException, TableExistsException {
        TableOperations tops = client.tableOperations();
        deleteAndCreateTable(tops, METADATA_TABLE_NAME);
        deleteAndCreateTable(tops, TableName.DATE_INDEX);
        deleteAndCreateTable(tops, TableName.LOAD_DATES);
        deleteAndCreateTable(tops, TableName.SHARD);
        deleteAndCreateTable(tops, TableName.SHARD_INDEX);
        deleteAndCreateTable(tops, TableName.SHARD_RINDEX);
        deleteAndCreateTable(tops, SHARD_DICT_INDEX_NAME);
        deleteAndCreateTable(tops, MODEL_TABLE_NAME);
    }
    
    protected void deleteAndCreateTable(TableOperations tops, String tableName) throws AccumuloException, AccumuloSecurityException, TableNotFoundException,
                    TableExistsException {
        if (tops.exists(tableName)) {
            tops.delete(tableName);
        }
        tops.create(tableName);
    }
    
    /**
     * Configures all of the default tables and associates a {@link BatchWriterConfig} object for ach table.
     * 
     * @param writer
     *            a mock writer
     * @throws AccumuloSecurityException
     *             for accumulo security exceptions
     * @throws AccumuloException
     *             for general accumulo exceptions
     * @throws TableNotFoundException
     *             if the table is not found
     */
    public void configureTables(MockAccumuloRecordWriter writer) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        configureAShardRelatedTable(writer, new MetadataTableConfigHelper(), ShardedDataTypeHandler.METADATA_TABLE_NAME, METADATA_TABLE_NAME);
        configureAShardRelatedTable(writer, new ShardTableConfigHelper(), ShardedDataTypeHandler.SHARD_TNAME, TableName.SHARD);
        configureAShardRelatedTable(writer, new ShardTableConfigHelper(), ShardedDataTypeHandler.SHARD_GIDX_TNAME, TableName.SHARD_INDEX);
        configureAShardRelatedTable(writer, new ShardTableConfigHelper(), ShardedDataTypeHandler.SHARD_GRIDX_TNAME, TableName.SHARD_RINDEX);
        configureAShardRelatedTable(writer, new ShardTableConfigHelper(), ShardedDataTypeHandler.SHARD_DINDX_NAME, SHARD_DICT_INDEX_NAME);
        
        // todo - configure the other tables...
    }
    
    protected void configureAShardRelatedTable(MockAccumuloRecordWriter writer, TableConfigHelper helper, String keyForTableName, String tableName)
                    throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        log.debug("---------------  configure table (" + keyForTableName + ")  ---------------");
        Configuration tableConfig = new Configuration();
        tableConfig.set(keyForTableName, tableName);
        helper.setup(tableName, tableConfig, log);
        helper.configure(client.tableOperations());
        writer.addWriter(new Text(tableName), client.createBatchWriter(tableName, bwCfg));
    }
    
    public void overrideUidAggregator() throws AccumuloSecurityException, AccumuloException {
        for (IteratorUtil.IteratorScope scope : IteratorUtil.IteratorScope.values()) {
            String stem = String.format("%s%s.%s", Property.TABLE_ITERATOR_PREFIX, scope.name(), "UIDAggregator");
            // Override the UidAggregator with a mock aggregator to lower the UID.List MAX uid limit.
            client.tableOperations().setProperty(TableName.SHARD_INDEX, stem + ".opt.*", "datawave.query.util.InMemoryGlobalIndexUidAggregator");
            client.tableOperations().setProperty(TableName.SHARD_RINDEX, stem + ".opt.*", "datawave.query.util.InMemoryGlobalIndexUidAggregator");
        }
    }
    
    public static void configureLogicToScanTables(ShardQueryLogic logic) {
        logic.setMetadataTableName(METADATA_TABLE_NAME);
        logic.setDateIndexTableName(TableName.DATE_INDEX);
        logic.setTableName(TableName.SHARD);
        logic.setIndexTableName(TableName.SHARD_INDEX);
        logic.setReverseIndexTableName(TableName.SHARD_RINDEX);
        logic.setModelTableName(MODEL_TABLE_NAME);
        logic.setMaxResults(5000);
        logic.setMaxWork(25000);
    }
}
