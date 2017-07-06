package datawave.query;

import datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler;
import datawave.ingest.table.config.ShardTableConfigHelper;
import datawave.query.tables.ShardQueryLogic;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import datawave.accumulo.inmemory.InMemoryInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
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
public final class QueryTestTableHelper {
    
    public static final String METADATA_TABLE_NAME = "metadata";
    public static final String DATE_INDEX_TABLE_NAME = "dateIndex";
    public static final String LOAD_DATES_METADATA_TABLE_NAME = "LoadDates";
    public static final String SHARD_TABLE_NAME = "shard";
    public static final String SHARD_INDEX_TABLE_NAME = "shardIndex";
    public static final String SHARD_RINDEX_TABLE_NAME = "shardReverseIndex";
    public static final String SHARD_DICT_INDEX_NAME = "shardTermDictionary";
    public static final String MODEL_TABLE_NAME = "DatawaveMetadata";
    public final Connector connector;
    private final Logger log; // passed in for context when debugging
    
    public QueryTestTableHelper(Connector connector, Logger log) throws AccumuloSecurityException, AccumuloException, TableExistsException,
                    TableNotFoundException {
        // create mock instance and connector
        this.connector = connector;
        this.log = log;
        createTables();
    }
    
    public QueryTestTableHelper(String instanceName, Logger log) throws AccumuloSecurityException, AccumuloException, TableExistsException,
                    TableNotFoundException {
        // create mock instance and connector
        InMemoryInstance i = new InMemoryInstance(instanceName);
        connector = i.getConnector("root", new PasswordToken(""));
        this.log = log;
        
        createTables();
    }
    
    public void dumpTables(Authorizations auths) {
        try {
            dumpTable(METADATA_TABLE_NAME, auths);
            dumpTable(DATE_INDEX_TABLE_NAME, auths);
            dumpTable(LOAD_DATES_METADATA_TABLE_NAME, auths);
            dumpTable(SHARD_TABLE_NAME, auths);
            dumpTable(SHARD_INDEX_TABLE_NAME, auths);
            dumpTable(SHARD_RINDEX_TABLE_NAME, auths);
            dumpTable(SHARD_DICT_INDEX_NAME, auths);
            dumpTable(MODEL_TABLE_NAME, auths);
        } catch (TableNotFoundException e) {
            // should not happen
            throw new IllegalArgumentException(e);
        }
    }
    
    public void dumpTable(String table, Authorizations auths) throws TableNotFoundException {
        TableOperations tops = connector.tableOperations();
        Scanner scanner = connector.createScanner(table, auths);
        Iterator<Map.Entry<Key,Value>> iterator = scanner.iterator();
        System.out.println("*************** " + table + " ********************");
        while (iterator.hasNext()) {
            Map.Entry<Key,Value> entry = iterator.next();
            System.out.println(entry);
        }
        scanner.close();
    }
    
    private void createTables() throws AccumuloSecurityException, AccumuloException, TableNotFoundException, TableExistsException {
        TableOperations tops = connector.tableOperations();
        deleteAndCreateTable(tops, METADATA_TABLE_NAME);
        deleteAndCreateTable(tops, DATE_INDEX_TABLE_NAME);
        deleteAndCreateTable(tops, LOAD_DATES_METADATA_TABLE_NAME);
        deleteAndCreateTable(tops, SHARD_TABLE_NAME);
        deleteAndCreateTable(tops, SHARD_INDEX_TABLE_NAME);
        deleteAndCreateTable(tops, SHARD_RINDEX_TABLE_NAME);
        deleteAndCreateTable(tops, SHARD_DICT_INDEX_NAME);
        deleteAndCreateTable(tops, MODEL_TABLE_NAME);
    }
    
    private void deleteAndCreateTable(TableOperations tops, String tableName) throws AccumuloException, AccumuloSecurityException, TableNotFoundException,
                    TableExistsException {
        if (tops.exists(tableName)) {
            tops.delete(tableName);
        }
        tops.create(tableName);
    }
    
    public void setupWriters(MockAccumuloRecordWriter recordWriter) throws TableNotFoundException {
        BatchWriterConfig bwCfg = new BatchWriterConfig().setMaxLatency(1, TimeUnit.SECONDS).setMaxMemory(1000L).setMaxWriteThreads(1);
        recordWriter.addWriter(new Text(METADATA_TABLE_NAME), connector.createBatchWriter(METADATA_TABLE_NAME, bwCfg));
        recordWriter.addWriter(new Text(DATE_INDEX_TABLE_NAME), connector.createBatchWriter(DATE_INDEX_TABLE_NAME, bwCfg));
        recordWriter.addWriter(new Text(LOAD_DATES_METADATA_TABLE_NAME), connector.createBatchWriter(LOAD_DATES_METADATA_TABLE_NAME, bwCfg));
        recordWriter.addWriter(new Text(SHARD_TABLE_NAME), connector.createBatchWriter(SHARD_TABLE_NAME, bwCfg));
        recordWriter.addWriter(new Text(SHARD_INDEX_TABLE_NAME), connector.createBatchWriter(SHARD_INDEX_TABLE_NAME, bwCfg));
        recordWriter.addWriter(new Text(SHARD_RINDEX_TABLE_NAME), connector.createBatchWriter(SHARD_RINDEX_TABLE_NAME, bwCfg));
        recordWriter.addWriter(new Text(SHARD_DICT_INDEX_NAME), connector.createBatchWriter(SHARD_DICT_INDEX_NAME, bwCfg));
        recordWriter.addWriter(new Text(MODEL_TABLE_NAME), connector.createBatchWriter(MODEL_TABLE_NAME, bwCfg));
    }
    
    public void configureTables() throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        configureShardIndex();
        configureShardReverseIndex();
        // todo - configure the other tables...
    }
    
    private void configureShardReverseIndex() throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        configureAShardRelatedTable(ShardedDataTypeHandler.SHARD_GRIDX_TNAME, SHARD_RINDEX_TABLE_NAME);
    }
    
    private void configureShardIndex() throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        configureAShardRelatedTable(ShardedDataTypeHandler.SHARD_GIDX_TNAME, SHARD_INDEX_TABLE_NAME);
    }
    
    private void configureAShardRelatedTable(String keyForTableName, String tableName) throws AccumuloException, AccumuloSecurityException,
                    TableNotFoundException {
        ShardTableConfigHelper tableConfigHelper = new ShardTableConfigHelper();
        Configuration tableConfig = new Configuration();
        tableConfig.set(keyForTableName, tableName);
        tableConfigHelper.setup(tableName, tableConfig, log);
        tableConfigHelper.configure(connector.tableOperations());
    }
    
    public void overrideUidAggregator() throws AccumuloSecurityException, AccumuloException {
        for (IteratorUtil.IteratorScope scope : IteratorUtil.IteratorScope.values()) {
            String stem = String.format("%s%s.%s", Property.TABLE_ITERATOR_PREFIX, scope.name(), "UIDAggregator");
            // Override the UidAggregator with a mock aggregator to lower the UID.List MAX uid limit.
            connector.tableOperations().setProperty(SHARD_INDEX_TABLE_NAME, stem + ".opt.*", "datawave.query.util.InMemoryGlobalIndexUidAggregator");
            connector.tableOperations().setProperty(SHARD_RINDEX_TABLE_NAME, stem + ".opt.*", "datawave.query.util.InMemoryGlobalIndexUidAggregator");
        }
    }
    
    public static void configureLogicToScanTables(ShardQueryLogic logic) {
        logic.setMetadataTableName(METADATA_TABLE_NAME);
        logic.setDateIndexTableName(DATE_INDEX_TABLE_NAME);
        logic.setTableName(SHARD_TABLE_NAME);
        logic.setIndexTableName(SHARD_INDEX_TABLE_NAME);
        logic.setReverseIndexTableName(SHARD_RINDEX_TABLE_NAME);
        logic.setModelTableName(MODEL_TABLE_NAME);
        logic.setMaxResults(5000);
        logic.setMaxRowsToScan(25000);
    }
}
