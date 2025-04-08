package datawave.microservice.querymetric.handler;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.core.common.connection.AccumuloClientPool;

public class AccumuloRecordWriter extends RecordWriter<Text,Mutation> {
    private MultiTableBatchWriter mtbw = null;
    private HashMap<Text,BatchWriter> bws;
    private Text defaultTableName;
    private Logger log = LoggerFactory.getLogger(AccumuloRecordWriter.class);
    
    private boolean simulate;
    private boolean createTables;
    
    private AccumuloClientPool accumuloClientPool;
    private AccumuloClient accumuloClient;
    private static final String PREFIX = AccumuloRecordWriter.class.getSimpleName();
    private static final String USERNAME = PREFIX + ".username";
    private static final String DEFAULT_TABLE_NAME = PREFIX + ".defaulttable";
    
    private static final String CREATETABLES = PREFIX + ".createtables";
    private static final String SIMULATE = PREFIX + ".simulate";
    
    // BatchWriter options
    private static final String MAX_MUTATION_BUFFER_SIZE = PREFIX + ".maxmemory";
    private static final String MAX_LATENCY = PREFIX + ".maxlatency";
    private static final String NUM_WRITE_THREADS = PREFIX + ".writethreads";
    
    private static final long DEFAULT_MAX_MUTATION_BUFFER_SIZE = 10000000; // ~10M
    private static final int DEFAULT_MAX_LATENCY = 120000; // 1 minute
    private static final int DEFAULT_NUM_WRITE_THREADS = 4;
    
    private AtomicBoolean healthy = new AtomicBoolean(true);
    
    public AccumuloRecordWriter(AccumuloClientPool accumuloClientPool, Configuration conf) throws Exception {
        this.simulate = getSimulationMode(conf);
        this.createTables = canCreateTables(conf);
        this.accumuloClientPool = accumuloClientPool;
        
        if (simulate) {
            log.info("Simulating output only. No writes to tables will occur");
        }
        
        this.bws = new HashMap<>();
        
        String tname = getDefaultTableName(conf);
        this.defaultTableName = (tname == null) ? null : new Text(tname);
        
        if (!simulate) {
            Map<String,String> trackingMap = AccumuloClientTracking.getTrackingMap(Thread.currentThread().getStackTrace());
            this.accumuloClient = accumuloClientPool.borrowObject(trackingMap);
            BatchWriterConfig bwConfig = new BatchWriterConfig();
            bwConfig.setMaxMemory(getMaxMutationBufferSize(conf));
            bwConfig.setMaxLatency(getMaxLatency(conf), TimeUnit.MILLISECONDS);
            bwConfig.setMaxWriteThreads(getMaxWriteThreads(conf));
            mtbw = this.accumuloClient.createMultiTableBatchWriter(bwConfig);
        }
    }
    
    public void setHealthy(boolean healthy) {
        this.healthy.set(healthy);
    }
    
    public boolean isHealthy() {
        return healthy.get();
    }
    
    /**
     * Push a mutation into a table. If table is null, the defaultTable will be used. If canCreateTable is set, the table will be created if it does not exist.
     * The table name must only contain alphanumerics and underscore.
     */
    @Override
    public void write(Text table, Mutation mutation) throws IOException {
        if (table == null || table.toString().isEmpty()) {
            table = this.defaultTableName;
        }
        
        if (!simulate && table == null) {
            throw new IOException("No table or default table specified. Try simulation mode next time");
        }
        
        printMutation(table, mutation);
        
        if (simulate) {
            return;
        }
        
        if (!bws.containsKey(table)) {
            try {
                addTable(table);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
                throw new IOException(e);
            }
        }
        
        try {
            bws.get(table).addMutation(mutation);
        } catch (MutationsRejectedException e) {
            log.error("Mutation rejected with constraint violations: " + e.getConstraintViolationSummaries() + " row: " + mutation.getRow() + " updates: "
                            + mutation.getUpdates());
            throw new IOException("MutationsRejectedException - ConstraintViolations: " + e.getConstraintViolationSummaries(), e);
        }
    }
    
    public void addTable(Text tableName) throws AccumuloException, AccumuloSecurityException {
        if (simulate) {
            log.info("Simulating adding table: " + tableName);
            return;
        }
        
        log.debug("Adding table: " + tableName);
        BatchWriter bw;
        String table = tableName.toString();
        
        if (createTables && !this.accumuloClient.tableOperations().exists(table)) {
            try {
                this.accumuloClient.tableOperations().create(table);
            } catch (AccumuloSecurityException e) {
                log.error("Accumulo security violation creating " + table, e);
                throw e;
            } catch (TableExistsException e) {
                // Shouldn't happen
            }
        }
        
        try {
            bw = mtbw.getBatchWriter(table);
        } catch (TableNotFoundException e) {
            log.error("Accumulo table " + table + " doesn't exist and cannot be created.", e);
            throw new AccumuloException(e);
        }
        
        if (bw != null) {
            bws.put(tableName, bw);
        }
    }
    
    private int printMutation(Text table, Mutation m) {
        if (log.isTraceEnabled()) {
            log.trace(String.format("Table %s row key: %s", table, hexDump(m.getRow())));
            for (ColumnUpdate cu : m.getUpdates()) {
                log.trace(String.format("Table %s column: %s:%s", table, hexDump(cu.getColumnFamily()), hexDump(cu.getColumnQualifier())));
                log.trace(String.format("Table %s security: %s", table, new ColumnVisibility(cu.getColumnVisibility()).toString()));
                log.trace(String.format("Table %s value: %s", table, hexDump(cu.getValue())));
            }
        }
        return m.getUpdates().size();
    }
    
    private String hexDump(byte[] ba) {
        StringBuilder sb = new StringBuilder();
        for (byte b : ba) {
            if ((b > 0x20) && (b < 0x7e)) {
                sb.append((char) b);
            } else {
                sb.append(String.format("x%02x", b));
            }
        }
        return sb.toString();
    }
    
    @Override
    public void close(TaskAttemptContext attempt) throws IOException, InterruptedException {
        if (simulate) {
            return;
        }
        
        try {
            mtbw.close();
        } catch (MutationsRejectedException e) {
            if (e.getSecurityErrorCodes().size() >= 0) {
                HashSet<String> tables = new HashSet<>();
                for (TabletId tabletId : e.getSecurityErrorCodes().keySet()) {
                    tables.add(tabletId.getTableId().toString());
                }
                
                log.error("Not authorized to write to tables : " + tables);
            }
            
            if (!e.getConstraintViolationSummaries().isEmpty()) {
                log.error("Constraint violations : " + e.getConstraintViolationSummaries());
            }
        } finally {
            returnClient();
        }
    }
    
    public void returnClient() {
        try {
            if (this.accumuloClient != null) {
                this.accumuloClientPool.returnObject(accumuloClient);
            }
            this.accumuloClient = null;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }
    
    public static void setMaxMutationBufferSize(Configuration conf, long numberOfBytes) {
        conf.setLong(MAX_MUTATION_BUFFER_SIZE, numberOfBytes);
    }
    
    public static void setMaxLatency(Configuration conf, int numberOfMilliseconds) {
        conf.setInt(MAX_LATENCY, numberOfMilliseconds);
    }
    
    public static void setMaxWriteThreads(Configuration conf, int numberOfThreads) {
        conf.setInt(NUM_WRITE_THREADS, numberOfThreads);
    }
    
    public static void setSimulationMode(Configuration conf) {
        conf.setBoolean(SIMULATE, true);
    }
    
    protected static String getUsername(Configuration conf) {
        return conf.get(USERNAME);
    }
    
    protected static boolean canCreateTables(Configuration conf) {
        return conf.getBoolean(CREATETABLES, false);
    }
    
    protected static String getDefaultTableName(Configuration conf) {
        return conf.get(DEFAULT_TABLE_NAME);
    }
    
    protected static long getMaxMutationBufferSize(Configuration conf) {
        return conf.getLong(MAX_MUTATION_BUFFER_SIZE, DEFAULT_MAX_MUTATION_BUFFER_SIZE);
    }
    
    protected static int getMaxLatency(Configuration conf) {
        return conf.getInt(MAX_LATENCY, DEFAULT_MAX_LATENCY);
    }
    
    protected static int getMaxWriteThreads(Configuration conf) {
        return conf.getInt(NUM_WRITE_THREADS, DEFAULT_NUM_WRITE_THREADS);
    }
    
    protected static boolean getSimulationMode(Configuration conf) {
        return conf.getBoolean(SIMULATE, false);
    }
    
    public void flush() throws Exception {
        this.mtbw.flush();
    }
}
