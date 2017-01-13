package nsa.datawave.ingest.poller.manager;

import nsa.datawave.ingest.data.RawRecordContainer;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.data.Mutation;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;
import java.util.Collection;

/**
 * A poller processor that inserts mutations into accumulo, and also generates ingest metrics (not implemented yet). This poller processor uses a batch writer
 * to send data to Accumulo. It is possible that all events in a file will be sent to the batch writer successfully, but then the failure will happen when the
 * {@link #finishFile()} method is invoked and the batch writer is flushed.
 * <p>
 * Given that Accumulo may be restarted with the pollers still running, this processor marks its batch writer as bad and attempts to recreate it any time there
 * is an error. The current file will fail, but hopefully future files are fine.
 */
public abstract class BaseAccumuloIngestProcessor implements PollerEventProcessor {
    private Collection<Option> requiredOpts;
    private Option instanceNameOpt;
    private Option zookeepersOpt;
    private Option usernameOpt;
    private Option passwordOpt;
    private Option tableNameOpt;
    private Option maxMemoryOpt;
    private Option maxLatencyOpt;
    private Option writeThreadsOpt;
    private Option retryCountOpt;
    
    private String instance;
    private String zookeepers;
    private String user;
    private String password;
    private String tableName;
    private long maxMemory;
    private long maxLatency;
    private int writeThreads;
    private BatchWriter batchWriter;
    private int retryCount;
    
    @Override
    public Collection<Option> getConfigurationOptions() {
        ArrayList<Option> o = new ArrayList<>();
        requiredOpts = new ArrayList<>();
        
        instanceNameOpt = new Option("inst", "instance", true, "Accumulo instance name");
        instanceNameOpt.setRequired(true);
        instanceNameOpt.setArgs(1);
        instanceNameOpt.setType(String.class);
        o.add(instanceNameOpt);
        requiredOpts.add(instanceNameOpt);
        
        zookeepersOpt = new Option("zoo", "zookeeperHosts", true, "Accumulo zookeeper hosts");
        zookeepersOpt.setRequired(true);
        zookeepersOpt.setArgs(1);
        zookeepersOpt.setType(String.class);
        o.add(zookeepersOpt);
        requiredOpts.add(zookeepersOpt);
        
        usernameOpt = new Option("user", "username", true, "Accumulo username");
        usernameOpt.setRequired(true);
        usernameOpt.setArgs(1);
        usernameOpt.setType(String.class);
        o.add(usernameOpt);
        requiredOpts.add(usernameOpt);
        
        passwordOpt = new Option("pass", "password", true, "Accumulo password");
        passwordOpt.setRequired(true);
        passwordOpt.setArgs(1);
        passwordOpt.setType(String.class);
        o.add(passwordOpt);
        requiredOpts.add(passwordOpt);
        
        tableNameOpt = new Option("table", "tableName", true, "table name to which data is written");
        tableNameOpt.setRequired(true);
        tableNameOpt.setArgs(1);
        tableNameOpt.setType(String.class);
        o.add(tableNameOpt);
        requiredOpts.add(tableNameOpt);
        
        maxMemoryOpt = new Option("mem", "maxMemory", true, "max memory in bytes for the Accumulo batch writer");
        maxMemoryOpt.setArgs(1);
        maxMemoryOpt.setType(Long.class);
        o.add(maxMemoryOpt);
        
        maxLatencyOpt = new Option("lat", "maxLatency", true, "max flush latency in ms for the Accumulo batch writer");
        maxLatencyOpt.setArgs(1);
        maxLatencyOpt.setType(Long.class);
        o.add(maxLatencyOpt);
        
        writeThreadsOpt = new Option("wt", "writeThreads", true, "max write threads for the Accumulo batch writer");
        writeThreadsOpt.setArgs(1);
        writeThreadsOpt.setType(Integer.class);
        o.add(writeThreadsOpt);
        
        retryCountOpt = new Option("retryCnt", "retryCount", true, "number of times to attempt to recreate the batch writer on a failed event before giving up");
        retryCountOpt.setArgs(1);
        retryCountOpt.setType(Integer.class);
        o.add(retryCountOpt);
        
        return o;
    }
    
    @Override
    public void configure(CommandLine cl, Configuration config) throws Exception {
        checkRequiredOpts(cl);
        instance = cl.getOptionValue(instanceNameOpt.getOpt());
        zookeepers = cl.getOptionValue(zookeepersOpt.getOpt());
        user = cl.getOptionValue(usernameOpt.getOpt());
        password = cl.getOptionValue(passwordOpt.getOpt());
        tableName = cl.getOptionValue(tableNameOpt.getOpt());
        
        maxMemory = Long.parseLong(cl.getOptionValue(maxMemoryOpt.getOpt(), "131072"));
        maxLatency = Long.parseLong(cl.getOptionValue(maxLatencyOpt.getOpt(), "3000"));
        writeThreads = Integer.parseInt(cl.getOptionValue(writeThreadsOpt.getOpt(), "1"));
        
        retryCount = Integer.parseInt(cl.getOptionValue(retryCountOpt.getOpt(), "3"));
        
        batchWriter = reopenBatchWriter();
    }
    
    @Override
    public void process(RawRecordContainer e) {
        Collection<Mutation> mutations = getEventMutations(e);
        if (mutations != null) {
            int retries = 0;
            boolean retry = true;
            while (retry) {
                try {
                    useBatchWriter().addMutations(mutations);
                    retry = false;
                } catch (MutationsRejectedException mre) {
                    batchWriter = null;
                    if (retries < retryCount)
                        ++retryCount;
                    else
                        throw new IllegalStateException(mre);
                }
            }
        }
    }
    
    @Override
    public void finishFile() {
        // TODO: write ingest metrics out. Right now we can't guarantee our accumulo connection will be to the metrics instance.
        try {
            useBatchWriter().flush();
        } catch (MutationsRejectedException mre) {
            batchWriter = null;
            throw new IllegalStateException(mre);
        }
    }
    
    @Override
    public void close() {
        try {
            useBatchWriter().close();
        } catch (MutationsRejectedException mre) {
            batchWriter = null;
            throw new IllegalStateException(mre);
        }
    }
    
    private void checkRequiredOpts(CommandLine cl) throws MissingOptionException {
        ArrayList<Option> missingOptions = new ArrayList<>();
        for (Option opt : requiredOpts) {
            if (!cl.hasOption(opt.getOpt()))
                missingOptions.add(opt);
        }
        if (!missingOptions.isEmpty())
            throw new MissingOptionException(missingOptions);
    }
    
    private BatchWriter useBatchWriter() {
        if (batchWriter == null) {
            try {
                batchWriter = reopenBatchWriter();
            } catch (AccumuloSecurityException | AccumuloException e) {
                throw new IllegalStateException(e);
            }
        }
        return batchWriter;
    }
    
    private BatchWriter reopenBatchWriter() throws AccumuloSecurityException, AccumuloException {
        ZooKeeperInstance zki = new ZooKeeperInstance(instance, zookeepers);
        Connector con = zki.getConnector(user, password);
        if (!con.tableOperations().exists(tableName)) {
            try {
                con.tableOperations().create(tableName);
            } catch (TableExistsException e) {
                // ignore -- someone beat us to it
            }
        }
        try {
            return con.createBatchWriter(tableName, maxMemory, maxLatency, writeThreads);
        } catch (TableNotFoundException e) {
            // Should never happen since we just created the table above, so rethrow as a runtime exception if the impossible happens
            throw new IllegalStateException(e);
        }
    }
    
    abstract protected Collection<Mutation> getEventMutations(RawRecordContainer e);
}
