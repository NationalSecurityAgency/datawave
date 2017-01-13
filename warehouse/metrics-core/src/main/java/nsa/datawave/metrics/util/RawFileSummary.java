package nsa.datawave.metrics.util;

import nsa.datawave.metrics.config.MetricsConfig;
import nsa.datawave.poller.metric.InputFile;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Map.Entry;

/**
 * 
 */
public class RawFileSummary implements Runnable {
    private static final Logger log = Logger.getLogger(RawFileSummary.class);
    
    public static final String INSTANCE = "instance", ZOOKEEPER = "zookeeper", USER = "user", PASSWORD = "password", TABLE = "table";
    
    public final Connector connector;
    public final String table;
    
    public RawFileSummary(String[] args) throws ParseException, java.text.ParseException, AccumuloException, AccumuloSecurityException {
        Options opts = new Options();
        
        opts.addOption(INSTANCE, true, "Accumulo instance name");
        opts.addOption(ZOOKEEPER, true, "ZooKeeper hosts");
        opts.addOption(USER, true, "Accumulo user");
        opts.addOption(PASSWORD, true, "Accumulo password");
        opts.addOption(TABLE, true, "Accumulo PollerMetrics table");
        
        GnuParser parser = new GnuParser();
        CommandLine cl = parser.parse(opts, args);
        
        String instance = cl.getOptionValue(INSTANCE), zookeeper = cl.getOptionValue(ZOOKEEPER), user = cl.getOptionValue(USER), password = cl
                        .getOptionValue(PASSWORD), tableStr = cl.getOptionValue(TABLE);
        
        if (null == tableStr) {
            table = MetricsConfig.DEFAULT_POLLER_TABLE;
        } else {
            table = tableStr;
        }
        
        if (null == instance || null == zookeeper || null == user || null == password) {
            final String msg = "usage: ${this_script} -instance=instance -zookeeper=hosts -user=username -password=passwd [-table=table]";
            System.err.println(msg);
            throw new ParseException(msg);
        }
        
        ZooKeeperInstance zk = new ZooKeeperInstance(ClientConfiguration.loadDefault().withInstance(instance).withZkHosts(zookeeper));
        connector = zk.getConnector(user, new PasswordToken(password));
    }
    
    @Override
    public void run() {
        BatchScanner bs;
        try {
            bs = connector.createBatchScanner(table, Authorizations.EMPTY, 4);
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }
        
        bs.setRanges(Collections.singleton(new Range()));
        
        System.out.println("RAW_FILE_NAME,TOTAL_TIME,PROCESSING_TIME");
        
        final StringBuilder sb = new StringBuilder();
        final String comma = ",";
        
        for (Entry<Key,Value> entry : bs) {
            Key k = entry.getKey();
            Value v = entry.getValue();
            
            Long totalRawFileDuration = WritableUtil.parseLong(k.getColumnFamily());
            
            Counters counters = new Counters();
            try {
                counters.readFields(new DataInputStream(new ByteArrayInputStream(v.get())));
            } catch (IOException e) {
                log.error("Could not deserialize counters for: " + k);
                continue;
            }
            
            String rawFileName = counters.getGroup("InputFile").iterator().next().getName();
            
            Long startTime = counters.findCounter(InputFile.POLLER_START_TIME).getValue();
            Long endTime = counters.findCounter(InputFile.POLLER_END_TIME).getValue();
            
            sb.append(rawFileName).append(comma).append(totalRawFileDuration).append(comma).append(endTime - startTime);
            
            System.out.println(sb);
            
            sb.setLength(0);
        }
    }
    
    public static void main(String[] args) throws ParseException, java.text.ParseException, AccumuloException, AccumuloSecurityException {
        RawFileSummary summary = new RawFileSummary(args);
        summary.run();
    }
}
