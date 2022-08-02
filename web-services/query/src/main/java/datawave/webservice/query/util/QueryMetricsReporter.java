package datawave.webservice.query.util;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.BaseQueryMetric.PageMetric;
import datawave.util.cli.PasswordConverter;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

public class QueryMetricsReporter {
    private Options options;
    private Option instanceOpt, zookeepersOpt, userOpt, passwordOpt, beginOpt, endOpt, tableOpt, useAllQueryPagesOpt, verboseSummariesOpt, queryUserOpt;
    
    private boolean useAllQueryPages = false, verboseSummaries = false;
    private String queryUser = null;
    
    private final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
    
    private Logger log = Logger.getLogger(QueryMetricsReporter.class);
    
    // A map of query duration
    private List<Long> sortedMetrics;
    
    public QueryMetricsReporter() {
        sortedMetrics = new ArrayList<>();
        options = getOptions();
    }
    
    private Options getOptions() {
        Options options = new Options();
        
        instanceOpt = new Option("i", "instance", true, "Accumulo instance name");
        instanceOpt.setArgName("name");
        instanceOpt.setRequired(true);
        options.addOption(instanceOpt);
        
        zookeepersOpt = new Option("zk", "zookeeper", true, "Comma-separated list of ZooKeeper servers");
        zookeepersOpt.setArgName("server[,server]");
        zookeepersOpt.setRequired(true);
        options.addOption(zookeepersOpt);
        
        userOpt = new Option("u", "username", true, "Accumulo username");
        userOpt.setArgName("name");
        userOpt.setRequired(true);
        options.addOption(userOpt);
        
        passwordOpt = new Option("p", "password", true, "Accumulo password");
        passwordOpt.setArgName("passwd");
        passwordOpt.setRequired(true);
        options.addOption(passwordOpt);
        
        beginOpt = new Option("b", "begin", true, "Begin date");
        beginOpt.setArgName("date");
        beginOpt.setRequired(true);
        options.addOption(beginOpt);
        
        endOpt = new Option("e", "end", true, "End date");
        endOpt.setArgName("date");
        endOpt.setRequired(true);
        options.addOption(endOpt);
        
        tableOpt = new Option("t", "table", true, "Specify a table output override for testing purposes.");
        tableOpt.setArgName("table");
        tableOpt.setRequired(false);
        options.addOption(tableOpt);
        
        useAllQueryPagesOpt = new Option("a", "allPages", false, "If present, query latency will use all pages in the query.");
        useAllQueryPagesOpt.setRequired(false);
        useAllQueryPagesOpt.setArgs(0);
        options.addOption(useAllQueryPagesOpt);
        
        verboseSummariesOpt = new Option("v", "verbose", false, "Print extra statistics on queries in the date range.");
        verboseSummariesOpt.setRequired(false);
        verboseSummariesOpt.setArgs(0);
        options.addOption(verboseSummariesOpt);
        
        queryUserOpt = new Option("qu", "queryUser", false, "Limit the metrics to a specific user");
        queryUserOpt.setRequired(false);
        queryUserOpt.setArgs(1);
        queryUserOpt.setArgName("queryUser");
        options.addOption(queryUserOpt);
        
        return options;
    }
    
    public int run(String[] args) {
        CommandLine cli;
        
        try {
            cli = new BasicParser().parse(options, args);
        } catch (ParseException e) {
            log.error("Could not parse command line arguments: " + e.getMessage(), e);
            log.error("Received command: " + Arrays.asList(args));
            
            return 1;
        }
        
        String instanceName = cli.getOptionValue(instanceOpt.getOpt());
        String zookeepers = cli.getOptionValue(zookeepersOpt.getOpt());
        String username = cli.getOptionValue(userOpt.getOpt());
        byte[] password = PasswordConverter.parseArg(cli.getOptionValue(passwordOpt.getOpt())).getBytes();
        String tableName = cli.getOptionValue(tableOpt.getOpt());
        
        String begin = cli.getOptionValue(beginOpt.getOpt());
        String end = cli.getOptionValue(endOpt.getOpt());
        
        useAllQueryPages = cli.hasOption(useAllQueryPagesOpt.getOpt());
        verboseSummaries = cli.hasOption(verboseSummariesOpt.getOpt());
        
        if (cli.hasOption(queryUserOpt.getOpt()))
            queryUser = cli.getOptionValue(queryUserOpt.getOpt());
        
        Date beginDate, endDate;
        try {
            beginDate = sdf.parse(begin);
            endDate = sdf.parse(end);
        } catch (java.text.ParseException e) {
            log.error("Could not parse begin/end date.", e);
            
            return 1;
        }
        
        // Ensure that a valid date range was provided
        if (beginDate.after(endDate)) {
            log.error("The begin date, " + beginDate + " must be before the end date, " + endDate);
            
            return 2;
        }
        
        // Connect to Accumulo
        ZooKeeperInstance zkInstance = new ZooKeeperInstance(ClientConfiguration.loadDefault().withInstance(instanceName).withZkHosts(zookeepers));
        Connector connector;
        try {
            connector = zkInstance.getConnector(username, new PasswordToken(password));
        } catch (AccumuloException e) {
            log.error("Could not obtain connector to Accumulo due to AccumuloException: " + e.getMessage(), e);
            
            return 1;
        } catch (AccumuloSecurityException e) {
            log.error("Could not obtain connector to Accumulo due to AccumuloSecurityException: " + e.getMessage(), e);
            
            return 1;
        }
        
        // Open up a BatchScanner to the QueryMetrics table
        try (BatchScanner bs = connector.createBatchScanner(tableName, Authorizations.EMPTY, 8)) {
            // Set a range for the entire table
            Range r = null;
            if (null == queryUser)
                r = new Range();
            else
                r = new Range(queryUser);
            
            bs.setRanges(Collections.singleton(r));
            IteratorSetting cfRegex = new IteratorSetting(20, RegExFilter.class);
            cfRegex.addOption(RegExFilter.COLF_REGEX, "RunningQuery.*");
            bs.addScanIterator(cfRegex);
            
            // Collect the data
            processResults(beginDate, endDate, bs.iterator());
            
            printResults(beginDate, endDate);
            
        } catch (TableNotFoundException e) {
            log.error("The requested table '" + tableName + "' does not exist!", e);
            
            return 2;
        }
        
        return 0;
    }
    
    /**
     * Given results from the QueryMetrics table, collect statistics on all queries falling in the date range.
     * 
     * @param iterator
     */
    private void processResults(Date beginDate, Date endDate, Iterator<Entry<Key,Value>> iterator) {
        final Text holder = new Text();
        while (iterator.hasNext()) {
            Entry<Key,Value> entry = iterator.next();
            
            entry.getKey().getColumnQualifier(holder);
            int pos = holder.find("\0");
            
            if (pos == -1) {
                log.warn("Could not parse key: " + entry.getKey());
                continue;
            }
            
            String queryCreateTime = null;
            try {
                queryCreateTime = Text.decode(holder.getBytes(), pos + 1, holder.getLength() - pos - 1);
            } catch (CharacterCodingException e) {
                log.warn("Could not decode bytes from key: " + entry.getKey());
            }
            
            Date queryTime = new Date(Long.parseLong(queryCreateTime));
            
            if (queryTime.after(beginDate) && queryTime.before(endDate)) {
                try {
                    addQuery(QueryMetricUtil.toMetric(entry.getValue()));
                } catch (ClassNotFoundException e) {
                    log.error("Could not load class when deserializing key: " + entry.getKey(), e);
                    continue;
                } catch (IOException e) {
                    log.error("Caught IOException when deserializing key: " + entry.getKey(), e);
                    continue;
                }
            }
        }
    }
    
    private void printResults(Date beginDate, Date endDate) {
        long medianLatency = this.sortedMetrics.isEmpty() ? 0 : this.sortedMetrics.get(this.sortedMetrics.size() / 2);
        
        System.out.println("Begin date: " + beginDate);
        System.out.println("End date: " + endDate);
        System.out.println("Number of queries run: " + this.sortedMetrics.size());
        System.out.println("Median query latency: " + medianLatency + " ms");
    }
    
    private void addQuery(BaseQueryMetric metric) {
        long currentTime = getQueryTime(metric);
        
        // If we're in verbose mode, print the entire query metric
        if (this.verboseSummaries) {
            System.out.println(metric);
        }
        
        int i = 0;
        boolean inserted = false;
        for (Long t : this.sortedMetrics) {
            if (currentTime < t) {
                this.sortedMetrics.add(i, currentTime);
                inserted = true;
                return;
            }
            
            i++;
        }
        
        if (!inserted) {
            this.sortedMetrics.add(currentTime);
        }
    }
    
    private long getQueryTime(BaseQueryMetric metric) {
        long time = metric.getSetupTime();
        
        List<PageMetric> pageTimes = metric.getPageTimes();
        
        if (useAllQueryPages) {
            for (PageMetric pageTime : pageTimes) {
                time += pageTime.getReturnTime();
            }
        } else {
            if (pageTimes.size() >= 1) {
                time += pageTimes.get(0).getReturnTime();
            }
        }
        
        return time;
    }
    
    public static void main(String[] args) {
        QueryMetricsReporter report = new QueryMetricsReporter();
        
        System.exit(report.run(args));
    }
}
