package datawave.ingest.mapreduce.job.statsd;

import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.security.Credentials;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;

/**
 * Created on 4/25/16.
 */
public class CounterStatsDClient {
    private static Logger log = Logger.getLogger(CounterStatsDClient.class);

    protected StatsDClient client = null;
    protected CounterToStatsDConfiguration config;

    public CounterStatsDClient(CounterToStatsDConfiguration config) {
        this.config = config;
        client = createClient(config.getQueueName() + ".dwingest", config.getHost(), config.getPort());
    }

    protected StatsDClient createClient(String prefix, String host, int port) {
        StatsDClient client = new NonBlockingStatsDClient(prefix, host, port);
        log.info("Created STATSD client with host = " + host + "; port = " + port + "; prefix = " + prefix);
        return client;
    }

    public void sendFinalStats(Counters counters) {
        if (client != null) {
            for (CounterGroup group : counters) {
                for (Counter counter : group) {
                    if (log.isTraceEnabled()) {
                        log.trace("Looking for aspect matching " + group.getName() + " / " + counter.getName());
                    }
                    CounterToStatsDConfiguration.StatsDAspect aspect = config.getAspect(CounterToStatsDConfiguration.StatsDOutputType.FINAL, group, counter);
                    if (aspect != null) {
                        String fullName = aspect.getFullName(counter.getName());
                        if (log.isTraceEnabled()) {
                            log.trace("Sending " + aspect.getType() + '(' + fullName + " -> " + counter.getValue() + ')');
                        }
                        switch (aspect.getType()) {
                            case GAUGE:
                                client.gauge(fullName, counter.getValue());
                                break;
                            case COUNTER:
                                client.count(fullName, counter.getValue());
                                break;
                            default:
                                client.time(fullName, counter.getValue());
                        }
                    }
                }
            }
        }
    }

    public void sendLiveStat(String groupName, Counter counter, long value) {
        if (client != null) {
            if (log.isTraceEnabled()) {
                log.trace("Looking for aspect matching " + groupName + " / " + counter.getName());
            }
            CounterToStatsDConfiguration.StatsDAspect aspect = config.getAspect(CounterToStatsDConfiguration.StatsDOutputType.LIVE, groupName, counter);
            if (aspect != null) {
                log.trace("Found aspect " + aspect);
                String fullName = aspect.getFullName(counter.getName());
                log.trace("Sending " + aspect.getType() + '(' + fullName + " -> " + counter.getValue() + ')');
                switch (aspect.getType()) {
                    case GAUGE:
                        client.gauge(fullName, value);
                        break;
                    case COUNTER:
                        client.count(fullName, value);
                        break;
                    default:
                        client.time(fullName, value);
                }
            }
        }
    }

    public TaskAttemptContext getTaskAttemptContext(TaskAttemptContext context) {
        return new StatsDTaskAttemptContext(context);
    }

    public void close() {
        if (client != null) {
            synchronized (this) {
                if (client != null) {
                    log.info("Closing the STATSD client");
                    client.stop();
                    client = null;
                }
            }
        }
    }

    private class StatsDTaskAttemptContext implements TaskAttemptContext {
        private TaskAttemptContext delegate;

        public StatsDTaskAttemptContext(TaskAttemptContext context) {
            this.delegate = context;
        }

        @Override
        public TaskAttemptID getTaskAttemptID() {
            return delegate.getTaskAttemptID();
        }

        @Override
        public void setStatus(String msg) {
            delegate.setStatus(msg);
        }

        @Override
        public String getStatus() {
            return delegate.getStatus();
        }

        @Override
        public float getProgress() {
            return delegate.getProgress();
        }

        @Override
        public Counter getCounter(Enum<?> counterName) {
            return new StatsDCounter(counterName.getClass().getName(), delegate.getCounter(counterName));
        }

        @Override
        public Counter getCounter(String groupName, String counterName) {
            return new StatsDCounter(groupName, delegate.getCounter(groupName, counterName));
        }

        @Override
        public Configuration getConfiguration() {
            return delegate.getConfiguration();
        }

        @Override
        public Credentials getCredentials() {
            return delegate.getCredentials();
        }

        @Override
        public JobID getJobID() {
            return delegate.getJobID();
        }

        @Override
        public int getNumReduceTasks() {
            return delegate.getNumReduceTasks();
        }

        @Override
        public Path getWorkingDirectory() throws IOException {
            return delegate.getWorkingDirectory();
        }

        @Override
        public Class<?> getOutputKeyClass() {
            return delegate.getOutputKeyClass();
        }

        @Override
        public Class<?> getOutputValueClass() {
            return delegate.getOutputValueClass();
        }

        @Override
        public Class<?> getMapOutputKeyClass() {
            return delegate.getMapOutputKeyClass();
        }

        @Override
        public Class<?> getMapOutputValueClass() {
            return delegate.getMapOutputValueClass();
        }

        @Override
        public String getJobName() {
            return delegate.getJobName();
        }

        @Override
        public Class<? extends InputFormat<?,?>> getInputFormatClass() throws ClassNotFoundException {
            return delegate.getInputFormatClass();
        }

        @Override
        public Class<? extends Mapper<?,?,?,?>> getMapperClass() throws ClassNotFoundException {
            return delegate.getMapperClass();
        }

        @Override
        public Class<? extends Reducer<?,?,?,?>> getCombinerClass() throws ClassNotFoundException {
            return delegate.getCombinerClass();
        }

        @Override
        public Class<? extends Reducer<?,?,?,?>> getReducerClass() throws ClassNotFoundException {
            return delegate.getReducerClass();
        }

        @Override
        public Class<? extends OutputFormat<?,?>> getOutputFormatClass() throws ClassNotFoundException {
            return delegate.getOutputFormatClass();
        }

        @Override
        public Class<? extends Partitioner<?,?>> getPartitionerClass() throws ClassNotFoundException {
            return delegate.getPartitionerClass();
        }

        @Override
        public RawComparator<?> getSortComparator() {
            return delegate.getSortComparator();
        }

        @Override
        public String getJar() {
            return delegate.getJar();
        }

        @Override
        public RawComparator<?> getCombinerKeyGroupingComparator() {
            return delegate.getCombinerKeyGroupingComparator();
        }

        @Override
        public RawComparator<?> getGroupingComparator() {
            return delegate.getGroupingComparator();
        }

        @Override
        public boolean getJobSetupCleanupNeeded() {
            return delegate.getJobSetupCleanupNeeded();
        }

        @Override
        public boolean getTaskCleanupNeeded() {
            return delegate.getTaskCleanupNeeded();
        }

        @Override
        public boolean getProfileEnabled() {
            return delegate.getProfileEnabled();
        }

        @Override
        public String getProfileParams() {
            return delegate.getProfileParams();
        }

        @Override
        public Configuration.IntegerRanges getProfileTaskRange(boolean isMap) {
            return delegate.getProfileTaskRange(isMap);
        }

        @Override
        public String getUser() {
            return delegate.getUser();
        }

        @Override
        @Deprecated
        public boolean getSymlink() {
            return delegate.getSymlink();
        }

        @Override
        public Path[] getArchiveClassPaths() {
            return delegate.getArchiveClassPaths();
        }

        @Override
        public URI[] getCacheArchives() throws IOException {
            return delegate.getCacheArchives();
        }

        @Override
        public URI[] getCacheFiles() throws IOException {
            return delegate.getCacheFiles();
        }

        @Override
        @Deprecated
        public Path[] getLocalCacheArchives() throws IOException {
            return delegate.getLocalCacheArchives();
        }

        @Override
        @Deprecated
        public Path[] getLocalCacheFiles() throws IOException {
            return delegate.getLocalCacheFiles();
        }

        @Override
        public Path[] getFileClassPaths() {
            return delegate.getFileClassPaths();
        }

        @Override
        public String[] getArchiveTimestamps() {
            return delegate.getArchiveTimestamps();
        }

        @Override
        public String[] getFileTimestamps() {
            return delegate.getFileTimestamps();
        }

        @Override
        public int getMaxMapAttempts() {
            return delegate.getMaxMapAttempts();
        }

        @Override
        public int getMaxReduceAttempts() {
            return delegate.getMaxReduceAttempts();
        }

        @Override
        public void progress() {
            delegate.progress();
        }
    }

    private class StatsDCounter implements Counter {
        private String groupName;
        private Counter delegate;

        public StatsDCounter(String groupName, Counter counter) {
            this.groupName = groupName;
            this.delegate = counter;
        }

        @Override
        @Deprecated
        public void setDisplayName(String displayName) {
            delegate.setDisplayName(displayName);
        }

        @Override
        public String getName() {
            return delegate.getName();
        }

        @Override
        public String getDisplayName() {
            return delegate.getDisplayName();
        }

        @Override
        public long getValue() {
            return delegate.getValue();
        }

        @Override
        public void setValue(long value) {
            delegate.setValue(value);
            sendLiveStat(groupName, delegate, value);
        }

        @Override
        public void increment(long incr) {
            delegate.increment(incr);
            sendLiveStat(groupName, delegate, incr);
        }

        @Override
        @InterfaceAudience.Private
        public Counter getUnderlyingCounter() {
            return delegate;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            delegate.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            delegate.readFields(in);
        }
    }
}
