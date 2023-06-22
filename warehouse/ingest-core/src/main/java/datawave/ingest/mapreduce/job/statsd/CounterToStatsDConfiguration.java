package datawave.ingest.mapreduce.job.statsd;

import com.google.common.base.Joiner;
import datawave.util.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created on 4/25/16.
 */
public class CounterToStatsDConfiguration {
    private static Logger log = Logger.getLogger(CounterToStatsDConfiguration.class);

    private String host = null;
    private int port = 0;
    private Map<CounterName,StatsDAspect> aspects = new HashMap<>();
    private String queueName = null;
    private String jobName = null;

    public CounterToStatsDConfiguration(Configuration conf) {
        this.queueName = conf.get("mapreduce.job.queuename", "default");
        this.jobName = conf.get("mapreduce.job.name", "unknown");

        for (Map.Entry<String,String> entry : conf) {
            if (entry.getKey().startsWith("statsd.")) {
                if (entry.getKey().equals("statsd.host")) {
                    this.host = entry.getValue();
                } else if (entry.getKey().equals("statsd.port")) {
                    this.port = Integer.parseInt(entry.getValue());
                } else {
                    String[] parts = StringUtils.split(entry.getKey(), '.');
                    if (parts.length < 4) {
                        log.warn("Unable to determine aspect name from " + entry.getKey() + "; skipping entry");
                        continue;
                    }
                    StatsDOutputType outputType;
                    try {
                        outputType = StatsDOutputType.valueOf(parts[1].toUpperCase());
                    } catch (Exception e) {
                        log.warn("Unable to determine StatsDOutputType from " + entry.getKey() + "; skipping entry");
                        continue;
                    }
                    StatsDType statsDType;
                    try {
                        statsDType = StatsDType.valueOf(parts[2].toUpperCase());
                    } catch (Exception e) {
                        log.warn("Unable to determine StatsDType from " + entry.getKey() + "; skipping entry");
                        continue;
                    }

                    String aspectContext = parts[3];
                    String aspectMetricName = (parts.length >= 5 ? Joiner.on('_').join(Arrays.asList(parts).subList(4, parts.length)) : null);

                    parts = StringUtils.split(entry.getValue(), '/');
                    if (parts.length > 2) {
                        log.warn("Unable to determine counter name from " + entry.getValue() + "; skipping entry");
                        continue;
                    }

                    String group = parts[0];
                    String counter = (parts.length == 2 ? parts[1] : null);

                    // if we have an aspect metric name, then we must have a counter name
                    if (aspectMetricName != null && counter == null) {
                        log.warn("A metrics name was specified without a counter name : " + entry.getKey() + "; skipping entry");
                        continue;
                    }

                    CounterName counterName = new CounterName(outputType, group, counter);
                    StatsDAspect statsDAspect = new StatsDAspect(statsDType, aspectContext, aspectMetricName);
                    log.info("Adding STATSD configuration for counter " + counterName + " --> " + statsDAspect);
                    this.aspects.put(counterName, statsDAspect);
                }
            }
        }
    }

    public boolean isConfigured() {
        return (host != null && port > 0);
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getQueueName() {
        return queueName;
    }

    public String getJobName() {
        return jobName;
    }

    public CounterStatsDClient getClient() {
        if (isConfigured()) {
            return new CounterStatsDClient(this);
        } else {
            return null;
        }
    }

    public StatsDAspect getAspect(StatsDOutputType type, CounterGroup group, Counter counter) {
        return getAspect(type, group.getName(), (counter == null ? null : counter.getName()));
    }

    public StatsDAspect getAspect(StatsDOutputType type, String group, Counter counter) {
        return getAspect(type, group, (counter == null ? null : counter.getName()));
    }

    public StatsDAspect getAspect(StatsDOutputType type, String group, String counter) {
        StatsDAspect aspect = aspects.get(new CounterName(type, group, counter));
        if (aspect == null) {
            aspect = aspects.get(new CounterName(type, group, null));
        }
        return aspect;
    }

    @Override
    public String toString() {
        return getHost() + ":" + getPort() + "//" + getQueueName();
    }

    public enum StatsDOutputType {
        LIVE, FINAL
    }

    public static class CounterName {
        private StatsDOutputType type;
        private String group;
        private String counter;

        public CounterName(StatsDOutputType type, String group, String counter) {
            this.type = type;
            this.group = group;
            this.counter = counter;
        }

        public StatsDOutputType getType() {
            return type;
        }

        public String getGroup() {
            return group;
        }

        public String getCounter() {
            return counter;
        }

        @Override
        public int hashCode() {
            return type.hashCode() + group.hashCode() + (counter == null ? 0 : counter.hashCode());
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof CounterName) {
                CounterName other = (CounterName) obj;
                return type.equals(other.type) && group.equals(other.group) && (counter == null ? counter == other.counter : counter.equals(other.counter));
            }
            return false;
        }

        @Override
        public String toString() {
            return type + ":" + group + "." + counter;
        }
    }

    public enum StatsDType {
        GAUGE, COUNTER, TIME
    }

    public static class StatsDAspect {
        private StatsDType type;
        private String context;
        private String name;

        public StatsDAspect(StatsDType type, String context, String name) {
            this.type = type;
            this.context = context;
            this.name = name;
        }

        public StatsDType getType() {
            return type;
        }

        public String getContext() {
            return context;
        }

        public String getName() {
            return name;
        }

        public String getFullName(String counterName) {
            return context + '_' + (name == null ? counterName.replace(' ', '_').replace('.', '_') : name);
        }

        @Override
        public int hashCode() {
            return type.hashCode() + context.hashCode() + (name == null ? 0 : name.hashCode());
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof StatsDAspect) {
                StatsDAspect other = (StatsDAspect) obj;
                return type.equals(other.type) && context.equals(other.context) && (name == null ? name == other.name : name.equals(other.name));
            }
            return false;
        }

        @Override
        public String toString() {
            return type + ":" + context + "." + name;
        }
    }

}
