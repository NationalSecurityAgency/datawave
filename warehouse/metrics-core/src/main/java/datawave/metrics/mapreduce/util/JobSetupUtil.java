package datawave.metrics.mapreduce.util;

import datawave.metrics.analytic.DateConverter;
import datawave.metrics.config.MetricsConfig;
import datawave.metrics.config.MetricsOptions;
import datawave.util.time.DateHelper;
import org.apache.accumulo.core.data.Range;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

public class JobSetupUtil {
    /*
     * Sets up the Configuration object
     */
    public static Configuration configure(String[] args, Configuration conf, Logger log) throws ParseException {
        log.info("Searching for metrics.xml on classpath.");
        URL cpConfig = Thread.currentThread().getContextClassLoader().getResource("metrics.xml");
        if (cpConfig == null) {
            log.error("No configuration file specified nor runtime args supplied- exiting.");
            System.exit(1);
        } else {
            log.info("Using conf file located at " + cpConfig);
            conf.addResource(cpConfig);
        }

        MetricsOptions mOpts = new MetricsOptions();
        CommandLine cl = new GnuParser().parse(mOpts, args);
        // add the file config options first
        String confFiles = cl.getOptionValue("conf", "");
        if (confFiles != null && !confFiles.isEmpty()) {
            for (String confFile : confFiles.split(",")) {

                if (!confFile.isEmpty()) {
                    log.trace("Adding " + confFile + " to configurations resource base.");
                    conf.addResource(confFile);
                }
            }
        }

        // now get the runtime overrides
        for (Option opt : cl.getOptions()) {
            // Ensure we don't try to set a null value (option-only) because this will
            // cause an NPE out of Configuration/Hashtable
            conf.set(MetricsConfig.MTX + opt.getOpt(), null == opt.getValue() ? "" : opt.getValue());
        }
        return conf;
    }

    /**
     * Changes the priority of a MapReduce job to VERY_HIGH. There doesn't seem to be a good way using the Hadoop API to do this, so we wait until there is map
     * progress reported (indicating the job is actually running) and then update its priority by executing a system command.
     *
     * @param job
     *            The {@link Job} whose priority is to be increased
     * @param log
     *            the logger
     * @throws IOException
     *             if there is a read/write issue
     */
    public static void changeJobPriority(Job job, Logger log) throws IOException {
        // Spin until we get some map progress, so that we can be sure the job is
        // registered with hadoop when we go to change its priority through the
        // command-line below.
        while (job.mapProgress() == 0) {
            // block
        }

        try {
            StringBuilder cmd = new StringBuilder();
            String hadoopHome = System.getProperty("HADOOP_HOME");
            if (hadoopHome == null) {
                log.debug("$HADOOP_HOME is not set; hopefully `hadoop` is on the classpath.");
            } else {
                cmd.append(hadoopHome).append('/');
            }
            cmd.append("hadoop job -set-priority ").append(job.getJobID()).append(" VERY_HIGH");

            log.info("Executing: " + cmd);
            Process pr = Runtime.getRuntime().exec(cmd.toString());

            if (log.isInfoEnabled()) {
                BufferedReader in = new BufferedReader(new InputStreamReader(pr.getInputStream()));
                while (in.ready()) {
                    log.info(in.readLine());
                }
            }

            int retCode = pr.waitFor();
            if (retCode == 0) {
                log.info("Successfully upgraded job priority.");
            } else {
                log.error("Hadoop process exited abnormally-- job may take a long time if system is saturated.");
            }
        } catch (Exception e) {
            log.error("This job may take a while on a system running at full ingest load.", e);
        }
    }

    /**
     * Prints out the configuration in sorted order.
     *
     * @param conf
     *            a configuration
     * @param log
     *            the logger
     */
    public static void printConfig(Configuration conf, Logger log) {
        if (log.isTraceEnabled()) {
            SortedSet<String> sortedKeys = new TreeSet<>();
            for (Map.Entry<String,String> e : conf) {
                sortedKeys.add(e.getKey());
            }
            for (String k : sortedKeys) {
                log.trace(k + "=" + conf.get(k));
            }
        }
    }

    /**
     * Computes a range to scan over entries whose row is a timestamp in milliseconds since the epoch. This uses the supplied start and end date parameters that
     * can be supplied at job start time, and if none are supplied, it uses the current day. The end of the range is always set exclusively to the start of the
     * day following the end of the supplied day (or the beginning of tomorrow if no end was supplied).
     *
     * @param conf
     *            the configuration
     * @param log
     *            the logger
     * @return a range
     */
    public static Range computeTimeRange(Configuration conf, Logger log) {
        String start = conf.get(MetricsConfig.START);
        String end = conf.get(MetricsConfig.END);

        long from, until;

        if (start != null) {
            from = DateConverter.convert(start).getTime();
        } else {
            GregorianCalendar gc = new GregorianCalendar();
            gc.set(Calendar.HOUR_OF_DAY, 0);
            gc.set(Calendar.MINUTE, 0);
            gc.set(Calendar.SECOND, 0);
            gc.set(Calendar.MILLISECOND, 0);
            from = gc.getTimeInMillis();
            if (log.isDebugEnabled())
                log.debug("Defaulting start to the beginning of today: " + new Date(from));
        }

        if (end != null) {
            until = DateConverter.convert(end).getTime() + TimeUnit.DAYS.toMillis(1);
        } else {
            GregorianCalendar gc = new GregorianCalendar();
            gc.set(Calendar.HOUR_OF_DAY, 0);
            gc.set(Calendar.MINUTE, 0);
            gc.set(Calendar.SECOND, 0);
            gc.set(Calendar.MILLISECOND, 0);
            gc.add(Calendar.DAY_OF_YEAR, 1);
            until = gc.getTimeInMillis();
            if (log.isDebugEnabled())
                log.debug("Defaulting end to the beginning of tomorrow: " + new Date(until));
        }

        if (until <= from) {
            log.error("Warning: end date (" + new Date(until) + ") is after begin date (" + new Date(from) + "), swapping!");
            long tmp = until;
            until = from;
            from = tmp;
        }

        return new Range(Long.toString(from), true, Long.toString(until), false);
    }

    /**
     * Computes a set of ranges to scan over entries whose row is a shaded day (yyyyMMdd_shardNum), one range per day. We calculate a range per day so that we
     * can assume that all entries for a particular day go to the same mapper in a MapReduce job.
     *
     * This uses the supplied start and end date parameters that can be supplied at job start time, and if none are supplied, it uses the current day. The end
     * of the range is always set exclusively to the start of the day following the end of the supplied day (or the beginning of tomorrow if no end was
     * supplied).
     *
     * @param conf
     *            a configuration
     * @param log
     *            the logger
     * @return a set of ranges
     */
    public static Collection<Range> computeShardedDayRange(Configuration conf, Logger log) {
        String start = conf.get(MetricsConfig.START);
        String end = conf.get(MetricsConfig.END);

        GregorianCalendar from = new GregorianCalendar();
        if (start != null)
            from.setTime(DateConverter.convert(start));
        from.set(Calendar.HOUR_OF_DAY, 0);
        from.set(Calendar.MINUTE, 0);
        from.set(Calendar.SECOND, 0);
        from.set(Calendar.MILLISECOND, 0);
        if (log.isDebugEnabled() && start == null)
            log.debug("Defaulting start to the beginning of today: " + from);

        GregorianCalendar until = new GregorianCalendar();
        if (end != null)
            until.setTimeInMillis(DateConverter.convert(end).getTime() + TimeUnit.DAYS.toMillis(1));
        until.set(Calendar.HOUR_OF_DAY, 0);
        until.set(Calendar.MINUTE, 0);
        until.set(Calendar.SECOND, 0);
        until.set(Calendar.MILLISECOND, 0);
        until.add(Calendar.DAY_OF_YEAR, 1);
        if (log.isDebugEnabled() && end == null)
            log.debug("Defaulting end to the beginning of tomorrow: " + until);

        if (until.compareTo(from) <= 0) {
            log.error("Warning: end date (" + until + ") is after begin date (" + from + "), swapping!");
            GregorianCalendar tmp = until;
            until = from;
            from = tmp;
        }

        ArrayList<Range> ranges = new ArrayList<>();
        while (from.compareTo(until) < 0) {
            String rangeStart = DateHelper.format(from.getTime());
            from.add(GregorianCalendar.DAY_OF_YEAR, 1);
            String rangeEnd = DateHelper.format(from.getTime());
            ranges.add(new Range(rangeStart, true, rangeEnd, false));
        }

        return ranges;
    }

    /**
     * Formats a range from timestamp in milliseconds to /YYYY/MM/DD
     *
     * @param dayRange
     *            a range of dates
     * @param log
     *            the logger
     * @return a range from the set timestamp
     */
    public static Range formatReverseSlashedTimeRange(Range dayRange, Logger log) {

        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd");
        long start = Long.parseLong(dayRange.getStartKey().getRow().toString());
        long end = Long.parseLong(dayRange.getEndKey().getRow().toString());

        String from = "/" + dateFormat.format(new Date(start));
        String until = "/" + dateFormat.format(new Date(end));

        return new Range(from, true, until, false);
    }

    public static Range formatReverseTimeRange(Range dayRange, Logger log) {
        long start = Long.parseLong(dayRange.getStartKey().getRow().toString());
        long end = Long.parseLong(dayRange.getEndKey().getRow().toString());

        String from = DateHelper.format(new Date(start));
        String until = DateHelper.format(new Date(end));

        return new Range(from, true, until, false);
    }

    /**
     * @param dayRange
     *            a date range
     * @param log
     *            the logger
     * @return a formatted range
     */
    public static Range formatEpochHourTimeRange(Range dayRange, Logger log) {

        long start = Long.parseLong(dayRange.getStartKey().getRow().toString());
        long end = Long.parseLong(dayRange.getEndKey().getRow().toString());

        return new Range(Long.toString(start / 60 / 60), true, Long.toString(end / 60 / 60), false);
    }

}
