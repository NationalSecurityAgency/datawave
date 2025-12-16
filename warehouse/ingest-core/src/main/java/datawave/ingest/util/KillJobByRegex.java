package datawave.ingest.util;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * A simple utility class to kill hadoop jobs by matching their job name against a regular expression.
 */
public class KillJobByRegex {

    private static Pattern NAME_PATTERN = null;
    private static AtomicInteger JOB_KILLED_COUNT = new AtomicInteger();
    private static AtomicInteger JOB_FAILED_COUNT = new AtomicInteger();
    private static ExecutorService JOB_KILLER_SVC = Executors.newFixedThreadPool(10);

    public static class JobKiller implements Runnable {

        private JobStatus js;
        private Cluster cluster;

        public JobKiller(Cluster cluster, JobStatus js) {
            this.cluster = cluster;
            this.js = js;
        }

        @Override
        public void run() {
            Job job = null;
            try {
                job = cluster.getJob(js.getJobID());
            } catch (Exception e) {
                System.out.println("Error getting job: " + js.getJobID() + " Reason: " + e.getMessage());
                JOB_FAILED_COUNT.getAndIncrement();
                return;
            }
            String jobName = job.getJobName();
            if (NAME_PATTERN.matcher(jobName).matches()) {
                try {
                    job.killJob();
                    JOB_KILLED_COUNT.getAndIncrement();
                } catch (IOException e) {
                    System.out.println("Error killing job: " + js.getJobID() + " Reason: " + e.getMessage());
                    JOB_FAILED_COUNT.getAndIncrement();
                }
            }
        }

    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Configuration conf = new Configuration();
        GenericOptionsParser parser = new GenericOptionsParser(conf, args);
        args = parser.getRemainingArgs();

        if (args.length != 1) {
            System.err.println("usage: KillJobByRegex jobNamePattern");
            System.exit(1);
        }

        NAME_PATTERN = Pattern.compile(args[0]);

        org.apache.hadoop.mapred.JobConf jobConf = new org.apache.hadoop.mapred.JobConf(conf);
        Cluster cluster = new Cluster(jobConf);

        for (JobStatus js : cluster.getAllJobStatuses()) {
            if (!js.isJobComplete()) {
                JOB_KILLER_SVC.execute(new JobKiller(cluster, js));
            }
        }

        try {
            JOB_KILLER_SVC.shutdown(); // signal shutdown
            JOB_KILLER_SVC.awaitTermination(1, TimeUnit.MINUTES); // allow processes to stop
        } catch (InterruptedException e) {
            JOB_KILLER_SVC.shutdownNow();
        }

        System.out.println("Killed " + JOB_KILLED_COUNT.get() + " jobs");
        System.out.println("Failed to kill " + JOB_FAILED_COUNT.get() + " jobs");
        System.exit(0);
    }
}
