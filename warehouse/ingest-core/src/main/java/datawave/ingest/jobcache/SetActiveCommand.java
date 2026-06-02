package datawave.ingest.jobcache;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

/**
 * Command line tool to set the active job cache in Zookeeper.
 */
@Parameters(commandDescription = "Sets the active job cache in Zookeeper.")
public class SetActiveCommand {

    private static final int ZK_NUM_RETRIES = 3;
    private static final int ZK_RETRY_SLEEP_MS = 1000;

    @Parameter(names = {"-z", "--zookeepers"}, description = "The zookeeper servers to update.", required = true, validateWith = NonEmptyStringValidator.class)
    private String zookeepers;

    @Parameter(names = {"-p", "--path"}, description = "The zookeeper path where the active job cache will be stored.", required = true,
                    validateWith = NonEmptyStringValidator.class)
    private String zkPath;

    @Parameter(names = {"-j", "--job-cache"}, description = "The full HDFS path to the active job cache (e.g. 'hdfs://ingest/data/jobCacheA').",
                    required = true, validateWith = NonEmptyStringValidator.class)
    private String jobCache;

    @Parameter(names = {"-h", "--help"}, description = "Prints the command usage.", help = true)
    private boolean help;

    public void run() {
        try (CuratorFramework zkClient = CuratorFrameworkFactory.newClient(zookeepers, new ExponentialBackoffRetry(ZK_RETRY_SLEEP_MS, ZK_NUM_RETRIES))) {
            zkClient.start();

            new ActiveSetter(zkClient).set(zkPath, jobCache);

        } catch (Exception e) {
            throw new RuntimeException("Failed to update " + zkPath + " to " + jobCache + ". Try again.", e);
        }
    }

    public boolean isHelp() {
        return help;
    }

    public static void main(String[] args) {
        SetActiveCommand tool = new SetActiveCommand();
        JCommander jcommander = JCommander.newBuilder().addObject(tool).build();

        try {
            jcommander.parse(args);

            if (tool.isHelp()) {
                jcommander.usage();
            } else {
                tool.run();
            }

        } catch (ParameterException e) {
            System.err.println(e.getMessage());
            jcommander.usage();
            System.exit(1);
        }
    }
}
