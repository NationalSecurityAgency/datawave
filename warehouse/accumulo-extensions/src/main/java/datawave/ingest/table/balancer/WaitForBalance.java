package datawave.ingest.table.balancer;

import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class can be used to call accumulo manager's waitForBalance to block on waiting for balancing to be complete
 */
public class WaitForBalance {

    private static final Logger log = LoggerFactory.getLogger(WaitForBalance.class);

    public static void main(String[] args) {
        String instance = System.getProperty("instance.name");
        String zookeepers = System.getProperty("instance.zookeepers");
        String user = System.getProperty("user");
        String password = System.getProperty("password");
        String timeoutStr = System.getProperty("balance.timeout");

        long timeout = 3600L;

        if (StringUtils.isNotBlank(timeoutStr)) {
            try {
                timeout = Long.parseLong(timeoutStr);
            } catch (NumberFormatException e) {
                log.error("Invalid timeout: {}", timeoutStr);
                System.exit(2);
            }
        }

        System.out.printf("Waiting for Accumulo balance (instance=%s, zk=%s, user=%s, timeout=%ds)%n", instance, zookeepers, user, timeout);

        try (AccumuloClient client = Accumulo.newClient().to(instance, zookeepers).as(user, password).build()) {

            ExecutorService executor = Executors.newSingleThreadExecutor();
            Future<?> task = executor.submit(() -> {
                try {
                    client.instanceOperations().waitForBalance();
                } catch (Exception e) {
                    throw new CompletionException(e);
                }
            });

            try {
                task.get(timeout, TimeUnit.SECONDS);
                log.info("Accumulo cluster is balanced.");
                System.exit(0);

            } catch (TimeoutException te) {
                task.cancel(true);
                log.error("Timed out after {} seconds waiting for balance.", timeout);
                System.exit(124);
            } catch (ExecutionException ee) {
                log.error("Error while waiting for balance: {}", String.valueOf(ee.getCause()));
                ee.getCause().printStackTrace(System.err);
                System.exit(3);
            } finally {
                executor.shutdownNow();
            }
        } catch (Exception e) {
            log.error("Failed to connect or execute: {}", e.getMessage());
            e.printStackTrace(System.err);
            System.exit(4);
        }
    }
}
