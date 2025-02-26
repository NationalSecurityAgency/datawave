package datawave.microservice.accumulo.stats.util;

import static org.apache.accumulo.core.conf.ClientProperty.INSTANCE_ZOOKEEPERS;

import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility to fetch the location (host:port) of the Accumulo monitor application.
 */
public class AccumuloMonitorLocator {
    private static final Logger LOGGER = LoggerFactory.getLogger(AccumuloMonitorLocator.class);
    
    private static final Charset ENCODING = StandardCharsets.UTF_8;
    private static final String MONITOR_HTTP_ADDR = "/accumulo/%s/monitor/http_addr";
    private static final int DEFAULT_NUM_RETRIES = 5;
    private static final int DEFAULT_RETRY_WAIT = 500;
    
    private RetryPolicy retryPolicy;
    
    public AccumuloMonitorLocator() {
        this(DEFAULT_NUM_RETRIES, DEFAULT_RETRY_WAIT);
    }
    
    public AccumuloMonitorLocator(int numRetries, int retryWaitMillis) {
        retryPolicy = new RetryNTimes(numRetries, retryWaitMillis);
    }
    
    /**
     * Fetches the 'host:port' for the Accumulo monitor from the zookeeper used by the given instance.
     *
     * @param accumuloClient
     *            the AccumuloClient to use for retrieving the monitor host:port
     * @return the monitor host:port, or null if not found
     */
    public String getHostPort(AccumuloClient accumuloClient) {
        Properties clientProps = accumuloClient.properties();
        try (CuratorFramework curator = CuratorFrameworkFactory.newClient(clientProps.getProperty(INSTANCE_ZOOKEEPERS.getKey()), retryPolicy)) {
            curator.start();
            byte[] bytes = curator.getData().forPath(String.format(MONITOR_HTTP_ADDR, accumuloClient.instanceOperations().getInstanceId()));
            String location = new String(bytes, ENCODING);
            if (location.startsWith("http://")) {
                URI uri = new URI(location);
                location = uri.getHost() + ":" + uri.getPort();
            }
            return location;
        } catch (Exception e) {
            LOGGER.error("Cloud not fetch Accumulo monitor URL from zookeeper", e);
            throw new IllegalStateException("Could not fetch Accumulo monitor URL from zookeeper", e);
        }
    }
}
