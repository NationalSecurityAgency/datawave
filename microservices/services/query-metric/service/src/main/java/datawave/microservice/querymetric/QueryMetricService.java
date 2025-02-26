package datawave.microservice.querymetric;

import static com.hazelcast.internal.util.EmptyStatement.ignore;

import java.io.IOException;
import java.net.ServerSocket;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.web.servlet.error.ErrorMvcAutoConfiguration;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.commons.util.InetUtils;
import org.springframework.cloud.commons.util.InetUtilsProperties;

/**
 * Launcher for the query metric service.
 */
@EnableDiscoveryClient
@SpringBootApplication(scanBasePackages = {"datawave.microservice", "datawave.query.util"}, exclude = {ErrorMvcAutoConfiguration.class})
public class QueryMetricService {
    private static final int MAX_PORT = 100;
    
    static {
        System.setProperty("hazelcast.cluster.host", findHostInfo().getIpAddress());
        System.setProperty("hazelcast.cluster.port", String.valueOf(getNextPort(5701)));
    }
    
    public static void main(String[] args) {
        SpringApplication.run(QueryMetricService.class, args);
    }
    
    public static void shutdown() {
        System.exit(-1);
    }
    
    private static int getNextPort(int start) {
        for (int port = start; port < start + MAX_PORT; ++port) {
            try {
                new ServerSocket(port).close();
                return port;
            } catch (IOException e) {
                ignore(e);
            }
        }
        return -1;
    }
    
    private static InetUtils.HostInfo findHostInfo() {
        InetUtils inetUtils = new InetUtils(new InetUtilsProperties());
        return inetUtils.findFirstNonLoopbackHostInfo();
    }
}
