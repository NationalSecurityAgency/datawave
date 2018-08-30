package datawave.microservice.cached.server;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.security.servlet.SecurityAutoConfiguration;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.commons.util.InetUtils;
import org.springframework.cloud.commons.util.InetUtilsProperties;

import java.io.IOException;
import java.net.ServerSocket;

import static com.hazelcast.util.EmptyStatement.ignore;

@EnableDiscoveryClient
@SpringBootApplication(scanBasePackages = "datawave.microservice", exclude = SecurityAutoConfiguration.class)
public class HazelcastService {
    private static final int MAX_PORT = 100;
    
    static {
        System.setProperty("hazelcast.cluster.host", findHostInfo().getIpAddress());
        System.setProperty("hazelcast.cluster.port", String.valueOf(getNextPort(5701)));
    }
    
    public static void main(String[] args) {
        SpringApplication.run(HazelcastService.class, args);
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
