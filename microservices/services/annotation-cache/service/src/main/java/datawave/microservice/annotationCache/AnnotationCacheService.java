package datawave.microservice.annotationCache;

import static com.hazelcast.internal.util.EmptyStatement.ignore;

import java.io.IOException;
import java.net.ServerSocket;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.web.servlet.error.ErrorMvcAutoConfiguration;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.commons.util.InetUtils;
import org.springframework.cloud.commons.util.InetUtilsProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

/** Application entry point for the regional annotation cache and its scheduled topology monitoring. */
@EnableDiscoveryClient
@EnableScheduling
@SpringBootApplication(scanBasePackages = {"datawave.microservice"}, exclude = {ErrorMvcAutoConfiguration.class})
public class AnnotationCacheService {
    private static final int MAX_PORT = 100;

    // Consul discovery advertises this member address; Docker Compose uses it for its scaled replicas too.
    static {
        InetUtils inetUtils = new InetUtils(new InetUtilsProperties());
        System.setProperty("hazelcast.cluster.host", inetUtils.findFirstNonLoopbackHostInfo().getIpAddress());
        System.setProperty("hazelcast.cluster.port", String.valueOf(getNextPort(5701)));
    }

    public static void main(String[] args) {
        SpringApplication.run(AnnotationCacheService.class, args);
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
}
