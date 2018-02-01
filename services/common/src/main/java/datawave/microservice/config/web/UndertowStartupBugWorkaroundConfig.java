package datawave.microservice.config.web;

import io.undertow.Undertow;
import io.undertow.servlet.api.DeploymentManager;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.embedded.Compression;
import org.springframework.boot.context.embedded.EmbeddedServletContainerException;
import org.springframework.boot.context.embedded.PortInUseException;
import org.springframework.boot.context.embedded.undertow.UndertowEmbeddedServletContainer;
import org.springframework.boot.context.embedded.undertow.UndertowEmbeddedServletContainerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.ReflectionUtils;
import org.xnio.channels.BoundChannel;

import javax.servlet.ServletException;
import java.lang.reflect.Field;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * This configuration overrides {@link UndertowEmbeddedServletContainerFactory} to produce an extended version of {@link UndertowEmbeddedServletContainer} that
 * works around a bug. The bug occurs when there is an error during Undertow startup (e.g., a designated port is in use). When this happens, Undertow has
 * completed enough initialization that it has launched non-daemon threads. However because startup didn't complete, the normal mechanism to stop Undertow is
 * not invoked. Therefore, the non-daemon threads are left running and the application never exits.
 * <p>
 * <strong>WARNING:</strong> This is an ugly hack that causes a lot of code to be copied from the parent class {@link UndertowEmbeddedServletContainer}. When
 * the underlying Spring Boot bug is fixed, this code can be removed.
 */
@Configuration
public class UndertowStartupBugWorkaroundConfig {
    
    @Bean
    @ConditionalOnProperty(name = "undertow.startup.bug.workaround.enabled", matchIfMissing = true)
    public UndertowEmbeddedServletContainerFactory undertowEmbeddedServletContainerFactory() {
        return new ExtendedUndertowEmbeddedServletContainerFactory();
    }
    
    public static class ExtendedUndertowEmbeddedServletContainerFactory extends UndertowEmbeddedServletContainerFactory {
        @Override
        protected UndertowEmbeddedServletContainer getUndertowEmbeddedServletContainer(Undertow.Builder builder, DeploymentManager manager, int port) {
            return new ExtendedUndertowEmbeddedServletContainer(builder, manager, getContextPath(), isUseForwardHeaders(), port >= 0, getCompression(),
                            getServerHeader());
        }
    }
    
    public static class ExtendedUndertowEmbeddedServletContainer extends UndertowEmbeddedServletContainer {
        public ExtendedUndertowEmbeddedServletContainer(Undertow.Builder builder, DeploymentManager manager, String contextPath, boolean useForwardHeaders,
                        boolean autoStart, Compression compression, String serverHeader) {
            super(builder, manager, contextPath, useForwardHeaders, autoStart, compression, serverHeader);
        }
        
        @Override
        public void start() throws EmbeddedServletContainerException {
            try {
                super.start();
            } catch (Exception e) {
                Field undertowField = ReflectionUtils.findField(UndertowEmbeddedServletContainer.class, null, Undertow.class);
                ReflectionUtils.makeAccessible(undertowField);
                Undertow undertow = (Undertow) ReflectionUtils.getField(undertowField, this);
                
                Field deploymentManagerField = ReflectionUtils.findField(UndertowEmbeddedServletContainer.class, null, DeploymentManager.class);
                ReflectionUtils.makeAccessible(deploymentManagerField);
                DeploymentManager deploymentManager = (DeploymentManager) ReflectionUtils.getField(deploymentManagerField, this);
                
                try {
                    if (findBindException(e) != null) {
                        List<Port> failedPorts = getConfiguredPorts(undertow);
                        List<Port> actualPorts = getActualPorts(undertow);
                        failedPorts.removeAll(actualPorts);
                        if (failedPorts.size() >= 1) {
                            throw new PortInUseException(failedPorts.iterator().next().getNumber());
                        }
                    }
                } finally {
                    undertow.stop();
                    try {
                        deploymentManager.stop();
                    } catch (ServletException se) {
                        // Just ignore the exception since we're trying to shut down anyway.
                    }
                }
                
                // Re-throw the exception we caught above, if we didn't override it with a PortInUseException
                throw e;
            }
        }
        
        private BindException findBindException(Exception ex) {
            Throwable candidate = ex;
            while (candidate != null) {
                if (candidate instanceof BindException) {
                    return (BindException) candidate;
                }
                candidate = candidate.getCause();
            }
            return null;
        }
        
        private List<Port> getActualPorts(Undertow undertow) {
            List<Port> ports = new ArrayList<>();
            try {
                for (BoundChannel channel : extractChannels(undertow)) {
                    ports.add(getPortFromChannel(channel));
                }
            } catch (Exception ex) {
                // Continue
            }
            return ports;
        }
        
        @SuppressWarnings("unchecked")
        private List<BoundChannel> extractChannels(Undertow undertow) {
            Field channelsField = ReflectionUtils.findField(Undertow.class, "channels");
            ReflectionUtils.makeAccessible(channelsField);
            return (List<BoundChannel>) ReflectionUtils.getField(channelsField, undertow);
        }
        
        private Port getPortFromChannel(BoundChannel channel) {
            SocketAddress socketAddress = channel.getLocalAddress();
            if (socketAddress instanceof InetSocketAddress) {
                String protocol = ReflectionUtils.findField(channel.getClass(), "ssl") != null ? "https" : "http";
                return new Port(((InetSocketAddress) socketAddress).getPort(), protocol);
            }
            return null;
        }
        
        private List<Port> getConfiguredPorts(Undertow undertow) {
            List<Port> ports = new ArrayList<>();
            for (Object listener : extractListeners(undertow)) {
                try {
                    ports.add(getPortFromListener(listener));
                } catch (Exception ex) {
                    // Continue
                }
            }
            return ports;
        }
        
        @SuppressWarnings("unchecked")
        private List<Object> extractListeners(Undertow undertow) {
            Field listenersField = ReflectionUtils.findField(Undertow.class, "listeners");
            ReflectionUtils.makeAccessible(listenersField);
            return (List<Object>) ReflectionUtils.getField(listenersField, undertow);
        }
        
        private Port getPortFromListener(Object listener) {
            Field typeField = ReflectionUtils.findField(listener.getClass(), "type");
            ReflectionUtils.makeAccessible(typeField);
            String protocol = ReflectionUtils.getField(typeField, listener).toString();
            Field portField = ReflectionUtils.findField(listener.getClass(), "port");
            ReflectionUtils.makeAccessible(portField);
            int port = (Integer) ReflectionUtils.getField(portField, listener);
            return new Port(port, protocol);
        }
    }
    
    /**
     * An active Undertow port.
     */
    private final static class Port {
        
        private final int number;
        
        private final String protocol;
        
        private Port(int number, String protocol) {
            this.number = number;
            this.protocol = protocol;
        }
        
        public int getNumber() {
            return this.number;
        }
        
        @Override
        public String toString() {
            return this.number + " (" + this.protocol + ")";
        }
        
        @Override
        public int hashCode() {
            return this.number;
        }
        
        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Port other = (Port) obj;
            if (this.number != other.number) {
                return false;
            }
            return true;
        }
        
    }
}
