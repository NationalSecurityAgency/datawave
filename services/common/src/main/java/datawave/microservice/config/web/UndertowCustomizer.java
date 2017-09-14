package datawave.microservice.config.web;

import io.undertow.Undertow;
import io.undertow.UndertowOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.context.embedded.ConfigurableEmbeddedServletContainer;
import org.springframework.boot.context.embedded.EmbeddedServletContainerCustomizer;
import org.springframework.boot.context.embedded.undertow.UndertowEmbeddedServletContainerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;
import org.xnio.Options;

/**
 * Customizdes Undertow for DATAWAVE use. Configures HTTP/2 unless disabled by the property {@code undertow.enable.http2}. This customizer also manages
 * configuring both the secure and non-secure listeners.
 */
@Component
@ConditionalOnClass({Undertow.class, UndertowEmbeddedServletContainerFactory.class})
public class UndertowCustomizer implements EmbeddedServletContainerCustomizer, ApplicationContextAware {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    
    @Value("${undertow.enable.http2:true}")
    private boolean enableHttp2;
    
    private ApplicationContext applicationContext;
    
    private DatawaveServerProperties serverProperties;
    
    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
    
    @Override
    public void customize(ConfigurableEmbeddedServletContainer container) {
        if (!(container instanceof UndertowEmbeddedServletContainerFactory)) {
            logger.error("Container {} is not an UndertowEmbeddedServletContainer. Cannot configure.", container.getClass());
            return;
        }
        final UndertowEmbeddedServletContainerFactory undertowContainer = (UndertowEmbeddedServletContainerFactory) container;
        
        serverProperties = applicationContext.getBean(DatawaveServerProperties.class);
        serverProperties.getError().setPath(null);
        
        // Replace the port with the secure port if we're using SSL, and save the original non-secure port in case we're configuring that later.
        final int nonSecurePort = serverProperties.getPort();
        if (serverProperties.getSsl() != null && serverProperties.getSsl().isEnabled()) {
            // If the secure port has been set, then we want to use it on the container. If not, then we'll use the default port.
            if (serverProperties.getSecurePort() != null) {
                // Override the port on the container. It will create a secured listenerby default if ssl is configured
                // so we want it to use the secured port.
                container.setPort(serverProperties.getSecurePort());
                
                // If we're only using ssl, then ensure the non-secure port matches the secure port. Other bits of code in the chain
                // may pull the nonSecurePort and make the assumption that it is the only port in use (i.e., if we're only using ssl
                // some code assumes the non-secure port is the secure port).
                if (!serverProperties.isNonSecureEnabled()) {
                    serverProperties.setPort(serverProperties.getSecurePort());
                }
            } else {
                serverProperties.setSecurePort(nonSecurePort);
            }
        }
        
        if (enableHttp2) {
            undertowContainer.addBuilderCustomizers(c -> c.setServerOption(UndertowOptions.ENABLE_HTTP2, true));
        }
        
        undertowContainer.addBuilderCustomizers(c -> {
            // @formatter:off
            // Tell XNIO to use Daemon threads (works around a bug where the VM won't exit if there's an error during undertow startup)
            c.setWorkerOption(Options.THREAD_DAEMON, true);

            // If we're using ssl and also want a non-secure listener, then add it here since the parent won't configure both
            if (serverProperties.getSsl() != null && serverProperties.getSsl().isEnabled() && serverProperties.isNonSecureEnabled()) {
                String host = undertowContainer.getAddress() == null ? "0.0.0.0" : undertowContainer.getAddress().getHostAddress();
                c.addHttpListener(nonSecurePort, host);
            }
            // @formatter:on
                    });
    }
}
