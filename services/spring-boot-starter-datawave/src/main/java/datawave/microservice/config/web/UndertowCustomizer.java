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
 * Customizes Undertow for DATAWAVE use. Configures HTTP/2 unless disabled by the property {@code undertow.enable.http2}. This customizer also manages
 * configuring both the secure and non-secure listeners.
 */
@Component
@ConditionalOnClass({Undertow.class, UndertowEmbeddedServletContainerFactory.class})
public class UndertowCustomizer implements EmbeddedServletContainerCustomizer, ApplicationContextAware {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    
    @Value("${undertow.enable.http2:true}")
    private boolean enableHttp2;
    
    @Value("${undertow.thread.daemon:false}")
    private boolean useDaemonThreads;
    
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
        
        if (enableHttp2) {
            undertowContainer.addBuilderCustomizers(c -> c.setServerOption(UndertowOptions.ENABLE_HTTP2, true));
        }
        
        // @formatter:off
        undertowContainer.addBuilderCustomizers(c -> {
            if (useDaemonThreads) {
                // Tell XNIO to use Daemon threads
                c.setWorkerOption(Options.THREAD_DAEMON, true);
            }

            // If we're using ssl and also want a non-secure listener, then add it here since the parent won't configure both
            if (serverProperties.getSsl() != null && serverProperties.getSsl().isEnabled() && serverProperties.getNonSecurePort() != null &&
                    serverProperties.getNonSecurePort() >= 0) {
                String host = undertowContainer.getAddress() == null ? "0.0.0.0" : undertowContainer.getAddress().getHostAddress();
                c.addHttpListener(serverProperties.getNonSecurePort(), host);
            }
        });
        // @formatter:on
    }
}
