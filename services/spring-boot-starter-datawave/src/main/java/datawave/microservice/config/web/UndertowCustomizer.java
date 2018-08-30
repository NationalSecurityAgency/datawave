package datawave.microservice.config.web;

import io.undertow.Undertow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.web.ServerProperties;
import org.springframework.boot.web.embedded.undertow.ConfigurableUndertowWebServerFactory;
import org.springframework.boot.web.server.AbstractConfigurableWebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;
import org.xnio.Options;

/**
 * Customizes Undertow for DATAWAVE use. Configures HTTP/2 unless disabled by the property {@code undertow.enable.http2}. This customizer also manages
 * configuring both the secure and non-secure listeners.
 */
@Component
@ConditionalOnClass({Undertow.class, ConfigurableUndertowWebServerFactory.class})
public class UndertowCustomizer implements WebServerFactoryCustomizer<ConfigurableUndertowWebServerFactory>, ApplicationContextAware {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    
    @Value("${undertow.enable.http2:true}")
    private boolean enableHttp2;
    
    @Value("${undertow.thread.daemon:false}")
    private boolean useDaemonThreads;
    
    private ApplicationContext applicationContext;
    
    private ServerProperties serverProperties;
    private DatawaveServerProperties datawaveServerProperties;
    
    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
    
    @Override
    public void customize(ConfigurableUndertowWebServerFactory factory) {
        serverProperties = applicationContext.getBean(ServerProperties.class);
        datawaveServerProperties = applicationContext.getBean(DatawaveServerProperties.class);
        
        // @formatter:off
        factory.addBuilderCustomizers(c -> {
            if (useDaemonThreads) {
                // Tell XNIO to use Daemon threads
                c.setWorkerOption(Options.THREAD_DAEMON, true);
            }

            if (factory instanceof AbstractConfigurableWebServerFactory) {
                AbstractConfigurableWebServerFactory undertowFactory = (AbstractConfigurableWebServerFactory) factory;
                // If we're using ssl and also want a non-secure listener, then add it here since the parent won't configure both
                if (serverProperties.getSsl() != null && serverProperties.getSsl().isEnabled() && datawaveServerProperties.getNonSecurePort() != null &&
                        datawaveServerProperties.getNonSecurePort() >= 0) {
                    String host = undertowFactory.getAddress() == null ? "0.0.0.0" : undertowFactory.getAddress().getHostAddress();
                    c.addHttpListener(datawaveServerProperties.getNonSecurePort(), host);
                }
            }
        });
        // @formatter:on
    }
}
