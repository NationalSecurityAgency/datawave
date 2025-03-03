package datawave.microservice.config.web;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.web.ServerProperties;
import org.springframework.boot.web.embedded.undertow.ConfigurableUndertowWebServerFactory;
import org.springframework.boot.web.embedded.undertow.UndertowServletWebServerFactory;
import org.springframework.boot.web.server.AbstractConfigurableWebServerFactory;
import org.springframework.boot.web.server.Http2;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;
import org.xnio.Options;

import io.undertow.Undertow;
import io.undertow.UndertowOptions;

/**
 * General Undertow customization for DATAWAVE use. This configuration applies to either a Servlet or Reactive Undertow server.
 * <p>
 * <ul>
 * <li>Configures Undertow to listen on both the secure and non-secure port.</li>
 * <li>Configures HTTP/2 support (if enabled via the property {@code undertow.enable.http2}</li>
 * <li>Tells Undertow workers to be daemon threads (enabled via the property {@code undertow.thread.daemon}, default is {@code false}</li>
 * </ul>
 */
@Component
@ConditionalOnClass({Undertow.class, ConfigurableUndertowWebServerFactory.class})
public class UndertowCustomizer implements WebServerFactoryCustomizer<UndertowServletWebServerFactory>, ApplicationContextAware {
    @Value("${undertow.enable.http2:true}")
    private boolean enableHttp2;
    
    @Value("${undertow.thread.daemon:false}")
    private boolean useDaemonThreads;
    
    private ApplicationContext applicationContext;
    
    private ServerProperties serverProperties;
    private DatawaveServerProperties datawaveServerProperties;
    
    @Override
    public void setApplicationContext(@NonNull ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
    
    @Override
    public void customize(UndertowServletWebServerFactory factory) {
        serverProperties = applicationContext.getBean(ServerProperties.class);
        datawaveServerProperties = applicationContext.getBean(DatawaveServerProperties.class);
        
        Http2 http2 = new Http2();
        http2.setEnabled(enableHttp2);
        factory.setHttp2(http2);
        
        // @formatter:off
        factory.addBuilderCustomizers(c -> {
            // Ensure that the request start time is set on the request by Undertow
            c.setServerOption(UndertowOptions.RECORD_REQUEST_START_TIME, true);
            // Tell XNIO to use Daemon threads if enabled.
            c.setWorkerOption(Options.THREAD_DAEMON, useDaemonThreads);

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
