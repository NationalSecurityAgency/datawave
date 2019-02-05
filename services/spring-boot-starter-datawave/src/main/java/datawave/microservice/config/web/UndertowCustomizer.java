package datawave.microservice.config.web;

import io.undertow.Undertow;
import io.undertow.server.Connectors;
import io.undertow.servlet.handlers.ServletRequestContext;
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
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;
import org.xnio.Options;

import javax.servlet.ServletRequest;

import static datawave.microservice.config.web.Constants.REQUEST_START_TIME_NS_ATTRIBUTE;

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
    public void setApplicationContext(@NonNull ApplicationContext applicationContext) throws BeansException {
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

        factory.addDeploymentInfoCustomizers(deploymentInfo -> {
            // Use the initial handler chain to set the request start time as early as possible in the call chain.
            // The ServletRequestContext won't be set on the exchange just yet, though, so we'll need to copy that
            // attribute onto the ServletRequest on the inner handler wrapper.
            deploymentInfo.addInitialHandlerChainWrapper(httpHandler ->
                httpServerExchange -> {
                    if (httpServerExchange.getRequestStartTime() == -1) {
                        Connectors.setRequestStartTime(httpServerExchange);
                    }
                    httpHandler.handleRequest(httpServerExchange);

                });
            deploymentInfo.addInnerHandlerChainWrapper(httpHandler ->
                httpServerExchange -> {
                    ServletRequestContext ctx = httpServerExchange.getAttachment(ServletRequestContext.ATTACHMENT_KEY);
                    if (ctx != null) {
                        ServletRequest servletRequest = ctx.getServletRequest();
                        if (servletRequest != null) {
                            servletRequest.setAttribute(REQUEST_START_TIME_NS_ATTRIBUTE, httpServerExchange.getRequestStartTime());
                        } else {
                            logger.warn("ServletRequest is null on the ServletRequestContext.");
                        }
                    } else {
                        logger.warn("ServletRequestContext could not be found on the HttpServerExchange.");
                    }
                    httpHandler.handleRequest(httpServerExchange);
                });
        });
        // @formatter:on
    }
}
