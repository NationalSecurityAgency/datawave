package datawave.microservice.config.web;

import static datawave.microservice.config.web.Constants.REQUEST_START_TIME_NS_ATTRIBUTE;

import javax.servlet.ServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.web.embedded.undertow.UndertowServletWebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.stereotype.Component;

import io.undertow.Undertow;
import io.undertow.server.Connectors;
import io.undertow.servlet.handlers.ServletRequestContext;

/**
 * Customizes Undertow Servlet-based server (vs the Reactive one) for DATAWAVE use. This handles setting up recording the request start time as early as
 * possible.
 */
@Component
@ConditionalOnClass({Undertow.class, UndertowServletWebServerFactory.class})
public class UndertowServletWebServerCustomizer implements WebServerFactoryCustomizer<UndertowServletWebServerFactory> {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    
    @Override
    public void customize(UndertowServletWebServerFactory factory) {
        
        factory.addDeploymentInfoCustomizers(deploymentInfo -> {
            // Use the initial handler chain to set the request start time as early as possible in the call chain.
            // The ServletRequestContext won't be set on the exchange just yet, though, so we'll need to copy that
            // attribute onto the ServletRequest on the inner handler wrapper.
            deploymentInfo.addInitialHandlerChainWrapper(httpHandler -> httpServerExchange -> {
                if (httpServerExchange.getRequestStartTime() == -1) {
                    Connectors.setRequestStartTime(httpServerExchange);
                }
                httpHandler.handleRequest(httpServerExchange);
                
            });
            deploymentInfo.addInnerHandlerChainWrapper(httpHandler -> httpServerExchange -> {
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
