package datawave.microservice.audit.replay.event.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cloud.bus.ConditionalOnBusEnabled;
import org.springframework.cloud.bus.ServiceMatcher;
import org.springframework.cloud.bus.event.AuditReplayRemoteRequestEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import datawave.microservice.audit.replay.ReplayController;

/**
 * Listens for remote request events from other audit service instances, and forwards the requests to the replay controller.
 */
@Component
@ConditionalOnBusEnabled
@ConditionalOnProperty(name = "audit.replay.enabled", havingValue = "true")
public class AuditReplayRemoteRequestEventListener implements ApplicationListener<AuditReplayRemoteRequestEvent> {
    private Logger log = LoggerFactory.getLogger(getClass());
    
    private final ReplayController replayController;
    private final ServiceMatcher serviceMatcher;
    
    @Autowired
    public AuditReplayRemoteRequestEventListener(ReplayController replayController, ServiceMatcher serviceMatcher) {
        this.replayController = replayController;
        this.serviceMatcher = serviceMatcher;
    }
    
    @Override
    public void onApplicationEvent(AuditReplayRemoteRequestEvent event) {
        // Ignore events that this service instance published, since we publish from a place
        // that takes the same action we do here, and we don't want to repeat the work.
        if (serviceMatcher.isFromSelf(event)) {
            log.debug("Dropping {} since it is from us.", event);
            return;
        }
        
        replayController.handleRemoteRequest(event.getRequest());
    }
}
