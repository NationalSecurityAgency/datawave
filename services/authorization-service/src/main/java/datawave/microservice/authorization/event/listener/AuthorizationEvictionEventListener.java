package datawave.microservice.authorization.event.listener;

import datawave.security.authorization.CachedDatawaveUserService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.bus.ConditionalOnBusEnabled;
import org.springframework.cloud.bus.ServiceMatcher;
import org.springframework.cloud.bus.event.AuthorizationEvictionEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

/**
 * A listener for {@link AuthorizationEvictionEvent}s on the message bus. Upon receipt, the designated users are evicted from the authorization cache by
 * delegating to {@link CachedDatawaveUserService}.
 */
@Component
@ConditionalOnBusEnabled
public class AuthorizationEvictionEventListener implements ApplicationListener<AuthorizationEvictionEvent> {
    private Logger log = LoggerFactory.getLogger(getClass());
    private final CachedDatawaveUserService userService;
    private final ServiceMatcher serviceMatcher;
    
    @Autowired
    public AuthorizationEvictionEventListener(CachedDatawaveUserService userService, ServiceMatcher serviceMatcher) {
        this.userService = userService;
        this.serviceMatcher = serviceMatcher;
    }
    
    @Override
    public void onApplicationEvent(AuthorizationEvictionEvent event) {
        // Ignore events that this service instance published, since we publish from a place
        // that takes the same action we do here, and we don't want to repeat the work.
        if (serviceMatcher.isFromSelf(event)) {
            log.debug("Dropping {} since it is from us.", event);
            return;
        }
        switch (event.getEvictionType()) {
            case FULL:
                log.info("Received event to evict all users from the cache.");
                userService.evictAll();
                break;
            case PARTIAL:
                log.info("Received event to evict users matching " + event.getSubstring() + " from the cache.");
                userService.evictMatching(event.getSubstring());
                break;
            case USER:
                log.info("Received event to evict user " + event.getSubstring() + " from the cache.");
                userService.evict(event.getSubstring());
                break;
        }
    }
}
