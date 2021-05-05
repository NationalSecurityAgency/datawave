package datawave.microservice.lock.local;

import datawave.microservice.lock.LockManager;
import datawave.microservice.lock.Semaphore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component("lockManager")
@ConditionalOnMissingBean(name = "lockManager")
@ConditionalOnProperty(name = "datawave.lock.type", havingValue = "local")
public class LocalLockManager implements LockManager {
    @Override
    public Semaphore getSemaphore(String name, int permits) {
        return new LocalSemaphore(name, new java.util.concurrent.Semaphore(permits));
    }
}
