package datawave.microservice.lock.distributed.hazelcast;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPSubsystem;
import com.hazelcast.cp.ISemaphore;
import datawave.microservice.lock.LockManager;
import datawave.microservice.lock.Semaphore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component("lockManager")
@ConditionalOnMissingBean(name = "lockManager")
@ConditionalOnBean(HazelcastInstance.class)
@ConditionalOnProperty(name = "datawave.lock.type", havingValue = "hazelcast")
public class HazelcastLockManager implements LockManager {

    private CPSubsystem cpSubsystem;

    public HazelcastLockManager(HazelcastInstance hazelcastInstance) {
        this.cpSubsystem = hazelcastInstance.getCPSubsystem();
    }

    @Override
    public Semaphore getSemaphore(String name, int permits) throws Exception {
        ISemaphore iSemaphore = cpSubsystem.getSemaphore(name);
        if (!iSemaphore.init(permits)) {
            throw new Exception("Unable to initialize ISemaphore");
        }
        return new HazelcastSemaphore(iSemaphore);
    }
}
