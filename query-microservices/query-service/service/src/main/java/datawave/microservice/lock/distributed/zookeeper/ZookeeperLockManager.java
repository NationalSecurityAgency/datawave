package datawave.microservice.lock.distributed.zookeeper;

import datawave.microservice.lock.Lock;
import datawave.microservice.lock.LockManager;
import datawave.microservice.lock.Semaphore;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreV2;
import org.apache.curator.framework.recipes.shared.SharedCount;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component("distributedLockManager")
@ConditionalOnBean(CuratorFramework.class)
@ConditionalOnProperty(name = "datawave.lock.type", havingValue = "zookeeper")
public class ZookeeperLockManager implements LockManager {
    
    final private CuratorFramework curatorFramework;
    
    public ZookeeperLockManager(CuratorFramework curatorFramework) {
        this.curatorFramework = curatorFramework;
    }
    
    @Override
    public Semaphore getSemaphore(String name, int permits) throws Exception {
        return new ZookeeperSemaphore(name, new InterProcessSemaphoreV2(curatorFramework, name, new SharedCount(curatorFramework, name + "/permits", permits)),
                        curatorFramework);
    }
    
    @Override
    public Lock getLock(String name) {
        return new ZookeeperLock(name, new InterProcessMutex(curatorFramework, name));
    }
}
