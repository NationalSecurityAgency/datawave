package datawave.webservice.query.cache.limits;

import java.nio.charset.StandardCharsets;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;
import org.springframework.context.annotation.Conditional;
import org.springframework.stereotype.Service;

import datawave.webservice.util.ZookeeperPropertyCondition;

@Service
@Conditional(ZookeeperPropertyCondition.class)
public class ZooKeekerQueryLimitStore implements QueryLimitStore {

    private final CuratorFramework curator;

    private static final String BASE_PATH = "/dn-limits";

    public ZooKeekerQueryLimitStore(CuratorFramework curator) {
        this.curator = curator;
    }

    @Override
    public Integer setQueryLimit(String dn, int limit) {
        final String base = BASE_PATH + dn;
        final byte[] data = Integer.toString(limit).getBytes(StandardCharsets.UTF_8);
        try {
            curator.create().creatingParentContainersIfNeeded().forPath(base, data);
            return limit;
        } catch (Exception e) {
            throw new RuntimeException("ZK Error while setting limit", e);
        }
    }

    @Override
    public Integer getQueryLimit(String dn, Integer defaultValue) {
        final String base = BASE_PATH + dn;
        try {
            final byte[] data = curator.getData().forPath(base);
            return Integer.parseInt(new String(data, StandardCharsets.UTF_8));
        } catch (KeeperException.NoNodeException e) {
            return defaultValue;
        } catch (Exception e) {
            throw new RuntimeException("ZK Error while setting limit", e);
        }
    }

}
