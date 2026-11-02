package datawave.microservice.annotationCache.api;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.hazelcast.config.Config;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.BuildInfoProvider;

/**
 * Started by {@code verify-supported-hazelcast-pairing.sh}, this loopback member provides test entries for the Sonicweb client probe. The script checks the
 * services' actual runtime dependencies in separate JVMs, which embedded tests with a shared classpath cannot verify.
 */
public final class AnnotationCacheHazelcastMemberProbe {
    private AnnotationCacheHazelcastMemberProbe() {}

    public static void main(String[] args) throws InterruptedException {
        String version = BuildInfoProvider.getBuildInfo().getVersion();
        int port = Integer.parseInt(args[0]);
        Config config = new Config();
        config.setClusterName("annotation-cache-wire-v1");
        config.setProperty("hazelcast.logging.type", "none");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        config.getNetworkConfig().setPort(port).setPortAutoIncrement(false);
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
        addMapIndexes(config, Constants.ANNOTATIONS_MAP);
        addMapIndexes(config, Constants.FETCH_MAP);

        HazelcastInstance member = Hazelcast.newHazelcastInstance(config);
        String shared = new String("member-write-doc");
        member.<AnnotationKey,String> getMap(Constants.ANNOTATIONS_MAP).put(new AnnotationKey("MEMBER", shared, shared), "member-side-value");
        member.<FetchKey,FetchRecord> getMap(Constants.FETCH_MAP).put(new FetchKey("MEMBER", shared, shared), new FetchRecord(201L, 6));
        Runtime.getRuntime().addShutdownHook(new Thread(member::shutdown));
        System.out.println("READY " + port + " Hazelcast member " + version);
        System.out.flush();
        new CountDownLatch(1).await(2, TimeUnit.MINUTES);
        member.shutdown();
    }

    private static void addMapIndexes(Config config, String mapName) {
        config.addMapConfig(new MapConfig(mapName)
                        .addIndexConfig(new IndexConfig(IndexType.HASH, Constants.ID_TYPE_KEY_ATTRIBUTE, Constants.DOCUMENT_ID_KEY_ATTRIBUTE))
                        .addIndexConfig(new IndexConfig(IndexType.HASH, Constants.DOCUMENT_ID_KEY_ATTRIBUTE)));
    }
}
