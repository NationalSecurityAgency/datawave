package datawave.reggieTests;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.system.SortedMapIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class ConsistencyTestCases {

    // @TempDir
    public static Path temporaryFolder;

    private static final String PASSWORD = "password";

    private static MiniAccumuloCluster mac;

    private static AccumuloClient client;

    // @BeforeAll
    // public static void setup() throws IOException, InterruptedException {
    // temporaryFolder = Files.createTempDirectory("myTestDir_");
    // MiniAccumuloConfig config = new MiniAccumuloConfig(temporaryFolder.toFile(), PASSWORD);
    // // int numTservers = config.getNumTservers();
    // int numTservers = 0;
    // config.setNumTservers(1);
    //
    // mac = new MiniAccumuloCluster(config);
    // mac.start();
    //
    // Thread.sleep(5000);
    //
    // client = mac.createAccumuloClient("root", new PasswordToken(PASSWORD));
    // }

    @BeforeAll
    public static void setup() throws Exception {
        temporaryFolder = Files.createTempDirectory("accumuloTestDir");

        MiniAccumuloConfig config = new MiniAccumuloConfig(temporaryFolder.toFile(), PASSWORD);
//        config.setNumTservers(2);

        // Enable ScanServers and executor pools
//        Map<String,String> siteConfig = new HashMap<>();
//        siteConfig.put("tserver.scan.executors", "default:4,scan:8");
//        siteConfig.put("scanserver.enabled", "true");
//        siteConfig.put("scanserver.port.search", "true");
//        config.setSiteConfig(siteConfig);

        mac = new MiniAccumuloCluster(config);
        mac.start();

        Thread.sleep(5000);
        client = mac.createAccumuloClient("root", new PasswordToken(PASSWORD));
    }

    @AfterAll
    public static void tearDown() throws IOException, InterruptedException {
        mac.stop();
    }

    @Test
    void testSomething() {
        MiniAccumuloConfig config = mac.getConfig();

        int numTservers = config.getNumTservers();

        config.getSiteConfig().forEach((key, value) -> {
            System.out.println(key);
            System.out.println(value);
        });

    }

    @Test
    public void testPrefixFilterIterator() throws Exception {
        SortedMap<Key,Value> data = new TreeMap<>();
        data.put(new Key("apple"), new Value("fruit".getBytes()));
        data.put(new Key("apricot"), new Value("fruit".getBytes()));
        data.put(new Key("banana"), new Value("fruit".getBytes()));

        // Wrap data in a SortedMapIterator (public API)
        SortedKeyValueIterator<Key,Value> source = new SortedMapIterator(data);

        PrefixFilterIterator iterator = new PrefixFilterIterator();
        Map<String,String> options = new HashMap<>();
        PrefixFilterIterator.setPrefixOption(options, "ap");

        iterator.init(source, options, null);
        iterator.seek(new Range(), Collections.emptyList(), false);

        List<String> keys = new ArrayList<>();
        while (iterator.hasTop()) {
            keys.add(iterator.getTopKey().getRow().toString());
            iterator.next();
        }

        assertEquals(Arrays.asList("apple", "apricot"), keys);
    }

    public static void loadTestData(String tableName, int rows) throws Exception {
        client.tableOperations().create(tableName);

        try (BatchWriter writer = client.createBatchWriter(tableName)) {
            for (int i = 0; i < rows; i++) {
                Mutation m = new Mutation("row" + i);
                m.put("cf", "cq", new Value(("value" + i).getBytes()));
                writer.addMutation(m);
            }
        }
    }

    @Test
    public void testConcurrentScans() throws Exception {
        final String table = "scanTestTable";
        final int numRows = 5000;
        loadTestData(table, numRows);

        int numThreads = 5;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        Callable<Integer> scanTask = () -> {
            int count = 0;
            try (Scanner scanner = client.createScanner(table, Authorizations.EMPTY)) {
                for (Map.Entry<Key,Value> entry : scanner) {
                    count++;
                }
            }
            return count;
        };

        List<Future<Integer>> futures = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            futures.add(executor.submit(scanTask));
        }

        for (Future<Integer> future : futures) {
            assertEquals(numRows, (int) future.get());
        }

        executor.shutdown();
    }

    @Test
    public void printTabletLocations() throws Exception {
        Scanner scanner = client.createScanner("accumulo.metadata", Authorizations.EMPTY);
        scanner.setRange(new Range());

        for (Map.Entry<Key,Value> entry : scanner) {
            String row = entry.getKey().getRow().toString();
            if (row.contains("tableId")) {
                System.out.printf("Tablet: %s -> %s%n", row, entry.getValue().toString());
            }
        }
    }

    @Test
    public void testScanLoggingIterator() throws Exception {
        String table = "loggingTestTable";
        int rows = 100;

        client.tableOperations().create(table);

        try (BatchWriter writer = client.createBatchWriter(table)) {
            for (int i = 0; i < rows; i++) {
                Mutation m = new Mutation("row" + i);
                m.put("cf", "cq", new Value(("value" + i).getBytes()));
                writer.addMutation(m);
            }
        }

        // Setup iterator (priority doesn't matter for this example)
        IteratorSetting setting = new IteratorSetting(20, "logscan", ScanLoggingIterator.class);

        Scanner scanner = client.createScanner(table, Authorizations.EMPTY);
        scanner.addScanIterator(setting);

        int count = 0;
        for (Map.Entry<Key,Value> entry : scanner) {
            count++;
        }

        assertEquals(rows, count);
    }

    @Test
    public void testScanRunsOnScanServer() throws Exception {
        String table = "scanExecTable";
        client.tableOperations().create(table);

        try (BatchWriter writer = client.createBatchWriter(table)) {
            Mutation m = new Mutation("row1");
            m.put("cf", "cq", new Value("value".getBytes()));
            writer.addMutation(m);
        }

        IteratorSetting setting = new IteratorSetting(10, "logger", ScanLoggingIterator.class);
        Scanner scanner = client.createScanner(table, Authorizations.EMPTY);
        scanner.addScanIterator(setting);

        // Force this scan to be handled by a ScanServer
        scanner.setExecutionHints(Map.of("executor", "scanserver"));
        scanner.setConsistencyLevel(ScannerBase.ConsistencyLevel.EVENTUAL);

        // To force scan to run on TServer:
        // scanner.setExecutionHints(Map.of("executor", "default"));

        for (Map.Entry<Key,Value> entry : scanner) {
            System.out.println("Key: " + entry.getKey() + ", Value: " + entry.getValue());
        }
    }

}
