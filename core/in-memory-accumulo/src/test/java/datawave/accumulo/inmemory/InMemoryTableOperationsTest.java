package datawave.accumulo.inmemory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.rfile.RFile;
import org.apache.accumulo.core.client.rfile.RFileWriter;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.LoadPlan;
import org.apache.accumulo.core.data.LoadPlan.RangeType;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class InMemoryTableOperationsTest {

    private static final String TABLE = "bulkTable";

    @TempDir
    File dir;

    private AccumuloClient client;
    private TableOperations tableOperations;

    @BeforeEach
    public void setup() throws Exception {
        client = new InMemoryAccumuloClient("root", new InMemoryAccumulo());
        tableOperations = client.tableOperations();
        tableOperations.create(TABLE);
    }

    @Test
    public void importDirectoryLoadsTheRFilesAndRemovesThem() throws Exception {
        Map<Key,Value> first = Map.of(new Key("row1", "cf", "cq", 3L), new Value("a"), new Key("row3", "cf", "cq", 4L), new Value("b"));
        Map<Key,Value> second = Map.of(new Key("row2", "cf", "cq", 5L), new Value("c"));
        writeRFile("first.rf", first);
        writeRFile("second.rf", second);

        tableOperations.importDirectory(dir.getAbsolutePath()).to(TABLE).load();

        SortedMap<Key,Value> expected = new TreeMap<>(first);
        expected.putAll(second);
        assertEquals(expected, scan(Authorizations.EMPTY));
        // the files are gone, along with the checksums the local file system wrote for them
        assertEquals(Set.of(), Set.of(dir.list()));
    }

    @Test
    public void importDirectoryOnlyLoadsTheFilesAccumuloWould() throws Exception {
        Map<Key,Value> loaded = Map.of(new Key("row1", "cf", "cq", 3L), new Value("a"));
        Map<Key,Value> skipped = Map.of(new Key("row2", "cf", "cq", 3L), new Value("b"));
        writeRFile("data.rf", loaded);
        // Accumulo's own working files, directories (even one named like an RFile) and files without the RFile extension are left alone
        writeRFile("accumulo-bulk-data.rf", skipped);
        Files.createDirectory(dir.toPath().resolve("nested.rf"));
        writeRFile("nested.rf/data.rf", skipped);
        Files.writeString(dir.toPath().resolve("data.txt"), "not an rfile");
        Files.createFile(dir.toPath().resolve("_SUCCESS"));

        tableOperations.importDirectory(dir.getAbsolutePath()).to(TABLE).load();

        assertEquals(loaded, scan(Authorizations.EMPTY));
        assertEquals(Set.of("accumulo-bulk-data.rf", ".accumulo-bulk-data.rf.crc", "nested.rf", "data.txt", "_SUCCESS"), Set.of(dir.list()));
        assertTrue(new File(dir, "nested.rf/data.rf").exists());
    }

    @Test
    public void importDirectoryWithTableTimeGivesEveryEntryTheImportTime() throws Exception {
        writeRFile("data.rf", Map.of(new Key("row1", "cf", "cq", 3L), new Value("a"), new Key("row2", "cf", "cq", 4L), new Value("b")));

        long before = System.currentTimeMillis();
        tableOperations.importDirectory(dir.getAbsolutePath()).to(TABLE).tableTime(true).load();
        long after = System.currentTimeMillis();

        SortedMap<Key,Value> entries = scan(Authorizations.EMPTY);
        assertEquals(Set.of("row1", "row2"), entries.keySet().stream().map(key -> key.getRow().toString()).collect(Collectors.toSet()));
        Set<Long> timestamps = entries.keySet().stream().map(Key::getTimestamp).collect(Collectors.toSet());
        assertEquals(1, timestamps.size());
        long timestamp = timestamps.iterator().next();
        assertTrue(before <= timestamp && timestamp <= after, "timestamp " + timestamp + " is not the time of the import");
    }

    @Test
    public void importDirectoryWithNothingToLoad() throws Exception {
        String path = dir.getAbsolutePath();

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> tableOperations.importDirectory(path).to(TABLE).load());
        assertTrue(e.getMessage().startsWith("Attempted to import zero files from"), e.getMessage());
        tableOperations.importDirectory(path).to(TABLE).ignoreEmptyDir(true).load();

        // as in Accumulo, an RFile without entries leaves nothing to import, and it stays where it is
        writeRFile("empty.rf", Map.of());
        assertThrows(IllegalArgumentException.class, () -> tableOperations.importDirectory(path).to(TABLE).load());
        tableOperations.importDirectory(path).to(TABLE).ignoreEmptyDir(true).load();
        assertTrue(new File(dir, "empty.rf").exists());
        assertEquals(Map.of(), scan(Authorizations.EMPTY));
    }

    @Test
    public void importDirectoryIntoAMissingTable() throws Exception {
        writeRFile("data.rf", Map.of(new Key("row1", "cf", "cq", 3L), new Value("a")));

        assertThrows(TableNotFoundException.class, () -> tableOperations.importDirectory(dir.getAbsolutePath()).to("missingTable").load());
        assertTrue(new File(dir, "data.rf").exists());
    }

    @Test
    public void importDirectoryKeepsDeletesAndVisibilities() throws Exception {
        try (BatchWriter writer = client.createBatchWriter(TABLE)) {
            Mutation mutation = new Mutation("row1");
            mutation.put("cf", "cq", 3L, "old");
            writer.addMutation(mutation);
        }
        Key delete = new Key("row1", "cf", "cq", 4L);
        delete.setDeleted(true);
        Key secret = new Key("row2", "cf", "cq", "A&B", 4L);
        writeRFile("data.rf", Map.of(delete, new Value(), secret, new Value("secret")));

        tableOperations.importDirectory(dir.getAbsolutePath()).to(TABLE).load();

        // the imported delete hides the older entry, and the other entry is only visible with both authorizations
        assertEquals(Map.of(), scan(Authorizations.EMPTY));
        assertEquals(Map.of(secret, new Value("secret")), scan(new Authorizations("A", "B")));
    }

    @Test
    public void importDirectoryRequiresAnExistingDirectory() throws Exception {
        writeRFile("data.rf", Map.of(new Key("row1", "cf", "cq", 3L), new Value("a")));
        String file = new File(dir, "data.rf").getAbsolutePath();
        String missing = new File(dir, "missing").getAbsolutePath();

        assertThrows(AccumuloException.class, () -> tableOperations.importDirectory(file).to(TABLE).load());
        assertThrows(AccumuloException.class, () -> tableOperations.importDirectory(missing).to(TABLE).load());
    }

    @Test
    public void importDirectoryLoadsNothingWhenAFileCannotBeRead() throws Exception {
        writeRFile("good.rf", Map.of(new Key("row1", "cf", "cq", 3L), new Value("a")));
        Files.writeString(dir.toPath().resolve("bad.rf"), "not an rfile");

        assertThrows(IOException.class, () -> tableOperations.importDirectory(dir.getAbsolutePath()).to(TABLE).load());

        assertEquals(Map.of(), scan(Authorizations.EMPTY));
        assertTrue(new File(dir, "good.rf").exists());
        assertTrue(new File(dir, "bad.rf").exists());
    }

    @Test
    public void importDirectoryWithALoadPlan() throws Exception {
        Map<Key,Value> entries = Map.of(new Key("row1", "cf", "cq", 3L), new Value("a"));
        writeRFile("data.rf", entries);
        LoadPlan plan = LoadPlan.builder().loadFileTo("data.rf", RangeType.FILE, "row1", "row1").build();

        // the options BulkIngestMapFileLoader uses
        tableOperations.importDirectory(dir.getAbsolutePath()).to(TABLE).plan(plan).ignoreEmptyDir(true).tableTime(false).load();

        assertEquals(entries, scan(Authorizations.EMPTY));
    }

    private void writeRFile(String name, Map<Key,Value> entries) throws IOException {
        try (RFileWriter writer = RFile.newWriter().to(new File(dir, name).getAbsolutePath()).build()) {
            writer.startDefaultLocalityGroup();
            for (Entry<Key,Value> entry : new TreeMap<>(entries).entrySet()) {
                writer.append(entry.getKey(), entry.getValue());
            }
        }
    }

    private SortedMap<Key,Value> scan(Authorizations authorizations) throws TableNotFoundException {
        SortedMap<Key,Value> entries = new TreeMap<>();
        try (Scanner scanner = client.createScanner(TABLE, authorizations)) {
            for (Entry<Key,Value> entry : scanner) {
                entries.put(entry.getKey(), entry.getValue());
            }
        }
        return entries;
    }
}
