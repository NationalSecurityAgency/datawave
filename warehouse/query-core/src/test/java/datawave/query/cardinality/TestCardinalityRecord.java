package datawave.query.cardinality;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import org.locationtech.jts.util.Assert;

public class TestCardinalityRecord {

    @Test
    public void testCartesianProductOfFields1() {

        Set<String> recordedFields = new HashSet<>();
        CardinalityRecord cr = new CardinalityRecord(recordedFields, CardinalityRecord.DateType.DOCUMENT);

        Map<String,List<String>> valueMap = new HashMap<>();
        List<String> results = cr.assembleValues("FIELD1", valueMap);

        Assert.equals(0, results.size());
    }

    @Test
    public void testCartesianProductOfFields2() {

        Set<String> recordedFields = new HashSet<>();
        CardinalityRecord cr = new CardinalityRecord(recordedFields, CardinalityRecord.DateType.DOCUMENT);

        Map<String,List<String>> valueMap = new HashMap<>();
        List<String> list1 = new ArrayList<>();
        list1.add("L1V1");
        list1.add("L1V2");
        list1.add("L1V3");
        list1.add("L1V4");
        valueMap.put("FIELD1", list1);
        List<String> results = cr.assembleValues("FIELD1", valueMap);

        int expectedSize = list1.size();
        Assert.equals(expectedSize, results.size());
    }

    @Test
    public void testCartesianProductOfFields3() {

        Set<String> recordedFields = new HashSet<>();
        CardinalityRecord cr = new CardinalityRecord(recordedFields, CardinalityRecord.DateType.DOCUMENT);

        Map<String,List<String>> valueMap = new HashMap<>();
        List<String> list1 = new ArrayList<>();
        list1.add("L1V1");
        list1.add("L1V2");
        list1.add("L1V3");
        list1.add("L1V4");
        valueMap.put("FIELD1", list1);
        List<String> list2 = new ArrayList<>();
        list2.add("L2V1");
        list2.add("L2V2");
        list2.add("L2V3");
        list2.add("L2V4");
        valueMap.put("FIELD2", list2);
        List<String> results = cr.assembleValues("FIELD1|FIELD2", valueMap);

        int expectedSize = list1.size() * list2.size();
        Assert.equals(expectedSize, results.size());
    }

    @Test
    public void testCartesianProductOfFields4() {

        Set<String> recordedFields = new HashSet<>();
        CardinalityRecord cr = new CardinalityRecord(recordedFields, CardinalityRecord.DateType.DOCUMENT);

        Map<String,List<String>> valueMap = new HashMap<>();
        List<String> list1 = new ArrayList<>();
        list1.add("L1V1");
        list1.add("L1V2");
        list1.add("L1V3");
        list1.add("L1V4");
        valueMap.put("FIELD1", list1);
        List<String> list2 = new ArrayList<>();
        list2.add("L2V1");
        list2.add("L2V2");
        list2.add("L2V3");
        list2.add("L2V4");
        valueMap.put("FIELD2", list2);
        List<String> list3 = new ArrayList<>();
        list3.add("L3V1");
        list3.add("L3V2");
        valueMap.put("FIELD3", list3);
        List<String> list4 = new ArrayList<>();
        list4.add("L4V1");
        list4.add("L4V2");
        list4.add("L4V3");
        list4.add("L4V4");
        list4.add("L4V5");
        valueMap.put("FIELD4", list4);
        List<String> list5 = new ArrayList<>();
        list5.add("L5V1");
        list5.add("L5V2");
        list5.add("L5V3");
        list5.add("L5V4");
        valueMap.put("FIELD5", list5);
        List<String> results = cr.assembleValues("FIELD1|FIELD2|FIELD3|FIELD4|FIELD5", valueMap);

        int expectedSize = list1.size() * list2.size() * list3.size() * list4.size() * list5.size();
        Assert.equals(expectedSize, results.size());
    }

    /**
     * The asynchronous write used to call {@link Object#notifyAll()} on the target file while holding an unrelated monitor, which raised an
     * {@link IllegalMonitorStateException} on the executor thread after every write.
     */
    @Test
    public void testWriteToDiskDoesNotThrowOnTheExecutorThread() throws Exception {

        Set<String> recordedFields = new HashSet<>();
        recordedFields.add("FIELD1");
        CardinalityRecord cr = new CardinalityRecord(recordedFields, CardinalityRecord.DateType.DOCUMENT);

        Map<String,List<String>> valueMap = new HashMap<>();
        valueMap.put("FIELD1", Collections.singletonList("L1V1"));
        cr.addEntry(valueMap, "eventId", "datatype", "20260729");

        Path directory = Files.createTempDirectory("cardinality-record-test");
        File file = new File(directory.toFile(), "cardinality.obj");

        AtomicReference<Throwable> uncaught = new AtomicReference<>();
        CountDownLatch uncaughtLatch = new CountDownLatch(1);
        Thread.UncaughtExceptionHandler previousHandler = Thread.getDefaultUncaughtExceptionHandler();
        Thread.setDefaultUncaughtExceptionHandler((thread, throwable) -> {
            uncaught.set(throwable);
            uncaughtLatch.countDown();
        });

        try {
            CardinalityRecord.writeToDisk(cr, file);

            // the write is asynchronous, so wait for the record to land before checking whether it also failed
            Clock clock = Clock.systemUTC();
            long deadline = clock.millis() + TimeUnit.SECONDS.toMillis(10);
            CardinalityRecord written = null;
            while (written == null && clock.millis() < deadline) {
                Thread.sleep(50);
                written = CardinalityRecord.readFromDisk(file);
            }
            assertNotNull("record was never written to disk", written);

            // the monitor violation is raised immediately after the stream is closed, so give it a chance to surface
            uncaughtLatch.await(500, TimeUnit.MILLISECONDS);
            assertNull("writeToDisk failed on the executor thread", uncaught.get());
        } finally {
            Thread.setDefaultUncaughtExceptionHandler(previousHandler);
            file.delete();
            directory.toFile().delete();
        }
    }
}
