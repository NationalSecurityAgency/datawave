package datawave.util.flag;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;

/**
 * Reads a FlagMetrics file from a given Path and verify its contents
 */
public class FlagMetricsFileVerification {
    private static final List<String> EXPECTED_GROUP_NAMES = Arrays.asList("FlagFile", "InputFile", "datawave.metrics.util.flag.InputFile");

    private final Counters counters;
    private final Text name;
    private final FlagFileTestSetup flagFileTestSetup;

    FlagMetricsFileVerification(Path metricsFilePath, FlagFileTestSetup flagFileTestSetup) throws IOException {
        SequenceFile.Reader.Option readerOptions = SequenceFile.Reader.file(metricsFilePath);
        SequenceFile.Reader reader = new SequenceFile.Reader(new Configuration(), readerOptions);
        this.name = new Text();
        this.counters = new Counters();
        reader.next(name, counters);

        this.flagFileTestSetup = flagFileTestSetup;
    }

    public void assertGroupNames() {
        assertTrue(Objects.equals(EXPECTED_GROUP_NAMES, getCounterGroups()));
    }

    // Counter Group "FlagFile" contains InputFile names and time-of-flagging timestamps for each
    public void assertCountersForFilesShowingFlagTimes(long startTime, long stopTime) {
        CounterGroup group = this.counters.getGroup("FlagFile");

        List<String> actualFileNames = StreamSupport.stream(group.spliterator(), false).map(Counter::getName).collect(Collectors.toList());
        Collection<String> expectedFileNames = flagFileTestSetup.getNamesOfCreatedFiles();
        assertTrue(expectedFileNames.containsAll(actualFileNames));
        assertTrue(actualFileNames.containsAll(expectedFileNames));

        List<Long> actualCurrentTimes = StreamSupport.stream(group.spliterator(), false).map(Counter::getValue).collect(Collectors.toList());
        for (Long actualTime : actualCurrentTimes) {
            assertTrue(startTime <= actualTime);
            assertTrue(stopTime >= actualTime);
        }
    }

    // Counter Group "InputFile" contains InputFile names and lastModified timestamps for each
    public void assertCountersForInputFileLastModified(Collection<String> expectedFileNames) {
        CounterGroup group = this.counters.getGroup("InputFile");

        List<String> actualFileNames = StreamSupport.stream(group.spliterator(), false).map(Counter::getName).collect(Collectors.toList());
        assertTrue(expectedFileNames.containsAll(actualFileNames));
        assertTrue(actualFileNames.containsAll(expectedFileNames));

        List<Long> actualCurrentTimes = StreamSupport.stream(group.spliterator(), false).map(Counter::getValue).collect(Collectors.toList());
        Collection<Long> expectedTimes = flagFileTestSetup.getLastModifiedTimes();
        assertTrue(expectedTimes.containsAll(actualCurrentTimes));
        assertTrue(actualCurrentTimes.containsAll(expectedTimes));
    }

    // Counter Group "datawave.metrics.util.flag.InputFile" contains Flag Maker start and stop times
    public void assertFlagMakerStartStopTimesInExpectedRange(long testSubjectExecutionStartTime, long testSubjectExecutionStopTime) {
        CounterGroup group = this.counters.getGroup("datawave.metrics.util.flag.InputFile");

        long counterStartTime = group.findCounter("FLAGMAKER_START_TIME").getValue();
        long counterStopTime = group.findCounter("FLAGMAKER_END_TIME").getValue();
        assertTrue(testSubjectExecutionStartTime < counterStartTime);
        assertTrue(counterStartTime < counterStopTime);
        assertTrue(testSubjectExecutionStopTime > counterStopTime);
    }

    public String getName() {
        return this.name.toString();
    }

    public Collection<String> getCounterGroups() {
        List<String> actual = new ArrayList<>();
        this.counters.getGroupNames().forEach(actual::add);
        return actual;
    }
}
