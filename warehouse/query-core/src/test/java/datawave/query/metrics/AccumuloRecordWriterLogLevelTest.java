package datawave.query.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class AccumuloRecordWriterLogLevelTest {

    @ParameterizedTest
    @CsvSource({"ALL, -2147483648", "TRACE, 5000", "DEBUG, 10000", "INFO, 20000", "WARN, 30000", "ERROR, 40000", "FATAL, 50000", "OFF, 2147483647"})
    void preservesLogLevelsInExistingHadoopConfigurations(String name, int encoding) {
        Configuration conf = new Configuration(false);
        Level level = Level.valueOf(name);
        String key = "AccumuloRecordWriter.loglevel";

        conf.setInt(key, encoding);
        assertEquals(level, AccumuloRecordWriter.getLogLevel(conf));

        AccumuloRecordWriter.setLogLevel(conf, level);
        assertEquals(encoding, conf.getInt(key, 0));
        assertEquals(level, AccumuloRecordWriter.getLogLevel(conf));
    }

    @Test
    void preservesDefaultsForUnsetAndUnknownLevels() {
        Configuration conf = new Configuration(false);
        assertNull(AccumuloRecordWriter.getLogLevel(conf));

        conf.setInt("AccumuloRecordWriter.loglevel", 12345);
        assertEquals(Level.INFO, AccumuloRecordWriter.getLogLevel(conf));
    }
}
