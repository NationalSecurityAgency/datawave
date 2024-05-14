package datawave.ingest.mapreduce.job;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.Text;

public interface LocalityGroupConfiguration {
    Set<String> getSupportedTables() throws IOException;

    Map<String,Set<Text>> getLocalityGroups(String tableName) throws IOException;
}
