package datawave.ingest.mapreduce.job;

import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

public interface LocalityGroupConfiguration {
    Set<String> getSupportedTables() throws IOException;
    Map<String,Set<Text>> getLocalityGroups(String tableName) throws IOException;
}
