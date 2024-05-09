package datawave.ingest.mapreduce.job;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class LocalityGroupTableConfiguration implements LocalityGroupConfiguration, Configurable {
    private Configuration conf;
    private TableConfigurationUtil tcu;

    @Override
    public void setConf(Configuration configuration) {
        this.conf = configuration;
        this.tcu = new TableConfigurationUtil(configuration);
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public Set<String> getSupportedTables() {
        Set<String> candidateTables = TableConfigurationUtil.getJobOutputTableNames(conf);
        Set<String> tablesToInclude = getCommaSeparatedProperty(conf, TableConfigurationUtil.JOB_OUTPUT_LOCALITY_GROUP_TABLES_INCLUDE);
        Set<String> tablesToExclude = getCommaSeparatedProperty(conf, TableConfigurationUtil.JOB_OUTPUT_LOCALITY_GROUP_TABLES_INCLUDE);
        if (!tablesToInclude.isEmpty()) {
            // limit the candidates to what is in the inclusion set
            candidateTables.retainAll(tablesToInclude);
            // add in anyting that may be explicitly included, but not in the job tables
            // provides additional explicit override capability
            candidateTables.addAll(tablesToInclude);
        }
        if (!tablesToExclude.isEmpty()) {
            candidateTables.removeAll(tablesToExclude);
        }
        return candidateTables;
    }

    @Override
    public Map<String, Set<Text>> getLocalityGroups(String tableName) throws IOException {
        return tcu.getLocalityGroups(tableName);
    }

    private static Set<String> getCommaSeparatedProperty(Configuration conf, String propertyName) {
        String value = conf.get(propertyName);
        return value != null ? Stream.of(value.split(",")).collect(Collectors.toSet()) : Set.of();
    }
}

