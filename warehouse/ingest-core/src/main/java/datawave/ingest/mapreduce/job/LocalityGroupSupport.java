package datawave.ingest.mapreduce.job;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.hadoop.io.Text;

public class LocalityGroupSupport {
    private Set<Text> tables;
    private Map<Text,ColumnFamilyToLocalityGroup> tableToColfLg;
    private Map<Text,Map<String,Set<ByteSequence>>> tableToLgColf;

    LocalityGroupSupport() {
        this.tables = new HashSet<>();
        this.tableToColfLg = new HashMap<>();
        this.tableToLgColf = new HashMap<>();
    }

    public Set<Text> getTables() {
        return tables;
    }

    public ColumnFamilyToLocalityGroup getColumnFamilyToLocalityGroup(Text table) {
        return tableToColfLg.get(table);
    }

    public Map<String,Set<ByteSequence>> getLocalityGroupToColumnFamily(Text table) {
        return tableToLgColf.get(table);
    }

    public static LocalityGroupSupport emptyLocalityGroupSupport() {
        return new LocalityGroupSupport();
    }

    public static LocalityGroupSupport.Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private LocalityGroupConfiguration lgConf;
        private String[] tableNames;

        public Builder withLocalityGroupConfiguration(LocalityGroupConfiguration lgConf) {
            this.lgConf = lgConf;
            return this;
        }

        public LocalityGroupSupport build() throws IOException {
            LocalityGroupSupport lgs = new LocalityGroupSupport();
            if (lgConf == null) {
                throw new IllegalArgumentException("A locality group configuration is required");
            }
            Set<String> supportedTables = lgConf.getSupportedTables();
            for (String tableName : supportedTables) {
                Map<String,Set<Text>> localityGroups = lgConf.getLocalityGroups(tableName);
                // pull the locality groups for this table.
                Map<Text,String> cftlg = new HashMap<>();
                Map<String,Set<ByteSequence>> lgtcf = new HashMap<>();
                for (Map.Entry<String,Set<Text>> locs : localityGroups.entrySet()) {
                    lgtcf.put(locs.getKey(), new HashSet<>());
                    for (Text loc : locs.getValue()) {
                        cftlg.put(loc, locs.getKey());
                        lgtcf.get(locs.getKey()).add(new ArrayByteSequence(loc.getBytes()));
                    }
                }
                lgs.tables.add(new Text(tableName));
                lgs.tableToColfLg.put(new Text(tableName), new ColumnFamilyToLocalityGroup(cftlg));
                lgs.tableToLgColf.put(new Text(tableName), lgtcf);
            }
            return lgs;
        }
    }

    public static class ColumnFamilyToLocalityGroup {
        private final Map<Text,String> colfToLg;
        private final int colfMaxLength;
        private final int colfMinLength;
        private final boolean empty;

        ColumnFamilyToLocalityGroup(Map<Text,String> colfToLg) {
            this.colfToLg = colfToLg;
            this.empty = colfToLg.isEmpty();
            this.colfMinLength = empty ? 0 : colfToLg.keySet().stream().mapToInt(Text::getLength).min().orElseThrow();
            this.colfMaxLength = empty ? 0 : colfToLg.keySet().stream().mapToInt(Text::getLength).max().orElseThrow();
        }

        public boolean isEmpty() {
            return empty;
        }

        public int columnFamilyMaxLength() {
            return colfMaxLength;
        }

        public int columnFamilyMinLength() {
            return colfMinLength;
        }

        public Map<Text,String> columnFamilyToLocalityGroup() {
            return colfToLg;
        }

        public Optional<String> lookupLocalityGroup(Text colf) {
            String lg = null;
            int colflen = colf.getLength();
            if (!colfToLg.isEmpty() && colflen >= colfMinLength && colflen <= colfMaxLength) {
                lg = colfToLg.get(colf);
            }
            return Optional.ofNullable(lg);
        }
    }
}
