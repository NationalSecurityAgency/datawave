package datawave.metrics.analytic;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MetricsDataFormat {
    public static final int LIVE_LENGTH = 4;
    public static final int BULK_LENGTH = 6;
    
    public enum MetricsField {
        EVENT_COUNT("Event Count"),
        RAWFILE_TRANSFORM_DURATION("Raw File Transform Process"),
        INGEST_DELAY("Sequence File on HDFS"),
        INGEST_DURATION("Ingest Process"),
        LOADER_DELAY("R File on HDFS"),
        LOADER_DURATION("Loader Process");
        
        private String str;
        
        MetricsField(String str) {
            this.str = str;
        }
        
        @Override
        public String toString() {
            return str;
        }
    }
    
    public static final List<MetricsField> LiveFields;
    public static final List<MetricsField> BulkFields;
    
    static {
        List<MetricsField> fields = new ArrayList<>();
        for (int i = 0; i < LIVE_LENGTH; ++i) {
            fields.add(MetricsField.values()[i]);
        }
        fields.remove(MetricsField.EVENT_COUNT.ordinal()); // get rid of count
        LiveFields = Collections.unmodifiableList(fields);
        
        fields = new ArrayList<>();
        Collections.addAll(fields, MetricsField.values());
        fields.remove(MetricsField.EVENT_COUNT.ordinal());
        BulkFields = Collections.unmodifiableList(fields);
    }
}
