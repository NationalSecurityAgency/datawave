package datawave.query.tables.ssdeep;

import au.com.bytecode.opencsv.CSVReader;
import datawave.query.tables.ssdeep.SSDeepDataType.SSDeepField;
import datawave.query.testframework.AbstractDataManager;
import datawave.query.testframework.BaseRawData;
import datawave.query.testframework.RawData;
import org.apache.log4j.Logger;
import org.junit.Assert;

import java.io.IOException;
import java.io.Reader;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Provides a mapping of the raw ingest data for query analysis. This will allow dynamic calculation of expected results and modification of the test data
 * without impacting the test cases. Each entry in the file will be transformed to a POJO entry.
 * <p>
 * </p>
 * This class is immutable.
 */
public class SSDeepDataManager extends AbstractDataManager {

    private static final Logger log = Logger.getLogger(SSDeepDataManager.class);

    public SSDeepDataManager() {
        super(SSDeepField.EVENT_ID.name(), SSDeepField.PROCESSING_DATE.name(), SSDeepField.getFieldsMetadata());
    }
    
    @Override
    public void addTestData(final URI file, final String datatype, final Set<String> indexes) throws IOException {
        Assert.assertFalse("datatype has already been configured(" + datatype + ")", this.rawData.containsKey(datatype));
        try (final Reader reader = Files.newBufferedReader(Paths.get(file)); final CSVReader csv = new CSVReader(reader, ',', '\"', '\0')) {
            String[] data;
            int count = 0;
            Set<RawData> ssdeepData = new HashSet<>();
            while (null != (data = csv.readNext())) {
                final RawData raw = new SSDeepRawData(datatype, data);
                ssdeepData.add(raw);
                count++;
            }
            this.rawData.put(datatype, ssdeepData);
            this.rawDataIndex.put(datatype, indexes);
            log.info("ssdeep test data(" + file + ") count(" + count + ")");
        }
    }
    
    @Override
    public List<String> getHeaders() {
        return SSDeepField.headers();
    }
    
    private Set<RawData> getRawData(final Set<RawData> rawData, final Date start, final Date end) {
        final Set<RawData> data = new HashSet<>(this.rawData.size());
        final Set<String> shards = this.shardValues.getShardRange(start, end);
        for (final RawData raw : rawData) {
            String id = raw.getValue(SSDeepField.PROCESSING_DATE.name());
            if (shards.contains(id)) {
                data.add(raw);
            }
        }
        
        return data;
    }
    
    /**
     * POJO for a single raw data entry.
     */
    private static class SSDeepRawData extends BaseRawData {
        SSDeepRawData(final String datatype, final String fields[]) {
            super(datatype, fields, SSDeepField.headers(), SSDeepField.getFieldsMetadata());
            //Assert.assertEquals("ssdeep ingest data field count is invalid", SSDeepField.headers().size(), fields.length);
        }
    }
}
