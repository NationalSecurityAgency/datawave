package datawave.query.testframework;

import au.com.bytecode.opencsv.CSVReader;
import datawave.query.testframework.CitiesDataType.CityField;
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
public class CityDataManager extends AbstractDataManager {
    
    private static final Logger log = Logger.getLogger(CityDataManager.class);
    
    public CityDataManager() {
        super(CityField.EVENT_ID.name(), CityField.START_DATE.name(), CityField.getFieldsMetadata());
    }
    
    @Override
    public void addTestData(final URI file, final String datatype, final Set<String> indexes) throws IOException {
        Assert.assertFalse("datatype has already been configured(" + datatype + ")", this.rawData.containsKey(datatype));
        try (final Reader reader = Files.newBufferedReader(Paths.get(file)); final CSVReader csv = new CSVReader(reader, ',', '\"', '\0')) {
            String[] data;
            int count = 0;
            Set<RawData> cityData = new HashSet<>();
            while (null != (data = csv.readNext())) {
                final RawData raw = new CityRawData(datatype, data);
                cityData.add(raw);
                count++;
            }
            this.rawData.put(datatype, cityData);
            this.rawDataIndex.put(datatype, indexes);
            log.info("city test data(" + file + ") count(" + count + ")");
        }
    }
    
    @Override
    public List<String> getHeaders() {
        return CityField.headers();
    }
    
    private Set<RawData> getRawData(final Set<RawData> rawData, final Date start, final Date end) {
        final Set<RawData> data = new HashSet<>(this.rawData.size());
        final Set<String> shards = this.shardValues.getShardRange(start, end);
        for (final RawData raw : rawData) {
            String id = raw.getValue(CityField.START_DATE.name());
            if (shards.contains(id)) {
                data.add(raw);
            }
        }
        
        return data;
    }
    
    /**
     * POJO for a single raw data entry.
     */
    private static class CityRawData extends BaseRawData {
        CityRawData(final String datatype, final String fields[]) {
            super(datatype, fields, CityField.headers(), CityField.getFieldsMetadata());
            Assert.assertEquals("city ingest data field count is invalid", CityField.headers().size(), fields.length);
        }
    }
}
