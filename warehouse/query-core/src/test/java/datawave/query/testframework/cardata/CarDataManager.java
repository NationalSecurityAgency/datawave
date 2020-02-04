package datawave.query.testframework.cardata;

import au.com.bytecode.opencsv.CSVReader;
import datawave.query.testframework.AbstractDataManager;
import datawave.query.testframework.BaseRawData;
import datawave.query.testframework.RawData;
import datawave.query.testframework.cardata.CarsDataType.CarField;
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

public class CarDataManager extends AbstractDataManager {
    
    private static final Logger log = Logger.getLogger(CarDataManager.class);
    
    public CarDataManager() {
        super(CarField.EVENT_ID.name(), CarField.START_DATE.name(), CarField.getFieldsMetadata());
    }
    
    @Override
    public void addTestData(final URI file, final String datatype, final Set<String> indexes) throws IOException {
        Assert.assertFalse("datatype has already been configured(" + datatype + ")", this.rawData.containsKey(datatype));
        try (final Reader reader = Files.newBufferedReader(Paths.get(file)); final CSVReader csv = new CSVReader(reader)) {
            String[] data;
            int count = 0;
            Set<RawData> carData = new HashSet<>();
            while (null != (data = csv.readNext())) {
                final RawData raw = new CarDataManager.CarRawData(datatype, data);
                carData.add(raw);
                count++;
            }
            this.rawData.put(datatype, carData);
            this.rawDataIndex.put(datatype, indexes);
            log.info("car test data(" + file + ") count(" + count + ")");
        }
    }
    
    @Override
    public List<String> getHeaders() {
        return CarField.headers();
    }
    
    private Set<RawData> getRawData(final Set<RawData> rawData, final Date start, final Date end) {
        final Set<RawData> data = new HashSet<>(this.rawData.size());
        final Set<String> shards = this.shardValues.getShardRange(start, end);
        for (final RawData raw : rawData) {
            String id = raw.getValue(CarField.START_DATE.name());
            if (shards.contains(id)) {
                data.add(raw);
            }
        }
        
        return data;
    }
    
    static class CarRawData extends BaseRawData {
        
        CarRawData(final String datatype, final String fields[]) {
            super(datatype, fields, CarField.headers(), CarField.getFieldsMetadata());
            Assert.assertEquals("car ingest data field count is invalid", CarField.headers().size(), fields.length);
        }
    }
}
