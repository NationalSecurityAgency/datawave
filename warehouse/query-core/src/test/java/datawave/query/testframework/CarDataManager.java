package datawave.query.testframework;

import au.com.bytecode.opencsv.CSVReader;
import datawave.data.normalizer.Normalizer;
import datawave.query.testframework.cardata.CarsDataType;
import org.apache.log4j.Logger;
import org.junit.Assert;

import java.io.IOException;
import java.io.Reader;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class CarDataManager extends AbstractDataManager {
    
    private static final Logger log = Logger.getLogger(CarDataManager.class);
    
    public CarDataManager() {
        super(CarsDataType.CarField.EVENT_ID.name(), CarsDataType.CarField.START_DATE.name());
        this.metadata = CarRawData.metadata;
    }
    
    @Override
    public void addTestData(final URI file, final String datatype, final Set<String> indexes) throws IOException {
        Assert.assertFalse("datatype has already been configured(" + datatype + ")", this.rawData.containsKey(datatype));
        try (final Reader reader = Files.newBufferedReader(Paths.get(file)); final CSVReader csv = new CSVReader(reader)) {
            String[] data;
            int count = 0;
            Set<RawData> carData = new HashSet<>();
            while (null != (data = csv.readNext())) {
                final RawData raw = new CarDataManager.CarRawData(data);
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
        return CarsDataType.CarField.headers();
    }
    
    @Override
    public Date[] getRandomStartEndDate() {
        return CarsDataType.CarShardId.getStartEndDates(true);
    }
    
    @Override
    public Date[] getShardStartEndDate() {
        return CarsDataType.CarShardId.getStartEndDates(false);
    }
    
    private Set<RawData> getRawData(final Set<RawData> rawData, final Date start, final Date end) {
        final Set<RawData> data = new HashSet<>(this.rawData.size());
        final Set<String> shards = CarsDataType.CarShardId.getShardRange(start, end);
        for (final RawData raw : rawData) {
            String id = raw.getValue(CarsDataType.CarField.START_DATE.name());
            if (shards.contains(id)) {
                data.add(raw);
            }
        }
        
        return data;
    }
    
    static class CarRawData extends BaseRawData {
        
        private static final Map<String,RawMetaData> metadata = new HashMap<>();
        static {
            for (final CarsDataType.CarField field : CarsDataType.CarField.values()) {
                metadata.put(field.name().toLowerCase(), field.getMetadata());
            }
        }
        
        CarRawData(final String fields[]) {
            super(fields);
            Assert.assertEquals("car ingest data field count is invalid", CarsDataType.CarField.headers().size(), fields.length);
        }
        
        @Override
        protected List<String> getHeaders() {
            return CarsDataType.CarField.headers();
        }
        
        @Override
        protected boolean containsField(final String field) {
            return CarsDataType.CarField.headers().contains(field.toLowerCase());
        }
        
        @Override
        protected Normalizer<?> getNormalizer(String field) {
            return metadata.get(field.toLowerCase()).normalizer;
        }
        
        @Override
        public boolean isMultiValueField(final String field) {
            return metadata.get(field.toLowerCase()).multiValue;
        }
    }
}
