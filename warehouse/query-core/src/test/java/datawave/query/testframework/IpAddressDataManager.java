package datawave.query.testframework;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.io.Reader;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import au.com.bytecode.opencsv.CSVReader;
import datawave.query.testframework.IpAddressDataType.IpAddrField;

/**
 * Data manager for IP address data.
 */
public class IpAddressDataManager extends AbstractDataManager {

    private static final Logger log = LoggerFactory.getLogger(IpAddressDataManager.class);
    private static IpAddressDataManager INSTANCE = new IpAddressDataManager();

    private IpAddressDataManager() {
        super(IpAddrField.EVENT_ID.name(), IpAddrField.START_DATE.name(), IpAddrField.getFieldsMetadata());
    }

    public static IpAddressDataManager getInstance() {
        return INSTANCE;
    }

    public static synchronized IpAddressDataManager newInstance() {
        INSTANCE = new IpAddressDataManager();
        return INSTANCE;
    }

    @Override
    public List<String> getHeaders() {
        return IpAddrField.headers();
    }

    @Override
    public void addTestData(URI file, String datatype, Set<String> indexes) throws IOException {
        assertFalse("datatype has already been configured(" + datatype + ")", this.rawData.containsKey(datatype));
        try (final Reader reader = Files.newBufferedReader(Paths.get(file)); final CSVReader csv = new CSVReader(reader)) {
            String[] data;
            int count = 0;
            Set<RawData> ipData = new HashSet<>();
            while (null != (data = csv.readNext())) {
                final RawData raw = new IpAddrRawData(datatype, data);
                ipData.add(raw);
                count++;
            }
            this.rawData.put(datatype, ipData);
            this.rawDataIndex.put(datatype, indexes);
            log.info("ip address test data(" + file + ") count(" + count + ")");
        }
    }

    static class IpAddrRawData extends BaseRawData {

        IpAddrRawData(final String datatype, final String fields[]) {
            super(datatype, fields, IpAddrField.headers(), IpAddrField.getFieldsMetadata());
            assertEquals("ingest data field count is invalid", IpAddrField.headers().size(), fields.length);
        }
    }
}
