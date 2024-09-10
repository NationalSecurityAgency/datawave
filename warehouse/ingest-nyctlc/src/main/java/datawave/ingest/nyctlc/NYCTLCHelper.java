package datawave.ingest.nyctlc;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.ingest.data.config.CSVHelper;
import datawave.ingest.data.config.ConfigurationHelper;

/**
 * This is a specialized version of the CSV Helper intended to be used with the NYC Taxi &amp; Limousine Commission dataset. This class sets up some
 * configuration properties for the NYCTLC data type, in order to enable dynamic field parsing based on the fields present in the header of the CSV. While the
 * fields are validated against a list of known fields, unknown fields are allowed.
 */
public class NYCTLCHelper extends CSVHelper {

    /**
     * Parameter to specify that we should generate extra geometries in the ALL_LOCATIONS field for testing purposes.
     */
    public static final String GENERATE_EXTRA_GEOMETRIES = ".generate.extra.geometries";
    public static final String GENERATE_OVERLOADED_COMPOSITE = ".generate.overloaded.composite";

    private static final Logger log = LoggerFactory.getLogger(NYCTLCHelper.class);

    private static Set<String> KNOWN_FIELDS = new HashSet<>(Arrays.asList("VENDORID", "LPEP_PICKUP_DATETIME", "LPEP_DROPOFF_DATETIME", "STORE_AND_FWD_FLAG",
                    "RATECODEID", "PICKUP_LONGITUDE", "PICKUP_LATITUDE", "DROPOFF_LONGITUDE", "DROPOFF_LATITUDE", "PASSENGER_COUNT", "TRIP_DISTANCE",
                    "FARE_AMOUNT", "EXTRA", "MTA_TAX", "TIP_AMOUNT", "TOLLS_AMOUNT", "EHAIL_FEE", "IMPROVEMENT_SURCHARGE", "TOTAL_AMOUNT", "PAYMENT_TYPE",
                    "TRIP_TYPE"));

    private boolean generateExtraGeometries = false;
    private boolean generateOverloadedComposite = false;

    private String[] parsedHeader;

    @Override
    public void setup(Configuration config) throws IllegalArgumentException {
        final String type = ConfigurationHelper.isNull(config, Properties.DATA_NAME, String.class);
        if (type != null) {
            config.setBoolean(type + DATA_HEADER_ENABLED, false);
            config.set(type + DATA_SEP, ",");
            config.setBoolean(type + PROCESS_EXTRA_FIELDS, true);

            generateExtraGeometries = config.getBoolean(type + GENERATE_EXTRA_GEOMETRIES, false);
            generateOverloadedComposite = config.getBoolean(type + GENERATE_OVERLOADED_COMPOSITE, false);
        }
        super.setup(config);
    }

    public void parseHeader(String header) {
        parsedHeader = header.trim().toUpperCase().split(",");

        // validate the header fields and log any unknown fields
        Set<String> unknownFields = new HashSet<>();
        for (String parsedField : parsedHeader)
            if (!KNOWN_FIELDS.contains(parsedField))
                unknownFields.add(parsedField);

        log.debug("Header contained unknown fields: [" + String.join(",", unknownFields) + "]");
    }

    public String[] getParsedHeader() {
        return parsedHeader;
    }

    public boolean isGenerateExtraGeometries() {
        return generateExtraGeometries;
    }

    public boolean isGenerateOverloadedComposite() {
        return generateOverloadedComposite;
    }
}
