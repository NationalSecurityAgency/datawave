package datawave.ingest.nyctlc;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.config.CSVHelper;
import datawave.ingest.data.config.ConfigurationHelper;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.ingest.BaseIngestHelper;
import datawave.ingest.data.config.ingest.CSVIngestHelper;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

import static datawave.ingest.data.config.CSVHelper.DATA_HEADER_ENABLED;
import static datawave.ingest.data.config.CSVHelper.DATA_SEP;
import static datawave.ingest.data.config.CSVHelper.PROCESS_EXTRA_FIELDS;

/**
 * This is a specialized version of the CSV Ingest Helper intended to be used with the NYC Taxi & Limousine Commission dataset. This class is responsible for
 * creating derived geometry fields, created from the lat/lon pairings. This enables downstream creation of GeoWave indices against these fields.
 */
public class NYCTLCIngestHelper extends CSVIngestHelper {
    
    private static final Logger log = LoggerFactory.getLogger(NYCTLCIngestHelper.class);
    
    private static final String PICKUP_LATITUDE = "PICKUP_LATITUDE";
    private static final String PICKUP_LONGITUDE = "PICKUP_LONGITUDE";
    private static final String DROPOFF_LATITUDE = "DROPOFF_LATITUDE";
    private static final String DROPOFF_LONGITUDE = "DROPOFF_LONGITUDE";
    
    private static final String PICKUP_LOCATION = "PICKUP_LOCATION";
    private static final String DROPOFF_LOCATION = "DROPOFF_LOCATION";
    
    @Override
    public void setup(Configuration config) {
        final String type = ConfigurationHelper.isNull(config, Properties.DATA_NAME, String.class);
        if (type != null) {
            config.setBoolean(type + DATA_HEADER_ENABLED, false);
            config.set(type + DATA_SEP, ",");
            config.setBoolean(type + PROCESS_EXTRA_FIELDS, true);
        }
        super.setup(config);
    }
    
    @Override
    protected CSVHelper createHelper() {
        return new NYCTLCHelper();
    }
    
    @Override
    public Multimap<String,NormalizedContentInterface> getEventFields(RawRecordContainer event) {
        final Multimap<String,NormalizedContentInterface> eventFields = super.getEventFields(event);
        
        HashMultimap<String,String> derivedFields = HashMultimap.create();
        
        if (eventFields.containsKey(PICKUP_LATITUDE) && eventFields.containsKey(PICKUP_LONGITUDE)) {
            Collection<NormalizedContentInterface> latNci = eventFields.get(PICKUP_LATITUDE);
            Collection<NormalizedContentInterface> lonNci = eventFields.get(PICKUP_LONGITUDE);
            if (latNci.size() == 1 && lonNci.size() == 1) {
                String lat = latNci.stream().findAny().get().getEventFieldValue();
                String lon = lonNci.stream().findAny().get().getEventFieldValue();
                derivedFields.put(PICKUP_LOCATION, "POINT (" + lon + " " + lat + ")");
            } else
                log.warn("Did not expect multiple pickup lat/lon pairs in the event.");
        } else
            log.warn("Did not find any pickup lat/lon in the event.");
        
        if (eventFields.containsKey(DROPOFF_LATITUDE) && eventFields.containsKey(DROPOFF_LONGITUDE)) {
            Collection<NormalizedContentInterface> latNci = eventFields.get(DROPOFF_LATITUDE);
            Collection<NormalizedContentInterface> lonNci = eventFields.get(DROPOFF_LONGITUDE);
            if (latNci.size() == 1 && lonNci.size() == 1) {
                String lat = latNci.stream().findAny().get().getEventFieldValue();
                String lon = lonNci.stream().findAny().get().getEventFieldValue();
                derivedFields.put(DROPOFF_LOCATION, "POINT (" + lon + " " + lat + ")");
            } else
                log.warn("Did not expect multiple dropoff lat/lon pairs in the event.");
        } else
            log.warn("Did not find any dropoff lat/lon in the event.");
        
        eventFields.putAll(normalize(derivedFields));
        
        return eventFields;
    }
}
