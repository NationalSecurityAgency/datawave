package datawave.ingest.nyctlc;

import static datawave.ingest.data.config.CSVHelper.DATA_HEADER_ENABLED;
import static datawave.ingest.data.config.CSVHelper.DATA_SEP;
import static datawave.ingest.data.config.CSVHelper.PROCESS_EXTRA_FIELDS;

import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.util.GeometricShapeFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.config.CSVHelper;
import datawave.ingest.data.config.ConfigurationHelper;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.ingest.CSVIngestHelper;

/**
 * This is a specialized version of the CSV Ingest Helper intended to be used with the NYC Taxi &amp; Limousine Commission dataset. This class is responsible
 * for creating derived geometry fields, created from the lat/lon pairings. This enables downstream creation of GeoWave indices against these fields.
 */
public class NYCTLCIngestHelper extends CSVIngestHelper {

    private static final Logger log = LoggerFactory.getLogger(NYCTLCIngestHelper.class);

    private static final String PICKUP_LATITUDE = "PICKUP_LATITUDE";
    private static final String PICKUP_LONGITUDE = "PICKUP_LONGITUDE";
    private static final String DROPOFF_LATITUDE = "DROPOFF_LATITUDE";
    private static final String DROPOFF_LONGITUDE = "DROPOFF_LONGITUDE";
    private static final String TOTAL_AMOUNT = "TOTAL_AMOUNT";

    private static final String PICKUP_LOCATION = "PICKUP_LOCATION";
    private static final String DROPOFF_LOCATION = "DROPOFF_LOCATION";
    private static final String ALL_LOCATIONS = "ALL_LOCATIONS";
    private static final String ALL_LOCATIONS_OVERLOADED = "ALL_LOCATIONS_OVERLOADED";
    private static final String TOTAL_AMOUNT_INDEXED = "TOTAL_AMOUNT_INDEXED";
    private static final String ALL_POINTS = "ALL_POINTS";
    private static final String ALL_POINTS_GEO = "ALL_POINTS_GEO";

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

    private String getFirstEventFieldValue(Collection<NormalizedContentInterface> collection) {
        if (collection.stream().findFirst().isPresent()) {
            return collection.stream().findFirst().get().getEventFieldValue();
        } else {
            return null;
        }
    }

    @Override
    public Multimap<String,NormalizedContentInterface> getEventFields(RawRecordContainer event) {
        final Multimap<String,NormalizedContentInterface> eventFields = super.getEventFields(event);

        HashMultimap<String,String> derivedFields = HashMultimap.create();

        String pickupLat = null, pickupLon = null, dropoffLat = null, dropoffLon = null;

        if (eventFields.containsKey(PICKUP_LATITUDE) && eventFields.containsKey(PICKUP_LONGITUDE)) {
            Collection<NormalizedContentInterface> latNci = eventFields.get(PICKUP_LATITUDE);
            Collection<NormalizedContentInterface> lonNci = eventFields.get(PICKUP_LONGITUDE);
            if (latNci.size() == 1 && lonNci.size() == 1) {
                pickupLat = getFirstEventFieldValue(latNci);
                pickupLon = getFirstEventFieldValue(lonNci);
            } else
                log.warn("Did not expect multiple pickup lat/lon pairs in the event.");
        } else
            log.warn("Did not find any pickup lat/lon in the event.");

        if (eventFields.containsKey(DROPOFF_LATITUDE) && eventFields.containsKey(DROPOFF_LONGITUDE)) {
            Collection<NormalizedContentInterface> latNci = eventFields.get(DROPOFF_LATITUDE);
            Collection<NormalizedContentInterface> lonNci = eventFields.get(DROPOFF_LONGITUDE);
            if (latNci.size() == 1 && lonNci.size() == 1) {
                dropoffLat = getFirstEventFieldValue(latNci);
                dropoffLon = getFirstEventFieldValue(lonNci);
            } else
                log.warn("Did not expect multiple dropoff lat/lon pairs in the event.");
        } else
            log.warn("Did not find any dropoff lat/lon in the event.");

        if (pickupLat != null && pickupLon != null) {
            derivedFields.put(PICKUP_LOCATION, "POINT (" + pickupLon + " " + pickupLat + ")");
            derivedFields.put(ALL_LOCATIONS, "POINT (" + pickupLon + " " + pickupLat + ")");
        }

        if (dropoffLat != null && dropoffLon != null) {
            derivedFields.put(DROPOFF_LOCATION, "POINT (" + dropoffLon + " " + dropoffLat + ")");
            derivedFields.put(ALL_LOCATIONS, "POINT (" + dropoffLon + " " + dropoffLat + ")");
        }

        // create some extra geos for testing purposes
        if (helper instanceof NYCTLCHelper && ((NYCTLCHelper) helper).isGenerateExtraGeometries()) {
            if (pickupLat != null && pickupLon != null && dropoffLat != null && dropoffLon != null) {
                double pickupLonDouble = Double.parseDouble(pickupLon), pickupLatDouble = Double.parseDouble(pickupLat),
                                dropoffLonDouble = Double.parseDouble(dropoffLon), dropoffLatDouble = Double.parseDouble(dropoffLat);
                double tripDistance = distance(pickupLonDouble, pickupLatDouble, dropoffLonDouble, dropoffLatDouble);
                derivedFields.put(ALL_LOCATIONS, createCircle(pickupLonDouble, pickupLatDouble, tripDistance / 2.0).toText());
                derivedFields.put(ALL_LOCATIONS, createCircle(dropoffLonDouble, dropoffLatDouble, tripDistance / 2.0).toText());

                double minLon = Math.min(pickupLonDouble, dropoffLonDouble);
                double minLat = Math.min(pickupLatDouble, dropoffLatDouble);
                double maxLon = Math.max(pickupLonDouble, dropoffLonDouble);
                double maxLat = Math.max(pickupLatDouble, dropoffLatDouble);
                derivedFields.put(ALL_LOCATIONS, createCircle(minLon + (maxLon - minLon), minLat + (maxLat - minLat), tripDistance / 2.0).toText());
            }
        }

        // create an overloaded composite field for testing purposes
        if (helper instanceof NYCTLCHelper && ((NYCTLCHelper) helper).isGenerateOverloadedComposite()) {
            if (pickupLat != null && pickupLon != null && dropoffLat != null && dropoffLon != null) {
                derivedFields.put(ALL_LOCATIONS, "POINT (" + pickupLon + " " + pickupLat + ")");
                derivedFields.put(ALL_LOCATIONS, "POINT (" + dropoffLon + " " + dropoffLat + ")");

                double pickupLonDouble = Double.parseDouble(pickupLon), pickupLatDouble = Double.parseDouble(pickupLat),
                                dropoffLonDouble = Double.parseDouble(dropoffLon), dropoffLatDouble = Double.parseDouble(dropoffLat);
                double tripDistance = distance(pickupLonDouble, pickupLatDouble, dropoffLonDouble, dropoffLatDouble);
                derivedFields.put(ALL_LOCATIONS_OVERLOADED, createCircle(pickupLonDouble, pickupLatDouble, tripDistance / 2.0).toText());
                derivedFields.put(ALL_LOCATIONS_OVERLOADED, createCircle(dropoffLonDouble, dropoffLatDouble, tripDistance / 2.0).toText());

                double minLon = Math.min(pickupLonDouble, dropoffLonDouble);
                double minLat = Math.min(pickupLatDouble, dropoffLatDouble);
                double maxLon = Math.max(pickupLonDouble, dropoffLonDouble);
                double maxLat = Math.max(pickupLatDouble, dropoffLatDouble);
                derivedFields.put(ALL_LOCATIONS_OVERLOADED, createCircle(minLon + (maxLon - minLon), minLat + (maxLat - minLat), tripDistance / 2.0).toText());
            }
        }

        // add an indexed version of TOTAL_AMOUNT for testing purposes
        if (eventFields.containsKey("TOTAL_AMOUNT")) {
            Collection<NormalizedContentInterface> totalAmountNci = eventFields.get(TOTAL_AMOUNT);
            if (totalAmountNci.size() == 1) {
                derivedFields.put(TOTAL_AMOUNT_INDEXED, getFirstEventFieldValue(totalAmountNci));
            } else
                log.warn("Did not expect multiple TOTAL_AMOUNT values in the event.");
        }

        // add point query fields
        derivedFields.put(ALL_POINTS, "POINT (" + pickupLon + " " + pickupLat + ")");
        derivedFields.put(ALL_POINTS, "POINT (" + dropoffLon + " " + dropoffLat + ")");
        derivedFields.put(ALL_POINTS_GEO, pickupLat + " " + pickupLon);
        derivedFields.put(ALL_POINTS_GEO, dropoffLat + " " + dropoffLon);

        eventFields.putAll(normalize(derivedFields));

        return eventFields;
    }

    private static Geometry createCircle(double x, double y, final double RADIUS) {
        GeometricShapeFactory shapeFactory = new GeometricShapeFactory();
        shapeFactory.setNumPoints(32);
        shapeFactory.setCentre(new Coordinate(x, y));
        shapeFactory.setSize(RADIUS * 2);
        return shapeFactory.createCircle();
    }

    private static double distance(double x1, double y1, double x2, double y2) {
        return Math.sqrt(Math.pow(x2 - x1, 2) + Math.pow(y2 - y1, 2));
    }
}
