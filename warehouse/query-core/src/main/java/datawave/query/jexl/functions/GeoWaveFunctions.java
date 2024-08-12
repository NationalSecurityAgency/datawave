package datawave.query.jexl.functions;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;

import datawave.data.normalizer.AbstractGeometryNormalizer;
import datawave.data.normalizer.GeoNormalizer;
import datawave.data.type.AbstractGeometryType;
import datawave.data.type.GeoType;
import datawave.data.type.util.AbstractGeometry;
import datawave.query.attributes.ValueTuple;
import datawave.query.collections.FunctionalSet;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;

/**
 * Provides functions for doing spatial queries, such as bounding boxes and circles of interest, as well as spatial relationships.
 *
 * Function names are all lower case and separated by underscores to play nice with case insensitive queries.
 *
 * NOTE: The JexlFunctionArgumentDescriptorFactory is implemented by GeoWaveFunctionsDescripter. This is kept as a separate class to reduce accumulo
 * dependencies on other jars.
 *
 */
@JexlFunctions(descriptorFactory = "datawave.query.jexl.functions.GeoWaveFunctionsDescriptor")
public class GeoWaveFunctions {
    public static final String GEOWAVE_FUNCTION_NAMESPACE = "geowave";

    // used to handle legacy geo data type
    private static final GeoNormalizer geoNormalizer = new GeoNormalizer();
    private static final GeometryFactory geometryFactory = new GeometryFactory();

    /**
     * Test intersection of a set of geometry with a field value
     *
     * @param fieldValue
     *            the field value
     * @param geometries
     *            group of geometries
     * @return true if there is an intersection, false otherwise
     */
    private static boolean intersectsGeometries(Object fieldValue, Geometry[] geometries) {
        if (fieldValue != null) {
            Geometry thisGeom = getGeometryFromFieldValue(fieldValue);
            for (Geometry g : geometries) {
                if (thisGeom.intersects(g)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean intersectsGeometries(Iterable<?> values, Geometry[] geometries) {
        if (values != null) {
            boolean successfullyParsedAValue = false;
            Exception parseException = null;
            for (Object fieldValue : values) {
                try {
                    if (intersectsGeometries(fieldValue, geometries)) {
                        return true;
                    }
                    successfullyParsedAValue = true;
                } catch (Exception e) {
                    // this is most likely a field value from the index (i.e. an encoded string @see GeometryNormalizer.getEncodedStringsFromGeometry)
                    // ignore and continue down the list of values. This will be thrown if every value in the list threw an exception
                    parseException = e;
                }
            }
            if (!successfullyParsedAValue) {
                BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.UNPARSEABLE_JEXL_QUERY,
                                "Did not find any properly encoded values to match against. " + parseException);
                throw new RuntimeException(qe);
            }
        }
        return false;
    }

    private static Geometry getGeometryFromFieldValue(Object fieldValue) {
        Geometry geometry = null;
        if (fieldValue instanceof Geometry) {
            geometry = (Geometry) fieldValue;
        } else if (fieldValue instanceof GeoNormalizer.GeoPoint) {
            geometry = geoPointToGeometry((GeoNormalizer.GeoPoint) fieldValue);
        } else if (fieldValue instanceof String) {
            geometry = parseGeometry((String) fieldValue);
        } else if (fieldValue instanceof ValueTuple) {
            ValueTuple t = (ValueTuple) fieldValue;
            Object o = t.second();
            if (o instanceof AbstractGeometryType) {
                AbstractGeometryType<?> gt = (AbstractGeometryType<?>) o;
                geometry = ((AbstractGeometry<?>) gt.getDelegate()).getJTSGeometry();
            } else if (o instanceof GeoType) {
                geometry = parseGeometryFromGeoType(ValueTuple.getNormalizedStringValue(fieldValue));
            }
        } else if (fieldValue instanceof FunctionalSet) {
            FunctionalSet<?> funcSet = (FunctionalSet<?>) fieldValue;
            Geometry[] geometries = funcSet.stream().map(GeoWaveFunctions::getGeometryFromFieldValue).toArray(Geometry[]::new);
            geometry = new GeometryCollection(geometries, geometryFactory);
        }

        if (geometry == null) {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.UNPARSEABLE_JEXL_QUERY,
                            "Field Value:" + fieldValue + " cannot be recognized as a geometry");
            throw new IllegalArgumentException(qe);
        }

        return geometry;
    }

    private static Geometry parseGeometry(String geomString) {
        Geometry geom;
        try {
            geom = AbstractGeometryNormalizer.parseGeometry(geomString);
        } catch (IllegalArgumentException e) {
            geom = parseGeometryFromGeoType(geomString);
        }

        if (geom == null) {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.UNPARSEABLE_JEXL_QUERY,
                            "Geom string:" + geomString + " cannot be recognized as a geometry");
            throw new IllegalArgumentException(qe);
        }

        return geom;
    }

    private static Geometry parseGeometryFromGeoType(String geoTypeString) {
        Geometry geom = null;
        try {
            geom = geoPointToGeometry(GeoNormalizer.isNormalized(geoTypeString) ? GeoNormalizer.GeoPoint.decodeZRef(geoTypeString)
                            : GeoNormalizer.GeoPoint.decodeZRef(geoNormalizer.normalize(geoTypeString)));
        } catch (Exception e) {
            // do nothing
        }
        return geom;
    }

    private static Geometry geoPointToGeometry(GeoNormalizer.GeoPoint geoPoint) {
        return geometryFactory.createPoint(new Coordinate(geoPoint.getLongitude(), geoPoint.getLatitude()));
    }

    public static boolean contains(Object fieldValue, String geoString) {
        if (fieldValue != null) {
            Geometry otherGeom = AbstractGeometryNormalizer.parseGeometry(geoString);
            Geometry thisGeom = getGeometryFromFieldValue(fieldValue);
            return thisGeom.contains(otherGeom);
        } else {
            return false;
        }
    }

    public static boolean contains(Iterable<?> values, String geoString) {
        if (values != null) {
            boolean successfullyParsedAValue = false;
            Exception parseException = null;
            for (Object fieldValue : values) {
                try {
                    if (contains(fieldValue, geoString)) {
                        return true;
                    }
                    successfullyParsedAValue = true;
                } catch (Exception e) {
                    // this is most likely a field value from the index (i.e. an encoded string @see GeometryNormalizer.getEncodedStringsFromGeometry)
                    // ignore and continue down the list of values. This will be thrown if every value in the list threw an exception
                    parseException = e;
                }
            }
            if (!successfullyParsedAValue) {
                BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.UNPARSEABLE_JEXL_QUERY, parseException,
                                "Did not find any properly encoded values to match against");
                throw new RuntimeException(qe);
            }
        }
        return false;
    }

    public static boolean covers(Object fieldValue, String geoString) {
        if (fieldValue != null) {
            Geometry otherGeom = AbstractGeometryNormalizer.parseGeometry(geoString);
            Geometry thisGeom = getGeometryFromFieldValue(fieldValue);
            return thisGeom.covers(otherGeom);
        } else {
            return false;
        }
    }

    public static boolean covers(Iterable<?> values, String geoString) {
        if (values != null) {
            boolean successfullyParsedAValue = false;
            Exception parseException = null;
            for (Object fieldValue : values) {
                try {
                    if (covers(fieldValue, geoString)) {
                        return true;
                    }
                    successfullyParsedAValue = true;
                } catch (Exception e) {
                    // this is most likely a field value from the index (i.e. an encoded string @see GeometryNormalizer.getEncodedStringsFromGeometry)
                    // ignore and continue down the list of values. This will be thrown if every value in the list threw an exception
                    parseException = e;
                }
            }
            if (!successfullyParsedAValue) {
                BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.UNPARSEABLE_JEXL_QUERY, parseException,
                                "Did not find any properly encoded values to match against");
                throw new RuntimeException(qe);
            }
        }
        return false;
    }

    public static boolean covered_by(Object fieldValue, String geoString) {
        if (fieldValue != null) {
            Geometry otherGeom = AbstractGeometryNormalizer.parseGeometry(geoString);
            Geometry thisGeom = getGeometryFromFieldValue(fieldValue);
            return thisGeom.coveredBy(otherGeom);
        } else {
            return false;
        }
    }

    public static boolean covered_by(Iterable<?> values, String geoString) {
        if (values != null) {
            boolean successfullyParsedAValue = false;
            Exception parseException = null;
            for (Object fieldValue : values) {
                try {
                    if (covered_by(fieldValue, geoString)) {
                        return true;
                    }
                    successfullyParsedAValue = true;
                } catch (Exception e) {
                    // this is most likely a field value from the index (i.e. an encoded string @see GeometryNormalizer.getEncodedStringsFromGeometry)
                    // ignore and continue down the list of values. This will be thrown if every value in the list threw an exception
                    parseException = e;
                }
            }
            if (!successfullyParsedAValue) {
                BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.UNPARSEABLE_JEXL_QUERY, parseException,
                                "Did not find any properly encoded values to match against");
                throw new RuntimeException(qe);
            }
        }
        return false;
    }

    public static boolean crosses(Object fieldValue, String geoString) {
        if (fieldValue != null) {
            Geometry otherGeom = AbstractGeometryNormalizer.parseGeometry(geoString);
            Geometry thisGeom = getGeometryFromFieldValue(fieldValue);
            return thisGeom.crosses(otherGeom);
        } else {
            return false;
        }
    }

    public static boolean crosses(Iterable<?> values, String geoString) {
        if (values != null) {
            boolean successfullyParsedAValue = false;
            Exception parseException = null;
            for (Object fieldValue : values) {
                try {
                    if (crosses(fieldValue, geoString)) {
                        return true;
                    }
                    successfullyParsedAValue = true;
                } catch (Exception e) {
                    // this is most likely a field value from the index (i.e. an encoded string @see GeometryNormalizer.getEncodedStringsFromGeometry)
                    // ignore and continue down the list of values. This will be thrown if every value in the list threw an exception
                    parseException = e;
                }
            }
            if (!successfullyParsedAValue) {
                BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.UNPARSEABLE_JEXL_QUERY, parseException,
                                "Did not find any properly encoded values to match against");
                throw new RuntimeException(qe);
            }
        }
        return false;
    }

    public static boolean intersects(Object fieldValue, String geoString) {
        if (fieldValue != null) {
            Geometry otherGeom = AbstractGeometryNormalizer.parseGeometry(geoString);
            Geometry thisGeom = getGeometryFromFieldValue(fieldValue);
            return thisGeom.intersects(otherGeom);
        } else {
            return false;
        }
    }

    public static boolean intersects(Iterable<?> values, String geoString) {
        if (values != null) {
            boolean successfullyParsedAValue = false;
            Exception parseException = null;
            for (Object fieldValue : values) {
                try {
                    if (intersects(fieldValue, geoString)) {
                        return true;
                    }
                    successfullyParsedAValue = true;
                } catch (Exception e) {
                    // this is most likely a field value from the index (i.e. an encoded string @see GeometryNormalizer.getEncodedStringsFromGeometry)
                    // ignore and continue down the list of values. This will be thrown if every value in the list threw an exception
                    parseException = e;
                }
            }
            if (!successfullyParsedAValue) {
                BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.UNPARSEABLE_JEXL_QUERY, parseException,
                                "Did not find any properly encoded values to match against");
                throw new RuntimeException(qe);
            }
        }
        return false;
    }

    public static boolean overlaps(Object fieldValue, String geoString) {
        if (fieldValue != null) {
            Geometry otherGeom = AbstractGeometryNormalizer.parseGeometry(geoString);
            Geometry thisGeom = getGeometryFromFieldValue(fieldValue);
            return thisGeom.overlaps(otherGeom);
        } else {
            return false;
        }
    }

    public static boolean overlaps(Iterable<?> values, String geoString) {
        if (values != null) {
            boolean successfullyParsedAValue = false;
            Exception parseException = null;
            for (Object fieldValue : values) {
                try {
                    if (overlaps(fieldValue, geoString)) {
                        return true;
                    }
                    successfullyParsedAValue = true;
                } catch (Exception e) {
                    // this is most likely a field value from the index (i.e. an encoded string @see GeometryNormalizer.getEncodedStringsFromGeometry)
                    // ignore and continue down the list of values. This will be thrown if every value in the list threw an exception
                    parseException = e;
                }
            }
            if (!successfullyParsedAValue) {
                throw new RuntimeException("Did not find any properly encoded values to match against", parseException);
            }
        }
        return false;
    }

    public static boolean within(Object fieldValue, String geoString) {
        if (fieldValue != null) {
            Geometry otherGeom = AbstractGeometryNormalizer.parseGeometry(geoString);
            Geometry thisGeom = getGeometryFromFieldValue(fieldValue);
            return thisGeom.within(otherGeom);
        } else {
            return false;
        }
    }

    public static boolean within(Iterable<?> values, String geoString) {
        if (values != null) {
            boolean successfullyParsedAValue = false;
            Exception parseException = null;
            for (Object fieldValue : values) {
                try {
                    if (within(fieldValue, geoString)) {
                        return true;
                    }
                    successfullyParsedAValue = true;
                } catch (Exception e) {
                    // this is most likely a field value from the index (i.e. an encoded string @see GeometryNormalizer.getEncodedStringsFromGeometry)
                    // ignore and continue down the list of values. This will be thrown if every value in the list threw an exception
                    parseException = e;
                }
            }
            if (!successfullyParsedAValue) {
                throw new RuntimeException("Did not find any properly encoded values to match against", parseException);
            }
        }
        return false;
    }
}
