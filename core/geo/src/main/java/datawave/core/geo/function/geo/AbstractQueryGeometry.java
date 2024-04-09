package datawave.core.geo.function.geo;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

import datawave.core.geo.utils.GeoUtils;

public abstract class AbstractQueryGeometry {
    public static final double MIN_LON = -180.0;
    public static final double MAX_LON = 180.0;

    public abstract Geometry getGeometry();

    public abstract Envelope getEnvelope();

    public static class BoundingBox extends AbstractQueryGeometry {
        private final double minLon;
        private final double maxLon;
        private final double minLat;
        private final double maxLat;
        private Geometry geometry;
        private Envelope envelope;

        public BoundingBox(double minLon, double maxLon, double minLat, double maxLat) {
            this.minLon = minLon;
            this.maxLon = maxLon;
            this.minLat = minLat;
            this.maxLat = maxLat;
        }

        public double getMinLon() {
            return minLon;
        }

        public double getMaxLon() {
            return maxLon;
        }

        public double getMinLat() {
            return minLat;
        }

        public double getMaxLat() {
            return maxLat;
        }

        @Override
        public Geometry getGeometry() {
            if (geometry == null) {
                geometry = GeoUtils.createRectangle(minLon, maxLon, minLat, maxLat);
            }
            return geometry;
        }

        @Override
        public Envelope getEnvelope() {
            if (envelope == null) {
                envelope = getGeometry().getEnvelopeInternal();
            }
            return envelope;
        }
    }

    public static class BoundingCircle extends AbstractQueryGeometry {
        private final double centerLon;
        private final double centerLat;
        private final double radius;
        private Geometry geometry;
        private Envelope envelope;

        public BoundingCircle(double centerLon, double centerLat, double radius) {
            this.centerLon = centerLon;
            this.centerLat = centerLat;
            this.radius = radius;
        }

        public double getCenterLon() {
            return centerLon;
        }

        public double getCenterLat() {
            return centerLat;
        }

        public double getRadius() {
            return radius;
        }

        @Override
        public Geometry getGeometry() {
            if (geometry == null) {
                geometry = GeoUtils.createCircle(centerLon, centerLat, radius);
            }
            return geometry;
        }

        @Override
        public Envelope getEnvelope() {
            if (envelope == null) {
                envelope = getGeometry().getEnvelopeInternal();
            }
            return envelope;
        }
    }

}
