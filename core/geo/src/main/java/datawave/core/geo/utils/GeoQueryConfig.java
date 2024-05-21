package datawave.core.geo.utils;

import java.util.Objects;

/**
 * This class contains all the configuration properties that would affect geo query range generation. Not all properties are applicable to each style of geo
 * query or index type.
 */
public class GeoQueryConfig {
    // geowave configuration
    private int geowaveMaxEnvelopes;
    private int geometryMaxExpansion;
    private int pointMaxExpansion;
    private boolean optimizeGeoWaveRanges;
    private int rangeSplitThreshold;
    private double maxRangeOverlap;

    // geo configuration
    private int geoMaxEnvelopes;
    private int geoMaxExpansion;
    private boolean optimizeGeoRanges;

    private GeoQueryConfig() {

    }

    public static Builder builder() {
        return new Builder();
    }

    public int getGeowaveMaxEnvelopes() {
        return geowaveMaxEnvelopes;
    }

    public int getGeometryMaxExpansion() {
        return geometryMaxExpansion;
    }

    public int getPointMaxExpansion() {
        return pointMaxExpansion;
    }

    public boolean isOptimizeGeoWaveRanges() {
        return optimizeGeoWaveRanges;
    }

    public int getRangeSplitThreshold() {
        return rangeSplitThreshold;
    }

    public double getMaxRangeOverlap() {
        return maxRangeOverlap;
    }

    public int getGeoMaxEnvelopes() {
        return geoMaxEnvelopes;
    }

    public int getGeoMaxExpansion() {
        return geoMaxExpansion;
    }

    public boolean isOptimizeGeoRanges() {
        return optimizeGeoRanges;
    }

    public static class Builder {
        // geowave configuration
        private int geowaveMaxEnvelopes = 4;
        private int geometryMaxExpansion = 8;
        private int pointMaxExpansion = 32;
        private boolean optimizeGeoWaveRanges = true;
        private int rangeSplitThreshold = 16;
        private double maxRangeOverlap = 0.25;

        // geo configuration
        private int geoMaxEnvelopes = 4;
        private int geoMaxExpansion = 32;
        private boolean optimizeGeoRanges = true;

        public GeoQueryConfig build() {
            GeoQueryConfig config = new GeoQueryConfig();
            config.geowaveMaxEnvelopes = geowaveMaxEnvelopes;
            config.geometryMaxExpansion = geometryMaxExpansion;
            config.pointMaxExpansion = pointMaxExpansion;
            config.optimizeGeoWaveRanges = optimizeGeoWaveRanges;
            config.rangeSplitThreshold = rangeSplitThreshold;
            config.maxRangeOverlap = maxRangeOverlap;
            config.geoMaxEnvelopes = geoMaxEnvelopes;
            config.geoMaxExpansion = geoMaxExpansion;
            config.optimizeGeoRanges = optimizeGeoRanges;
            return config;
        }

        public Builder setGeowaveMaxEnvelopes(int geowaveMaxEnvelopes) {
            this.geowaveMaxEnvelopes = geowaveMaxEnvelopes;
            return this;
        }

        public Builder setGeometryMaxExpansion(int geometryMaxExpansion) {
            this.geometryMaxExpansion = geometryMaxExpansion;
            return this;
        }

        public Builder setPointMaxExpansion(int pointMaxExpansion) {
            this.pointMaxExpansion = pointMaxExpansion;
            return this;
        }

        public Builder setOptimizeGeoWaveRanges(boolean optimizeGeoWaveRanges) {
            this.optimizeGeoWaveRanges = optimizeGeoWaveRanges;
            return this;
        }

        public Builder setRangeSplitThreshold(int rangeSplitThreshold) {
            this.rangeSplitThreshold = rangeSplitThreshold;
            return this;
        }

        public Builder setMaxRangeOverlap(double maxRangeOverlap) {
            this.maxRangeOverlap = maxRangeOverlap;
            return this;
        }

        public Builder setGeoMaxEnvelopes(int geoMaxEnvelopes) {
            this.geoMaxEnvelopes = geoMaxEnvelopes;
            return this;
        }

        public Builder setGeoMaxExpansion(int geoMaxExpansion) {
            this.geoMaxExpansion = geoMaxExpansion;
            return this;
        }

        public Builder setOptimizeGeoRanges(boolean optimizeGeoRanges) {
            this.optimizeGeoRanges = optimizeGeoRanges;
            return this;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        GeoQueryConfig that = (GeoQueryConfig) o;
        return geowaveMaxEnvelopes == that.geowaveMaxEnvelopes && geometryMaxExpansion == that.geometryMaxExpansion
                        && pointMaxExpansion == that.pointMaxExpansion && optimizeGeoWaveRanges == that.optimizeGeoWaveRanges
                        && rangeSplitThreshold == that.rangeSplitThreshold && Double.compare(maxRangeOverlap, that.maxRangeOverlap) == 0
                        && geoMaxEnvelopes == that.geoMaxEnvelopes && geoMaxExpansion == that.geoMaxExpansion && optimizeGeoRanges == that.optimizeGeoRanges;
    }

    @Override
    public int hashCode() {
        return Objects.hash(geowaveMaxEnvelopes, geometryMaxExpansion, pointMaxExpansion, optimizeGeoWaveRanges, rangeSplitThreshold, maxRangeOverlap,
                        geoMaxEnvelopes, geoMaxExpansion, optimizeGeoRanges);
    }
}
