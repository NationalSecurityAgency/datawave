package datawave.data.type;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import datawave.data.normalizer.Normalizer;
import datawave.data.type.util.Point;

/**
 * Provides support for point geometry types. Other geometry types are not compatible with this type.
 */
public class PointType extends AbstractGeometryType<Point> {

    private String toString;
    private String delegateString;

    public PointType() {
        super(Normalizer.POINT_NORMALIZER);
    }

    @Override
    public String getDelegateAsString() {
        if (delegateString == null) {
            // this also calls AbstractGeometry#toString() which calls Geometry#toText()
            // which creates a new WKTWriter per call
            delegateString = super.getDelegateAsString();
        }
        return delegateString;
    }

    @Override
    public String toString() {
        if (toString == null) {
            // this is an expensive call to AbstractGeometry#toString() which itself calls Geometry#toText()
            // which creates a new WKTWriter per call
            toString = super.toString();
        }
        return toString;
    }

    @Override
    public void write(Kryo kryo, Output output) {
        output.writeString(getDelegateAsString());
        output.writeString(getNormalizedValue());
    }

    @Override
    public void read(Kryo kryo, Input input) {
        String delegateString = input.readString();
        String normalizedValue = input.readString();

        this.delegate = normalizer.denormalize(delegateString);
        this.normalizedValue = normalizedValue;
    }

    @Override
    public long sizeInBytes() {
        return super.sizeInBytes() + toString().length() + getDelegateAsString().length();
    }
}
