package datawave.data.type;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import datawave.data.normalizer.Normalizer;

public class GeoLonType extends BaseType<String> {

    private static final long serialVersionUID = 8912983433360105604L;
    private static final long STATIC_SIZE = PrecomputedSizes.STRING_STATIC_REF * 2 + Sizer.REFERENCE;

    public GeoLonType() {
        super(Normalizer.GEO_LON_NORMALIZER);
    }

    /**
     * Two String + normalizer reference
     *
     * @return the size in bytes
     */
    @Override
    public long sizeInBytes() {
        return STATIC_SIZE + (2L * normalizedValue.length()) + (2L * delegate.length());
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
        this.delegate = delegateString;
        this.normalizedValue = normalizedValue;
    }
}
