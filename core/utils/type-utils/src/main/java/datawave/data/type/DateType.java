package datawave.data.type;

import static com.esotericsoftware.kryo.serializers.DefaultSerializers.DateSerializer;

import java.util.Date;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import datawave.data.normalizer.Normalizer;

public class DateType extends BaseType<Date> {

    private static final long serialVersionUID = 936566410691643144L;
    private static final long STATIC_SIZE = PrecomputedSizes.STRING_STATIC_REF + PrecomputedSizes.DATE_STATIC_REF + Sizer.REFERENCE;

    private static final DateSerializer serializer = new DateSerializer();

    private String normalizedDelegate = null;

    public DateType() {
        super(Normalizer.DATE_NORMALIZER);
    }

    public DateType(String dateString) {
        super(Normalizer.DATE_NORMALIZER);
        super.setDelegate(normalizer.denormalize(dateString));
    }

    @Override
    public String getDelegateAsString() {
        if (normalizedDelegate == null) {
            // the normalized form of the date preserves milliseconds
            normalizedDelegate = normalizer.normalizeDelegateType(getDelegate());
        }
        return normalizedDelegate;
    }

    /**
     * One string, one date object, one reference to the normalizer
     *
     * @return the size in bytes
     */
    @Override
    public long sizeInBytes() {
        return STATIC_SIZE + (2L * normalizedValue.length()) + getDelegateAsString().length();
    }

    @Override
    public void write(Kryo kryo, Output output) {
        kryo.writeObject(output, delegate, serializer);
        output.writeString(getNormalizedValue());
    }

    @Override
    public void read(Kryo kryo, Input input) {
        this.delegate = kryo.readObject(input, Date.class);
        this.normalizedValue = input.readString();
    }
}
