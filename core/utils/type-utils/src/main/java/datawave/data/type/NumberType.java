package datawave.data.type;

import java.math.BigDecimal;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import datawave.data.normalizer.Normalizer;

public class NumberType extends BaseType<BigDecimal> {

    private static final long serialVersionUID = 1398451215614987988L;
    private static final long STATIC_SIZE = PrecomputedSizes.STRING_STATIC_REF + PrecomputedSizes.BIGDECIMAL_STATIC_REF + Sizer.REFERENCE;

    public NumberType() {
        super(Normalizer.NUMBER_NORMALIZER);
    }

    public NumberType(String delegateString) {
        super(delegateString, Normalizer.NUMBER_NORMALIZER);
    }

    /**
     * one String, one BigDecimal and one reference to a normalizer
     */
    @Override
    public long sizeInBytes() {
        return STATIC_SIZE + (2L * normalizedValue.length());
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
        this.delegate = new BigDecimal(delegateString);
        this.normalizedValue = normalizedValue;
    }
}
