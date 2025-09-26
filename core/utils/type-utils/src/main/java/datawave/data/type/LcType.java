package datawave.data.type;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import datawave.data.normalizer.Normalizer;

/**
 * Note: there were no significant optimizations found with overriding the Kryo {@link #read(Kryo, Input)} and {@link #write(Kryo, Output)} methods
 */
public class LcType extends BaseType<String> {

    private static final long serialVersionUID = -5102714749195917406L;
    private static final long STATIC_SIZE = PrecomputedSizes.STRING_STATIC_REF * 2 + Sizer.REFERENCE;

    public LcType() {
        super(Normalizer.LC_NORMALIZER);
    }

    public LcType(String delegateString) {
        super(delegateString, Normalizer.LC_NORMALIZER);
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
}
