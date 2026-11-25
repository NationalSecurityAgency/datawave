package datawave.annotation.projection;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Converts a selected scalar to the event text required by its configured format. Text, annotation Float scores, exact JSON numbers and Booleans each have an
 * explicit format. Numeric-looking strings remain strings, and exact JSON numbers are rendered without conversion through Float or Double.
 */
enum ScalarFormat {
    TEXT("text", ScopedSelector.Kind.STRING),
    FLOAT("java-float-text", ScopedSelector.Kind.FLOAT),
    NUMBER("json-number-text", ScopedSelector.Kind.NUMBER),
    BOOLEAN("boolean-text", ScopedSelector.Kind.BOOLEAN);

    private static final int MAX_NUMERIC_DIGITS = 8 * 1024;
    private static final int MAX_EXPLICIT_EXPONENT = 10_000;
    // A decimal token's fractional digits and negative exponent can both increase its resulting scale.
    private static final int MAX_NUMERIC_SCALE = MAX_NUMERIC_DIGITS + MAX_EXPLICIT_EXPONENT;
    // ceil(8192 * log2(10)): check magnitude before conversion or decimal precision calculation can allocate digit text.
    private static final int MAX_SIGNIFICAND_BITS = 27_214;
    // Include room for sign, decimal point and scientific exponent without expanding the exponent into zeros.
    private static final int FLOAT_TEXT_BOUND = 16;
    private static final int DECIMAL_TEXT_OVERHEAD = 16;

    private final String configuredName;
    private final ScopedSelector.Kind inputKind;

    ScalarFormat(String configuredName, ScopedSelector.Kind inputKind) {
        this.configuredName = configuredName;
        this.inputKind = inputKind;
    }

    static ScalarFormat named(String name) {
        for (ScalarFormat format : values()) {
            if (format.configuredName.equals(name)) {
                return format;
            }
        }
        throw new ProjectionException("Unknown scalar format: " + name);
    }

    /**
     * Checks a selected property's declared type during profile compilation. A prepared extension entry can have a data-dependent type, so its actual value is
     * checked by {@link #render(Object, ProjectionWorkGuard, String)} during projection.
     */
    void requireCompatible(ScopedSelector.Kind kind) {
        if (kind != ScopedSelector.Kind.DYNAMIC && kind != inputKind) {
            throw new ProjectionException("Configured format " + this + " is incompatible with declared " + kind);
        }
    }

    /**
     * Checks that the selected value has the type required by this format, then checks the proposed string size before rendering. Exact numbers remove trailing
     * zeros and may use scientific notation so a large exponent does not allocate a long run of zeros.
     */
    String render(Object value, ProjectionWorkGuard work, String context) {
        switch (this) {
            case TEXT:
                if (value instanceof String) {
                    work.checkStringLength(((String) value).length());
                    return (String) value;
                }
                break;
            case FLOAT:
                if (value instanceof Float && Float.isFinite((Float) value)) {
                    work.checkStringLength(FLOAT_TEXT_BOUND);
                    return Float.toString((Float) value);
                }
                break;
            case NUMBER:
                BigDecimal decimal = exactNumber(value);
                if (decimal != null) {
                    checkNumber(decimal);
                    // BigDecimal.toString uses bounded scientific notation rather than expanding a large exponent.
                    work.checkStringLength((long) decimal.precision() + DECIMAL_TEXT_OVERHEAD);
                    return decimal.stripTrailingZeros().toString();
                }
                break;
            case BOOLEAN:
                if (value instanceof Boolean) {
                    work.checkStringLength(5);
                    return value.toString();
                }
                break;
            default:
                throw new AssertionError(this);
        }
        throw new ProjectionException(
                        context + ": format " + configuredName + " cannot consume " + (value == null ? "null" : value.getClass().getSimpleName()));
    }

    /**
     * Converts supported exact numeric representations to BigDecimal. BigDecimal, BigInteger and integral wrapper types preserve their mathematical values. A
     * null result means the selected value has another type; strings and Float/Double are not converted to exact numbers.
     */
    static BigDecimal exactNumber(Object value) {
        if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        }
        if (value instanceof BigInteger) {
            checkMagnitude((BigInteger) value);
            return new BigDecimal((BigInteger) value);
        }
        if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long) {
            return BigDecimal.valueOf(((Number) value).longValue());
        }
        return null;
    }

    /**
     * Checks the size of an already prepared exact number before formatting it. Significand and scale bounds conservatively follow the 8-KiB numeric-token
     * limit and the +/-10,000 explicit-exponent limit. Preparation must check the original token bytes and exponent before constructing the number; these
     * checks protect rendering of the resulting Java value.
     */
    static void checkNumber(BigDecimal value) {
        checkMagnitude(value.unscaledValue());
        if (value.precision() > MAX_NUMERIC_DIGITS || value.scale() < -MAX_EXPLICIT_EXPONENT || value.scale() > MAX_NUMERIC_SCALE) {
            throw new ProjectionException("Exact number exceeds the bounded numeric representation");
        }
    }

    private static void checkMagnitude(BigInteger value) {
        if (value.bitLength() > MAX_SIGNIFICAND_BITS) {
            throw new ProjectionException("Exact number exceeds the bounded numeric representation");
        }
    }
}
