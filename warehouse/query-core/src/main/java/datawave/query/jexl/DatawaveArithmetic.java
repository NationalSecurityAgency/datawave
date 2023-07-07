package datawave.query.jexl;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.jexl2.JexlArithmetic;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.log4j.Logger;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.Util;

import datawave.data.type.DateType;
import datawave.data.type.Type;
import datawave.data.type.util.NumericalEncoder;
import datawave.query.attributes.ValueTuple;

public abstract class DatawaveArithmetic extends JexlArithmetic {
    private static final String LESS_THAN = "<", GREATER_THAN = ">", LESS_THAN_OR_EQUAL = "<=", GREATER_THAN_OR_EQUAL = ">=";

    private static final Logger log = Logger.getLogger(DatawaveArithmetic.class);

    /**
     * Default to being lenient so we don't have to add "null" for every field in the query that doesn't exist in the document
     */
    public DatawaveArithmetic() {
        super(false);
    }

    public DatawaveArithmetic(boolean lenient) {
        super(lenient);
    }

    /**
     * Get the number class of an object. If a set of objects, then get the most inclusive number class. null is returned if no numeric class can be determined
     *
     * @param o
     *            The object to test
     * @param convert
     *            If true, then conversion from String to Number is permitted.
     * @return the number class, or null if not determinable
     */
    protected Class<? extends Number> getNumberClass(Object o, boolean convert) {
        if (o instanceof Set) {
            Class<? extends Number> lastC = null;
            for (Object setObject : (Set<?>) o) {
                Class<? extends Number> c = getNumberClass(setObject, convert);

                if (c == null) {
                    return null;
                } else if (lastC == null) {
                    lastC = c;
                } else if (lastC.equals(BigDecimal.class) || c.equals(BigDecimal.class)) {
                    lastC = BigDecimal.class;
                } else if (lastC.equals(BigInteger.class) || c.equals(BigInteger.class)) {
                    lastC = BigInteger.class;
                } else if (lastC.equals(Double.class) || c.equals(Double.class)) {
                    lastC = Double.class;
                } else if (lastC.equals(Float.class) || c.equals(Float.class)) {
                    lastC = Float.class;
                } else if (lastC.equals(Long.class) || c.equals(Long.class)) {
                    lastC = Long.class;
                } else if (lastC.equals(Integer.class) || c.equals(Integer.class)) {
                    lastC = Integer.class;
                } else if (lastC.equals(Short.class) || c.equals(Short.class)) {
                    lastC = Short.class;
                } else if (lastC.equals(Byte.class) || c.equals(Byte.class)) {
                    lastC = Byte.class;
                } else {
                    throw new IllegalStateException("Found a number class that is not handled: " + c);
                }
            }
            return lastC;
        } else if (o instanceof Number) {
            return ((Number) o).getClass();
        } else if (convert) {
            try {
                Number num = NumberUtils.createNumber(o.toString());
                return num.getClass();
            } catch (Exception nfe) {
                return null;
            }
        } else {
            // no conversion allowed, no number class determined
            return null;
        }
    }

    /**
     * Convert an object to the desired number class.
     *
     * @param o
     *            The object to convert
     * @param c
     *            The class to convert it to
     * @return The converted class, or the original object if conversion was not possible
     */
    protected Object convertToNumber(Object o, Class<? extends Number> c) {
        if (o instanceof Set) {
            Set<Object> numbers = new HashSet<>();
            for (Object setObject : (Set<?>) o) {
                numbers.add(convertToNumber(setObject, c));
            }
            return numbers;
        } else {
            try {
                Number num = NumberUtils.createNumber(o.toString());
                if (c.equals(BigDecimal.class)) {
                    return (num instanceof BigDecimal ? num : BigDecimal.valueOf(num.doubleValue()));
                } else if (c.equals(BigInteger.class)) {
                    return (num instanceof BigInteger ? num : BigInteger.valueOf(num.longValue()));
                } else if (c.equals(Double.class)) {
                    return (num instanceof Double ? num : num.doubleValue());
                } else if (c.equals(Float.class)) {
                    return (num instanceof Float ? num : num.floatValue());
                } else if (c.equals(Long.class)) {
                    return (num instanceof Long ? num : num.longValue());
                } else if (c.equals(Integer.class)) {
                    return (num instanceof Integer ? num : num.intValue());
                } else if (c.equals(Short.class)) {
                    return (num instanceof Short ? num : num.shortValue());
                } else if (c.equals(Byte.class)) {
                    return (num instanceof Byte ? num : num.byteValue());
                } else {
                    return num;
                }
            } catch (Exception nfe) {
                return o;
            }
        }
    }

    /**
     * Performs a comparison.
     *
     * @param left
     *            the left operand
     * @param right
     *            the right operator
     * @param operator
     *            the operator
     * @return {@code -1 if left < right; +1 if left > right; 0 if left == right}
     * @throws ArithmeticException
     *             if either left or right is null
     * @since 2.1
     */
    protected int compare(Object left, Object right, String operator) {
        if (left != null && right != null) {
            if ((isNumberable(left) || isFloatingPoint(left)) && NumericalEncoder.isPossiblyEncoded(right.toString())) {
                long lhs = toLong(left);
                return compare(toBigDecimal(left), NumericalEncoder.decode(right.toString()), operator);
            } else if (NumericalEncoder.isPossiblyEncoded(left.toString()) && (isNumberable(right) || isFloatingPoint(right))) {
                return compare(NumericalEncoder.decode(left.toString()), toBigDecimal(right), operator);
            } else {
                return super.compare(left, right, operator);
            }
        }
        throw new ArithmeticException("Object comparison:(" + left + " " + operator + " " + right + ")");
    }

    /**
     * if either one are Dates, try to
     *
     * @param left
     *            the left date
     * @param right
     *            the right date
     * @return if we can subtract the dates
     */
    public Object subtract(Object left, Object right) {
        if (left == null && right == null) {
            return controlNullNullOperands();
        }

        if (left instanceof ValueTuple && right instanceof ValueTuple) {
            Long dateDifference = subtract((ValueTuple) left, (ValueTuple) right);
            if (dateDifference != null) {
                return dateDifference;
            }
        }

        // if either are floating point (double or float) use double
        if (isFloatingPointNumber(left) || isFloatingPointNumber(right)) {
            double l = toDouble(left);
            double r = toDouble(right);
            return new Double(l - r);
        }

        // if either are bigdecimal use that type
        if (left instanceof BigDecimal || right instanceof BigDecimal) {
            BigDecimal l = toBigDecimal(left);
            BigDecimal r = toBigDecimal(right);
            BigDecimal result = l.subtract(r, getMathContext());
            return narrowBigDecimal(left, right, result);
        }

        // otherwise treat as integers
        BigInteger l = toBigInteger(left);
        BigInteger r = toBigInteger(right);
        BigInteger result = l.subtract(r);
        return narrowBigInteger(left, right, result);
    }

    protected Long subtract(Date left, Date right) {
        return left.getTime() - right.getTime();
    }

    protected Long subtract(DateType left, DateType right) {
        return subtract(left.getDelegate(), right.getDelegate());
    }

    protected Long subtract(ValueTuple left, ValueTuple right) {
        Object leftValue = ((ValueTuple) left).getValue();
        Object rightValue = ((ValueTuple) right).getValue();
        if (leftValue instanceof DateType && rightValue instanceof DateType) {
            return subtract((DateType) leftValue, (DateType) rightValue);
        }
        return null;
    }

    protected Long subtract(Collection<?> left, Collection<?> right) {
        if (left.size() == 1 && right.size() == 1) { // both singletons
            Object leftObject = left.iterator().next();
            Object rightObject = right.iterator().next();
            if (leftObject instanceof ValueTuple && rightObject instanceof ValueTuple) {
                Object leftValue = ((ValueTuple) left).getValue();
                Object rightValue = ((ValueTuple) right).getValue();
                if (leftValue instanceof DateType && rightValue instanceof DateType) {
                    return subtract((DateType) leftValue, (DateType) rightValue);
                }
            }
        }
        return null;
    }

    protected void addAll(Collection<Object> set, Object o) {
        if (o instanceof Collection) {
            set.addAll((Collection<?>) o);
        } else {
            set.add(o);
        }
    }

    /**
     * This method deals with the ValueTuple objects and turns them into the normalized value parts
     *
     * @param o
     *            an object
     * @return the normalized values
     */
    protected Object normalizedValues(Object o) {
        if (o instanceof Set) {
            Set<Object> normalizedValues = new HashSet<>();
            for (Object value : ((Set<?>) o)) {
                addAll(normalizedValues, normalizedValues(value));
            }
            if (normalizedValues.size() == 1) {
                return normalizedValues.iterator().next();
            } else {
                return normalizedValues;
            }
        } else {
            return ValueTuple.getNormalizedValue(o);
        }
    }

    /**
     * Tests whether the right value matches our FST.
     *
     * @param fst
     *            first value
     * @param right
     *            second value
     * @return test result.
     */
    public boolean fstMatch(FST fst, Object right) {
        right = normalizedValues(right);

        if (right instanceof Set) {
            Set<Object> set = (Set<Object>) right;

            for (Object o : set) {
                if (o != null) {
                    try {
                        if (matchesFst(o, fst))
                            return true;
                    } catch (IOException e) {
                        if (log.isTraceEnabled()) {
                            log.trace("Failed to evaluate " + right.toString() + " against the FST.");
                        }
                    }
                }
            }
        } else {
            if (right != null) {
                try {
                    if (matchesFst(right, fst))
                        return true;
                } catch (IOException e) {
                    if (log.isTraceEnabled()) {
                        log.trace("Failed to evaluate " + right.toString() + " against the FST.");
                    }
                }
            }
        }

        return false;
    }

    public static boolean matchesFst(Object object, FST fst) throws IOException {
        final IntsRefBuilder irBuilder = new IntsRefBuilder();
        Util.toUTF16(object.toString(), irBuilder);
        final IntsRef ints = irBuilder.get();
        synchronized (fst) {
            return Util.get(fst, ints) != null;
        }
    }

    /**
     * some of our nodes evaluate to a collection of matches. Coerce this collection to true or false based on the size of matches
     *
     * @param val
     *            a value
     * @return a boolean
     */
    @Override
    public boolean toBoolean(Object val) {
        if (val instanceof Collection) {
            return !((Collection) val).isEmpty();
        }
        return super.toBoolean(val);
    }

    private Object possibleValueTupleToDelegate(Object val) {
        // if the incoming val is a ValueTuple, swap in the delegate value
        if (val instanceof ValueTuple) {
            val = ((ValueTuple) val).second();
            if (val instanceof Type<?>) {
                val = ((Type) val).getDelegate();
            }
        }
        return val;
    }

    public int toInteger(Object val) {
        return super.toInteger(possibleValueTupleToDelegate(val));
    }

    public BigInteger toBigInteger(Object val) {
        return super.toBigInteger(possibleValueTupleToDelegate(val));
    }

    public BigDecimal toBigDecimal(Object val) {
        return super.toBigDecimal(possibleValueTupleToDelegate(val));
    }

    public double toDouble(Object val) {
        return super.toDouble(possibleValueTupleToDelegate(val));
    }

}
