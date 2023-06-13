package datawave.query.jexl.visitors;

import org.apache.commons.jexl3.JexlArithmetic;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;

public class DelegatingArithmetic extends JexlArithmetic {
    private final JexlArithmetic delegate;
    
    public DelegatingArithmetic(JexlArithmetic delegate) {
        super(false);
        this.delegate = delegate;
    }
    
    @Override
    public MathContext getMathContext() {
        return delegate.getMathContext();
    }
    
    @Override
    public int getMathScale() {
        return delegate.getMathScale();
    }
    
    @Override
    public Object add(Object left, Object right) {
        return delegate.add(left, right);
    }
    
    @Override
    public Object divide(Object left, Object right) {
        return delegate.divide(left, right);
    }
    
    @Override
    public Object mod(Object left, Object right) {
        return delegate.mod(left, right);
    }
    
    @Override
    public Object multiply(Object left, Object right) {
        return delegate.multiply(left, right);
    }
    
    @Override
    public Object subtract(Object left, Object right) {
        return delegate.subtract(left, right);
    }
    
    @Override
    public Object negate(Object val) {
        return delegate.negate(val);
    }
    
    @Override
    public Boolean contains(Object container, Object value) {
        return delegate.contains(container, value);
    }
    
    @Override
    public boolean equals(Object left, Object right) {
        return delegate.equals(left, right);
    }
    
    @Override
    public boolean lessThan(Object left, Object right) {
        return delegate.lessThan(left, right);
    }
    
    @Override
    public boolean greaterThan(Object left, Object right) {
        return delegate.greaterThan(left, right);
    }
    
    @Override
    public boolean lessThanOrEqual(Object left, Object right) {
        return delegate.lessThanOrEqual(left, right);
    }
    
    @Override
    public boolean greaterThanOrEqual(Object left, Object right) {
        return delegate.greaterThanOrEqual(left, right);
    }
    
    @Override
    public boolean toBoolean(Object val) {
        return delegate.toBoolean(val);
    }
    
    @Override
    public int toInteger(Object val) {
        return delegate.toInteger(val);
    }
    
    @Override
    public long toLong(Object val) {
        return delegate.toLong(val);
    }
    
    @Override
    public BigInteger toBigInteger(Object val) {
        return delegate.toBigInteger(val);
    }
    
    @Override
    public BigDecimal toBigDecimal(Object val) {
        return delegate.toBigDecimal(val);
    }
    
    @Override
    public double toDouble(Object val) {
        return delegate.toDouble(val);
    }
    
    @Override
    public String toString(Object val) {
        return delegate.toString(val);
    }
    
    @Override
    public Number narrow(Number original) {
        return delegate.narrow(original);
    }
    
}
