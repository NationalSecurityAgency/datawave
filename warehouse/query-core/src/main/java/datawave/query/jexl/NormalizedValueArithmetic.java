package datawave.query.jexl;

import org.apache.commons.jexl2.JexlArithmetic;

/**
 * This is a simplified version of the JexlArithmetic which is intended to be used to operate against normalized field values. This is commonly used when
 * filtering composite index fields.
 */
public class NormalizedValueArithmetic extends JexlArithmetic {
    public NormalizedValueArithmetic() {
        this(false);
    }
    
    public NormalizedValueArithmetic(boolean lenient) {
        super(lenient);
    }
    
    @Override
    public boolean equals(Object left, Object right) {
        if ((left == right) || (left == null) || (right == null)) {
            return false;
        } else {
            return left.toString().compareTo(right.toString()) == 0;
        }
    }
    
    @Override
    public boolean lessThan(Object left, Object right) {
        if ((left == right) || (left == null) || (right == null)) {
            return false;
        } else {
            return left.toString().compareTo(right.toString()) < 0;
        }
    }
    
    @Override
    public boolean greaterThan(Object left, Object right) {
        if ((left == right) || (left == null) || (right == null)) {
            return false;
        } else {
            return left.toString().compareTo(right.toString()) > 0;
        }
    }
    
    @Override
    public boolean lessThanOrEqual(Object left, Object right) {
        if ((left == right) || (left == null) || (right == null)) {
            return false;
        } else {
            return left.toString().compareTo(right.toString()) <= 0;
        }
    }
    
    @Override
    public boolean greaterThanOrEqual(Object left, Object right) {
        if ((left == right) || (left == null) || (right == null)) {
            return false;
        } else {
            return left.toString().compareTo(right.toString()) >= 0;
        }
    }
}
