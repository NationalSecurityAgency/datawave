package nsa.datawave.query.rewrite.iterator;

import java.util.HashSet;

@SuppressWarnings("rawtypes")
public class PowerSet<T> extends HashSet<T> {
    private static final long serialVersionUID = 1L;
    
    private static PowerSet inst;
    
    static {
        inst = new PowerSet();
    }
    
    @SuppressWarnings("unchecked")
    public static <T> PowerSet<T> instance() {
        return inst;
    }
    
    private PowerSet() {
        super(0);
    }
    
    @Override
    public boolean contains(Object o) {
        return true;
    }
    
    @Override
    public boolean add(T e) {
        return true;
    }
    
}
