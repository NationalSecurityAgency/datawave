package nsa.datawave.query.rewrite.discovery;

import java.lang.reflect.Type;

import nsa.datawave.query.rewrite.jexl.LiteralRange;

import com.google.common.collect.Multimap;
import com.google.gson.reflect.TypeToken;

public class LiteralRangeType {
    private static final TypeToken<LiteralRange<String>> tt = new TypeToken<LiteralRange<String>>() {};
    
    public static Type get() {
        return tt.getType();
    }
    
    public static TypeToken<LiteralRange<String>> token() {
        return tt;
    }
}
