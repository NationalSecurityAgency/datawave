package datawave.query.discovery;

import java.lang.reflect.Type;

import com.google.gson.reflect.TypeToken;

import datawave.query.jexl.LiteralRange;

public class LiteralRangeType {
    private static final TypeToken<LiteralRange<String>> tt = new TypeToken<LiteralRange<String>>() {};

    public static Type get() {
        return tt.getType();
    }

    public static TypeToken<LiteralRange<String>> token() {
        return tt;
    }
}
