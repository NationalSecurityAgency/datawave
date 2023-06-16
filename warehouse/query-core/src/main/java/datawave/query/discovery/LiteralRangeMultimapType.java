package datawave.query.discovery;

import java.lang.reflect.Type;

import datawave.query.jexl.LiteralRange;

import com.google.common.collect.Multimap;
import com.google.gson.reflect.TypeToken;

public class LiteralRangeMultimapType {
    private static final TypeToken<Multimap<String,LiteralRange<String>>> tt = new TypeToken<Multimap<String,LiteralRange<String>>>() {};

    public static Type get() {
        return tt.getType();
    }

    public static TypeToken<Multimap<String,LiteralRange<String>>> token() {
        return tt;
    }
}
