package datawave.query.discovery;

import java.lang.reflect.Type;

import com.google.common.collect.Multimap;
import com.google.gson.reflect.TypeToken;

public class MultimapType {
    private static final TypeToken<Multimap<String,String>> tt = new TypeToken<Multimap<String,String>>() {};

    public static Type get() {
        return tt.getType();
    }

    public static TypeToken<Multimap<String,String>> token() {
        return tt;
    }
}
