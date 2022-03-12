package datawave.query.attributes;

import datawave.data.type.StringType;

import java.util.function.Predicate;

public class HitTermType extends StringType {

    public static final boolean matches(Attribute attr) {
        return attr instanceof TypeAttribute && ((TypeAttribute)attr).getType() instanceof HitTermType;
    }
}
