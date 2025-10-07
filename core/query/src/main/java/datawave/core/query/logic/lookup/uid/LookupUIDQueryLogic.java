package datawave.core.query.logic.lookup.uid;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.util.MultiValueMap;

import datawave.core.query.logic.BaseQueryLogic;
import datawave.core.query.logic.lookup.LookupQueryLogic;

public class LookupUIDQueryLogic<T> extends LookupQueryLogic<T> {
    public static final String UID_TERM_SEPARATOR = " ";
    private static final String EVENT_FIELD = "event";

    public LookupUIDQueryLogic(BaseQueryLogic<T> delegateQueryLogic) {
        super(delegateQueryLogic);
    }

    public LookupUIDQueryLogic(LookupQueryLogic<T> other) throws CloneNotSupportedException {
        super(other);
    }

    @Override
    public boolean isEventLookupRequired(MultiValueMap<String,String> lookupTerms) {
        return !(lookupTerms.keySet().size() == 1 && lookupTerms.containsKey(EVENT_FIELD));
    }

    @Override
    public Set<String> getContentLookupTerms(MultiValueMap<String,String> lookupTerms) {
        return lookupTerms.values().stream().flatMap(Collection::stream).collect(Collectors.toSet());
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return new LookupUIDQueryLogic<>(this);
    }
}
