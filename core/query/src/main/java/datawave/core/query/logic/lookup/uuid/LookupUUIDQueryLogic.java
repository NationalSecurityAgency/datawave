package datawave.core.query.logic.lookup.uuid;

import java.util.Set;

import org.springframework.util.MultiValueMap;

import datawave.core.query.logic.BaseQueryLogic;
import datawave.core.query.logic.lookup.LookupQueryLogic;

public class LookupUUIDQueryLogic<T> extends LookupQueryLogic<T> {
    private static final String UUID_TERM_SEPARATOR = " OR ";

    public LookupUUIDQueryLogic(BaseQueryLogic<T> delegateQueryLogic) {
        super(delegateQueryLogic);
    }

    public LookupUUIDQueryLogic(LookupQueryLogic<T> other) throws CloneNotSupportedException {
        super(other);
    }

    @Override
    public boolean isEventLookupRequired(MultiValueMap<String,String> lookupTerms) {
        // always, regardless of the terms
        return true;
    }

    @Override
    public Set<String> getContentLookupTerms(MultiValueMap<String,String> lookupTerms) throws UnsupportedOperationException {
        throw new UnsupportedOperationException("Cannot convert lookup terms to event lookups for LookupUUIDQueryLogic");
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return new LookupUUIDQueryLogic<>(this);
    }

}
