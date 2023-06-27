package datawave.core.query.logic.lookup.uuid;

import java.util.Set;
import java.util.stream.Collectors;

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
    public String createQueryFromLookupTerms(MultiValueMap<String,String> lookupUUIDPairs) {
        // @formatter:off
        return lookupUUIDPairs
                .entrySet().stream()
                .flatMap(entry -> entry.getValue().stream().map(value -> String.join(LOOKUP_KEY_VALUE_DELIMITER, entry.getKey(), value)))
                .collect(Collectors.joining(UUID_TERM_SEPARATOR));
        // @formatter:on
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
