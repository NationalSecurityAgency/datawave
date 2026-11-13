package datawave.webservice.query.util;

import java.util.List;
import java.util.Map;

import org.jboss.resteasy.specimpl.MultivaluedMapImpl;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import jakarta.ws.rs.core.MultivaluedMap;

public class MapUtils {

    public static <A,B> MultiValueMap<A,B> toMultiValueMap(MultivaluedMap<A,B> multivaluedMap) {
        MultiValueMap<A,B> multiValueMap = null;
        if (multivaluedMap != null) {
            multiValueMap = new LinkedMultiValueMap<>();
            multivaluedMap.forEach(multiValueMap::put);
        }
        return multiValueMap;
    }

    public static <A,B> MultivaluedMap<A,B> toMultivaluedMap(Map<A,List<B>> multiValueMap) {
        MultivaluedMap<A,B> multivaluedMap = null;
        if (multiValueMap != null) {
            multivaluedMap = new MultivaluedMapImpl<>();
            multiValueMap.forEach(multivaluedMap::put);
        }
        return multivaluedMap;
    }
}
