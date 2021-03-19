package datawave.webservice.query.util;

import org.jboss.resteasy.specimpl.MultivaluedMapImpl;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import javax.ws.rs.core.MultivaluedMap;

public class MapUtils {
    
    public static <A,B> MultiValueMap<A,B> toMultiValueMap(MultivaluedMap<A,B> multivaluedMap) {
        MultiValueMap<A,B> multiValueMap = new LinkedMultiValueMap<>();
        multivaluedMap.forEach(multiValueMap::put);
        return multiValueMap;
    }
    
    public static <A,B> MultivaluedMap<A,B> toMultivaluedMap(MultiValueMap<A,B> multiValueMap) {
        MultivaluedMap<A,B> multivaluedMap = new MultivaluedMapImpl<>();
        multiValueMap.forEach(multivaluedMap::put);
        return multivaluedMap;
    }
}
