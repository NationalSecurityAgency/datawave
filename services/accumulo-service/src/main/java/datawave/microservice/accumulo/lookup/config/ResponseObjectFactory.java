package datawave.microservice.accumulo.lookup.config;

import datawave.webservice.response.LookupResponse;
import datawave.webservice.response.objects.DefaultKey;
import datawave.webservice.response.objects.Entry;
import datawave.webservice.response.objects.KeyBase;

public interface ResponseObjectFactory {
    
    default KeyBase createKey() {
        return new DefaultKey();
    }
    
    default LookupResponse createLookupResponse() {
        return new LookupResponse();
    }
    
    default Entry createEntry(KeyBase key, byte[] value) {
        return new Entry(key, value);
    }
}
