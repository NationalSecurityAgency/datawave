package datawave.microservice.authorization.mock;

import com.google.common.collect.HashMultimap;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.DatawaveUser.UserType;
import datawave.security.authorization.SubjectIssuerDNPair;
import org.springframework.cache.annotation.CacheConfig;
import org.springframework.cache.annotation.Cacheable;

import java.util.Map;
import java.util.regex.Pattern;

import static datawave.microservice.authorization.mock.MockDatawaveUserService.CACHE_NAME;

/**
 * A helper class to allow calls to be cached using Spring annotations. Normally, these could just be methods in {@link MockDatawaveUserService}. However,
 * Spring caching works by wrapping the class in a proxy, and self-calls on the class will not go through the proxy. By having a separate class with these
 * methods, we get a separate proxy that performs the proper cache operations on these methods.
 */
@CacheConfig(cacheNames = CACHE_NAME)
public class MockDatawaveUserLookup {
    private final MockDULProperties mockDULProperties;
    
    public MockDatawaveUserLookup(MockDULProperties mockDULProperties) {
        this.mockDULProperties = mockDULProperties;
    }
    
    @Cacheable(key = "#dn.toString()")
    public DatawaveUser lookupUser(SubjectIssuerDNPair dn) {
        return buildUser(dn);
    }
    
    @Cacheable(key = "#dn.toString()")
    public DatawaveUser reloadUser(SubjectIssuerDNPair dn) {
        return buildUser(dn);
    }
    
    private DatawaveUser buildUser(SubjectIssuerDNPair dn) {
        UserType userType = UserType.USER;
        if (mockDULProperties.getServerDnRegex() != null && Pattern.matches(mockDULProperties.getServerDnRegex(), dn.subjectDN())) {
            userType = UserType.SERVER;
        }
        
        Map<String,String> rolesToAuths = mockDULProperties.getPerUserRolesToAuths().get(dn.toString());
        if (rolesToAuths == null) {
            rolesToAuths = mockDULProperties.getGlobalRolesToAuths();
        }
        
        HashMultimap<String,String> mapping = HashMultimap.create();
        rolesToAuths.forEach(mapping::put);
        return new DatawaveUser(dn, userType, rolesToAuths.values(), rolesToAuths.keySet(), mapping, System.currentTimeMillis());
    }
}
