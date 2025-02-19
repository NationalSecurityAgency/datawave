package datawave.security.user;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.user.AuthorizationsListBase;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.cache.concurrent.ConcurrentMapCache;
import org.springframework.cache.support.SimpleCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.List;

@RunWith(SpringRunner.class)
@ContextConfiguration
public class ListEffectiveAuthorizationsTest {

    @Configuration
    static class Config {

        @Bean
        public ResponseObjectFactory responseObjectFactory() {
            ResponseObjectFactory rof ;
            return rof;
        }
    }

    @Autowired
    private UserOperationsBean uob = new UserOperationsBean();

    @Test
    public void reduceRemoteProxiedUsersTest () {
        MockitoAnnotations.initMocks(this);

        SubjectIssuerDNPair userDN = SubjectIssuerDNPair.of("userDN", "issuerDN");
        SubjectIssuerDNPair p1dn = SubjectIssuerDNPair.of("entity1UserDN", "entity1IssuerDN");

        DatawaveUser user = new DatawaveUser(userDN, DatawaveUser.UserType.USER, Sets.newHashSet("A", "C", "D"), null, null, System.currentTimeMillis());
        DatawaveUser p1 = new DatawaveUser(p1dn, DatawaveUser.UserType.SERVER, Sets.newHashSet("A", "B", "E"), null, null, System.currentTimeMillis());

        DatawavePrincipal proxiedUserPrincipal = new DatawavePrincipal(Lists.newArrayList(user, p1));

        Mockito.when(responseObjectFactory.getAuthorizationsList()).thenReturn(Mockito.mock(AuthorizationsListBase.class));

        uob.listEffectiveAuthorizations(proxiedUserPrincipal);
        // UserOperations
        // UserOperationsBean
        // Need to mock getRemoteUser call of UserOperations
        // create local principal

        // userOperationsBean.listEffectiveAuthorizations

        // check that remote only proxied users were removed
    }
}
