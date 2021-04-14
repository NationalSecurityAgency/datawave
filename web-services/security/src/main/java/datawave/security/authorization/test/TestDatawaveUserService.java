package datawave.security.authorization.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.collect.HashMultimap;
import datawave.configuration.RefreshableScope;
import datawave.configuration.spring.SpringBean;
import datawave.security.authorization.AuthorizationException;
import datawave.security.authorization.CachedDatawaveUserService;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.DatawaveUserInfo;
import datawave.security.authorization.DatawaveUserService;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.webservice.common.connection.AccumuloConnectionFactory;
import datawave.webservice.util.NotEqualPropertyExpressionInterpreter;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.deltaspike.core.api.exclude.Exclude;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Priority;
import javax.enterprise.context.spi.CreationalContext;
import javax.enterprise.inject.Alternative;
import javax.enterprise.inject.spi.Bean;
import javax.enterprise.inject.spi.BeanManager;
import javax.inject.Inject;
import javax.interceptor.Interceptor;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

/**
 * A {@link CachedDatawaveUserService} for testing purposes. This version will only be active if the syste property {@code dw.security.use.testuserservice} is
 * set to {@code true}. When active, any incoming requests will first check a map of "canned" users and use the canned result if found. When no result is found,
 * it will delegate to the highest priority other {@link CachedDatawaveUserService} or {@link DatawaveUserService} that can be found. If no other instance is
 * found, then construction of this bean will fail.
 */
@RefreshableScope
@Alternative
// Make this alternative active for the entire application per the CDI 1.2 specification
@Priority(Interceptor.Priority.PLATFORM_AFTER)
// Exclude this bean if the system property dw.security.use.testuserservice isn't defined to be true
@Exclude(onExpression = "dw.security.use.testuserservice!=true", interpretedBy = NotEqualPropertyExpressionInterpreter.class)
public class TestDatawaveUserService implements CachedDatawaveUserService {
    private HashMap<SubjectIssuerDNPair,DatawaveUser> cannedUsers = new HashMap<>();
    private DatawaveUserService delegateService;
    private CachedDatawaveUserService delegateCachedService;
    private CreationalContext<?> delegateContext;
    
    @Inject
    @SpringBean(name = "testAuthDatawaveUsers", required = false)
    private Set<String> encodedTestUsers;
    
    @Inject
    private BeanManager beanManager;
    
    @Inject
    private AccumuloConnectionFactory accumuloConnectionFactory;
    
    @Override
    public DatawaveUser list(String name) {
        return delegateCachedService == null ? null : delegateCachedService.list(name);
    }
    
    @Override
    public Collection<? extends DatawaveUserInfo> listAll() {
        return delegateCachedService == null ? Collections.emptyList() : delegateCachedService.listAll();
    }
    
    @Override
    public Collection<? extends DatawaveUserInfo> listMatching(String substring) {
        return delegateCachedService == null ? Collections.emptyList() : delegateCachedService.listMatching(substring);
    }
    
    @Override
    public String evict(String name) {
        return delegateCachedService == null ? "No cache available." : delegateCachedService.evict(name);
    }
    
    @Override
    public String evictMatching(String substring) {
        return delegateCachedService == null ? "No cache available." : delegateCachedService.evictMatching(substring);
    }
    
    @Override
    public String evictAll() {
        return delegateCachedService == null ? "No cache available." : delegateCachedService.evictAll();
    }
    
    @Override
    public Collection<DatawaveUser> reload(Collection<SubjectIssuerDNPair> dns) throws AuthorizationException {
        // @formatter:off
        ArrayList<SubjectIssuerDNPair> missing = new ArrayList<>();
        HashMap<SubjectIssuerDNPair, DatawaveUser> results = new HashMap<>();
        dns.forEach(dn -> {
            if (cannedUsers.containsKey(dn))
                results.put(dn, cannedUsers.get(dn));
            else
                missing.add(dn);
        });
        // @formatter:on
        if (!missing.isEmpty()) {
            if (delegateCachedService != null) {
                delegateCachedService.reload(missing).forEach(u -> results.put(u.getDn(), u));
            } else {
                // If the delegate service is not a cached one, then simply call lookup for each DN
                // since it isn't cached anyway.
                delegateService.lookup(missing).forEach(u -> results.put(u.getDn(), u));
            }
        }
        return results.values();
    }
    
    @Override
    public Collection<DatawaveUser> lookup(Collection<SubjectIssuerDNPair> dns) throws AuthorizationException {
        // @formatter:off
        ArrayList<SubjectIssuerDNPair> missing = new ArrayList<>();
        HashMap<SubjectIssuerDNPair, DatawaveUser> results = new HashMap<>();
        dns.forEach(dn -> {
            if (cannedUsers.containsKey(dn))
                results.put(dn, cannedUsers.get(dn));
            else
                missing.add(dn);
        });
        // @formatter:on
        if (!missing.isEmpty()) {
            delegateService.lookup(missing).forEach(u -> results.put(u.getDn(), u));
        }
        return results.values();
    }
    
    @PostConstruct
    protected void init() {
        // @formatter:off
        Comparator<Bean<?>> beanComparator = Comparator.comparing(
            b -> b.getBeanClass().isAnnotationPresent(Priority.class)
               ? b.getBeanClass().getAnnotation(Priority.class).value()
               : Integer.MIN_VALUE);

        Bean<?> alternate = beanManager.getBeans(CachedDatawaveUserService.class).stream()
                .filter(b -> b.getBeanClass() != getClass())
                .sorted(beanComparator.reversed())
                .findFirst().orElse(null);

        Bean<?> basicAlternate = beanManager.getBeans(DatawaveUserService.class).stream()
                .filter(b -> b.getBeanClass() != getClass())
                .sorted(beanComparator.reversed())
                .findFirst().orElse(null);
        // @formatter:on
        
        if (alternate == null && basicAlternate == null) {
            throw new IllegalStateException("No delegate " + CachedDatawaveUserService.class + " or " + DatawaveUser.class + " was found.");
        } else if (alternate != null) {
            delegateContext = beanManager.createCreationalContext(alternate);
            delegateCachedService = (CachedDatawaveUserService) beanManager.getReference(alternate, alternate.getBeanClass(), delegateContext);
            delegateService = delegateCachedService;
        } else {
            delegateContext = beanManager.createCreationalContext(basicAlternate);
            delegateService = (DatawaveUserService) beanManager.getReference(basicAlternate, basicAlternate.getBeanClass(), delegateContext);
        }
        
        readTestUsers();
    }
    
    protected void readTestUsers() {
        if (encodedTestUsers == null || encodedTestUsers.isEmpty())
            return;
        
        // Read in the Accumulo authorizations so we can trim what was supplied in the test file.
        List<String> accumuloAuthorizations = readAccumuloAuthorizations();
        
        // Use Jackson to de-serialize te JSON provided for each test user.
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new GuavaModule());
        encodedTestUsers.forEach(u -> {
            try {
                DatawaveUser user = objectMapper.readValue(u, DatawaveUser.class);
                
                // Strip off any authorizations not held by the designated Accumulo user.
                ArrayList<String> auths = new ArrayList<>(user.getAuths());
                HashMultimap<String,String> authMapping = HashMultimap.create(user.getRoleToAuthMapping());
                auths.removeIf(a -> !accumuloAuthorizations.contains(a));
                authMapping.entries().removeIf(e -> !accumuloAuthorizations.contains(e.getValue()));
                
                user = new DatawaveUser(user.getDn(), user.getUserType(), user.getEmail(), auths, user.getRoles(), authMapping, user.getCreationTime(), user
                                .getExpirationTime());
                
                cannedUsers.put(user.getDn(), user);
            } catch (IOException e) {
                throw new RuntimeException("Invalid test user configuration: " + e.getMessage(), e);
            }
        });
    }
    
    protected List<String> readAccumuloAuthorizations() {
        try {
            AccumuloClient client = accumuloConnectionFactory.getClient(null, AccumuloConnectionFactory.Priority.ADMIN, new HashMap<>());
            Authorizations auths = client.securityOperations().getUserAuthorizations(client.whoami());
            return Arrays.asList(auths.toString().split("\\s*,\\s*"));
        } catch (Exception e) {
            throw new RuntimeException("Unable to acquire accumulo connector: " + e.getMessage(), e);
        }
    }
    
    @PreDestroy
    protected void shutdown() {
        if (delegateContext != null) {
            delegateContext.release();
        }
    }
}
