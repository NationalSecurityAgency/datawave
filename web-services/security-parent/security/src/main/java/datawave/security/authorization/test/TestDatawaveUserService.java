package datawave.security.authorization.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.collect.HashMultimap;

import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.security.authorization.AuthorizationException;
import datawave.security.authorization.CachedDatawaveUserService;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.DatawaveUserInfo;
import datawave.security.authorization.DatawaveUserService;
import datawave.security.authorization.SubjectIssuerDNPair;

/**
 * A {@link CachedDatawaveUserService} for testing purposes. This version will only be active if the system property {@code dw.security.use.testuserservice} is
 * set to {@code true}. When active, any incoming requests will first check a map of "canned" users and use the canned result if found. When no result is found,
 * it will delegate to the highest priority other {@link CachedDatawaveUserService} or {@link DatawaveUserService} that can be found. If no other instance is
 * found, then construction of this bean will fail.
 */
public class TestDatawaveUserService implements CachedDatawaveUserService {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private Map<SubjectIssuerDNPair,DatawaveUser> testUsers = new HashMap<>();
    private DatawaveUserService delegateService;
    private Map<String,DatawaveUserService> delegateServices;
    private String activeDelegateService;
    private Set<String> encodedTestUsers;

    @Inject
    private AccumuloConnectionFactory accumuloConnectionFactory;

    @Override
    public DatawaveUser list(String name) {
        if (delegateService instanceof CachedDatawaveUserService) {
            CachedDatawaveUserService delegateCachedService = (CachedDatawaveUserService) delegateService;
            return delegateCachedService.list(name);
        }
        return null;
    }

    @Override
    public Collection<? extends DatawaveUserInfo> listAll() {
        if (delegateService instanceof CachedDatawaveUserService) {
            CachedDatawaveUserService delegateCachedService = (CachedDatawaveUserService) delegateService;
            return delegateCachedService.listAll();
        }
        return Collections.emptyList();
    }

    @Override
    public Collection<? extends DatawaveUserInfo> listMatching(String substring) {
        if (delegateService instanceof CachedDatawaveUserService) {
            CachedDatawaveUserService delegateCachedService = (CachedDatawaveUserService) delegateService;
            return delegateCachedService.listMatching(substring);
        }
        return Collections.emptyList();
    }

    @Override
    public String evict(String name) {
        if (delegateService instanceof CachedDatawaveUserService) {
            CachedDatawaveUserService delegateCachedService = (CachedDatawaveUserService) delegateService;
            return delegateCachedService.evict(name);
        }
        return "No cache available.";
    }

    @Override
    public String evictMatching(String substring) {
        if (delegateService instanceof CachedDatawaveUserService) {
            CachedDatawaveUserService delegateCachedService = (CachedDatawaveUserService) delegateService;
            return delegateCachedService.evictMatching(substring);
        }
        return "No cache available.";
    }

    @Override
    public String evictAll() {
        if (delegateService instanceof CachedDatawaveUserService) {
            CachedDatawaveUserService delegateCachedService = (CachedDatawaveUserService) delegateService;
            return delegateCachedService.evictAll();
        }
        return "No cache available.";
    }

    @Override
    public Collection<DatawaveUser> reload(Collection<SubjectIssuerDNPair> dns) throws AuthorizationException {
        // @formatter:off
        ArrayList<SubjectIssuerDNPair> missing = new ArrayList<>();
        Map<SubjectIssuerDNPair, DatawaveUser> results = new LinkedHashMap<>();
        dns.forEach(dn -> {
            if (testUsers.containsKey(dn))
                results.put(dn, testUsers.get(dn));
            else
                missing.add(dn);
        });
        // @formatter:on
        if (!missing.isEmpty()) {
            if (delegateService instanceof CachedDatawaveUserService) {
                CachedDatawaveUserService delegateCachedService = (CachedDatawaveUserService) delegateService;
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
        Map<SubjectIssuerDNPair, DatawaveUser> results = new LinkedHashMap<>();
        dns.forEach(dn -> {
            if (testUsers.containsKey(dn))
                results.put(dn, testUsers.get(dn));
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
        this.delegateService = this.delegateServices.get(this.activeDelegateService);
        if (this.delegateService == null) {
            throw new IllegalStateException("No delegate DatawaveUserService found for name: " + this.activeDelegateService);
        }
        readEncodedTestUsers();
        try {
            this.testUsers = limitAuthsOfTestUsers();
        } catch (Exception e) {
            log.debug(String.format("Error limiting authorizations of test users: %s", e.getMessage()));
        }
    }

    public void setDelegateServices(Map<String,DatawaveUserService> delegateServices) {
        this.delegateServices = delegateServices;
    }

    public void setActiveDelegateService(String activeDelegateService) {
        this.activeDelegateService = activeDelegateService;
    }

    public void setEncodedTestUsers(Set<String> encodedTestUsers) {
        this.encodedTestUsers = encodedTestUsers;
    }

    public void setTestUsers(Map<SubjectIssuerDNPair,DatawaveUser> testUsers) {
        this.testUsers = testUsers;
    }

    private Map<SubjectIssuerDNPair,DatawaveUser> limitAuthsOfTestUsers() {
        // Strip off any authorizations not held by the designated Accumulo user.
        Map<SubjectIssuerDNPair,DatawaveUser> users = new HashMap<>();
        // Read in the Accumulo authorizations so we can trim what was supplied in configuration.
        List<String> accumuloAuthorizations = readAccumuloAuthorizations();
        this.testUsers.forEach((dn, user) -> {
            // Strip off any authorizations not held by the designated Accumulo user.
            ArrayList<String> auths = new ArrayList<>(user.getAuths());
            HashMultimap<String,String> authMapping = HashMultimap.create(user.getRoleToAuthMapping());
            auths.removeIf(a -> !accumuloAuthorizations.contains(a));
            authMapping.entries().removeIf(e -> !accumuloAuthorizations.contains(e.getValue()));

            DatawaveUser updatedUser = new DatawaveUser(user.getDn(), user.getUserType(), user.getEmail(), auths, user.getRoles(), authMapping,
                            user.getCreationTime(), user.getExpirationTime());
            users.put(dn, updatedUser);
        });
        return users;
    }

    protected void readEncodedTestUsers() {
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
                testUsers.put(user.getDn(), user);
            } catch (IOException e) {
                throw new RuntimeException("Invalid test user configuration: " + e.getMessage(), e);
            }
        });
    }

    protected List<String> readAccumuloAuthorizations() {
        try {
            AccumuloClient client = accumuloConnectionFactory.getClient(null, null, AccumuloConnectionFactory.Priority.ADMIN, new HashMap<>());
            Authorizations auths = client.securityOperations().getUserAuthorizations(client.whoami());
            return Arrays.asList(auths.toString().split("\\s*,\\s*"));
        } catch (Exception e) {
            throw new RuntimeException("Unable to acquire accumulo connector: " + e.getMessage(), e);
        }
    }
}
