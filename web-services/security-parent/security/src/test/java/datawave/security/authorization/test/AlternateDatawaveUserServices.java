package datawave.security.authorization.test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import datawave.security.authorization.AuthorizationException;
import datawave.security.authorization.CachedDatawaveUserService;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.DatawaveUserInfo;
import datawave.security.authorization.SubjectIssuerDNPair;

public class AlternateDatawaveUserServices {
    public static class DatawaveUserService1 extends DefaultDatawaveUserService {
        @Override
        String getName() {
            return "DefaultService";
        }
    }

    public static class AltDatawaveUserService1 extends DefaultDatawaveUserService {
        @Override
        String getName() {
            return "Alternative1";
        }
    }

    public static class AltDatawaveUserService2 extends DefaultDatawaveUserService {
        @Override
        String getName() {
            return "Alternative2";
        }
    }

    public static class AltDatawaveUserService3 extends DefaultCachedDatawaveUserService {
        @Override
        String getName() {
            return "Alternative3";
        }
    }

    public static class AltDatawaveUserService4 extends DefaultCachedDatawaveUserService {
        @Override
        String getName() {
            return "Alternative4";
        }
    }

    public static class AltDatawaveUserService5 extends DefaultDatawaveUserService {
        @Override
        String getName() {
            return "Alternative5";
        }
    }

    public abstract static class DefaultDatawaveUserService extends TestDatawaveUserService {
        private Map<SubjectIssuerDNPair,DatawaveUser> testUsers = new HashMap<>();

        abstract String getName();

        @Override
        public Collection<DatawaveUser> lookup(Collection<SubjectIssuerDNPair> dns) throws AuthorizationException {
            // @formatter:off
            return super.lookup(dns).stream()
                    .map(user -> new DatawaveUser(user.getDn(), user.getUserType(), user.getAuths(),
                            appendToRoles(user.getRoles(), getName()),
                            user.getRoleToAuthMapping(), System.currentTimeMillis())).collect(Collectors.toList());
            // @formatter:on
        }

        private List<String> appendToRoles(Collection<String> roles, String roleToAppend) {
            List<String> newRoles = new ArrayList<>(roles);
            newRoles.add(roleToAppend);
            return newRoles;
        }
    }

    protected abstract static class DefaultCachedDatawaveUserService extends DefaultDatawaveUserService implements CachedDatawaveUserService {

        @Override
        public Collection<DatawaveUser> reload(Collection<SubjectIssuerDNPair> dns) throws AuthorizationException {
            return null;
        }

        @Override
        public DatawaveUser list(String name) {
            return null;
        }

        @Override
        public Collection<? extends DatawaveUserInfo> listAll() {
            return null;
        }

        @Override
        public Collection<? extends DatawaveUserInfo> listMatching(String substring) {
            return null;
        }

        @Override
        public String evict(String name) {
            return null;
        }

        @Override
        public String evictMatching(String substring) {
            return null;
        }

        @Override
        public String evictAll() {
            return null;
        }
    }
}
