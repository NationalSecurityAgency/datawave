package datawave.security.test;

import java.security.Principal;
import java.security.spec.AlgorithmParameterSpec;
import java.util.Set;

import org.wildfly.security.auth.SupportLevel;
import org.wildfly.security.auth.principal.NamePrincipal;
import org.wildfly.security.auth.server.RealmIdentity;
import org.wildfly.security.auth.server.SecurityRealm;
import org.wildfly.security.authz.Attributes;
import org.wildfly.security.authz.AuthorizationIdentity;
import org.wildfly.security.authz.MapAttributes;
import org.wildfly.security.credential.Credential;
import org.wildfly.security.evidence.Evidence;

/**
 * A simple {@link SecurityRealm} that will return a user with the given principal name and the roles Administrator and InternalUser.
 */
public class TestRealm implements SecurityRealm {

    public static final String ROLES_ATTRIBUTE = "Roles";

    @Override
    public SupportLevel getCredentialAcquireSupport(Class<? extends Credential> credentialType, String algorithmName, AlgorithmParameterSpec parameterSpec) {
        return SupportLevel.POSSIBLY_SUPPORTED;
    }

    @Override
    public SupportLevel getEvidenceVerifySupport(Class<? extends Evidence> evidenceType, String algorithmName) {
        return SupportLevel.POSSIBLY_SUPPORTED;
    }

    @Override
    public RealmIdentity getRealmIdentity(Principal principal) {
        return new TestRealmIdentity(principal.getName());
    }

    private static class TestRealmIdentity implements RealmIdentity {

        private final String name;
        private final Attributes attributes;

        public TestRealmIdentity(String name) {
            this.name = name;
            MapAttributes mapAttributes = new MapAttributes();
            mapAttributes.addAll(ROLES_ATTRIBUTE, Set.of("Administrator", "InternalUser"));
            this.attributes = mapAttributes.asReadOnly();
        }

        @Override
        public Principal getRealmIdentityPrincipal() {
            return new NamePrincipal(name);
        }

        @Override
        public SupportLevel getCredentialAcquireSupport(Class<? extends Credential> credentialType, String algorithmName,
                        AlgorithmParameterSpec parameterSpec) {
            return SupportLevel.POSSIBLY_SUPPORTED;
        }

        @Override
        public <C extends Credential> C getCredential(Class<C> credentialType) {
            return null;
        }

        @Override
        public SupportLevel getEvidenceVerifySupport(Class<? extends Evidence> evidenceType, String algorithmName) {
            return SupportLevel.POSSIBLY_SUPPORTED;
        }

        @Override
        public boolean verifyEvidence(Evidence evidence) {
            return true;
        }

        @Override
        public boolean exists() {
            return true;
        }

        @Override
        public AuthorizationIdentity getAuthorizationIdentity() {
            return exists() ? AuthorizationIdentity.basicIdentity(attributes) : AuthorizationIdentity.EMPTY;
        }

        @Override
        public Attributes getAttributes() {
            return attributes;
        }
    }
}
