# Overview

Datawave leverages Wildfly's Elytron framework for authenticating and authorizing users within the Datawave EAR deployment. The full documentation for Wildfly is available via the [Wildfly docs](https://docs.wildfly.org/26/). There are several key concepts to understand when discussing Elytron, and for the sake of brevity, this README will only touch on concepts that are used by the Datawave security configuration.

## Key Concepts
* `HttpServerAuthenticationMechanismFactory`
    * An HttpServerAuthenticationMechanismFactory is responsible for providing instances of an HttpServerAuthenticationMechanism, and can be associated with specific mechanisms that it supports, such as BASIC, DIGEST, FORM, etc.
* `HttpServerAuthenticationMechanism`
    * An HttpServerAuthenticationMechanism is an authentication policy for authentication using HTTP/Websocket mechanisms. A HttpAuthenticationMechanism will be backed by a SecurityDomain.
* `SecurityDomain`
    * SecurityDomain can be considered a security policy that is backed by one or more SecurityRealm instances. The SecurityDomain is responsible for providing a SecurityIdentity, which is a representation of the current identity with roles and permissions. The SecurityDomain is the general wrapper around the policy providing a resulting SecurityIdentity, and makes use of the following components to define this policy:

* `SecurityRealm`
    * One more named SecurityRealms are associated with a SecurityDomain, the SecurityRealms are the access to the underlying repository of identities and are used for obtaining credentials to allow authentication mechanisms to perform verification, for validation of Evidence and for obtaining the raw AuthorizationIdentity performing the authentication.
* `RoleDecoder`
    * Along with the SecurityRealm association is also a reference to a RoleDecoder, the RoleDecoder takes the raw AuthorizationIdentity returned from the SecurityRealm and converts its attributes into roles.
* `EvidenceDecoder`
    * A EvidenceDecoder converts from an Evidence to a Principal.
* `PermissionMapper`
    * In addition to having roles a SecurityIdentity can also have a set of permissions, the PermissionMapper assigns those permissions to the identity. Crucially, one such Permissions is the LoginPermission, which allows a request to be successfully authorized.

## Datawave Elytron Implementations

* [`DatawaveHttpAuthenticationMechanismFactory`](src/main/java/datawave/security/auth/DatawaveHttpAuthenticationMechanismFactory.java): An implementation of HttpServerAuthenticationMechanismFactory. This class is responsible for providing instances of `DatawaveHttpAuthenticationMechanism`, which will handle incoming HTTP Requests for the mechanism `DATAWAVE-AUTH`. In order to make this factory detected as a service by Wildfly, the fully qualified name of the class must be in a file named org.wildfly.security.http.HttpServerAuthenticationMechanismFactory in the META-INF/services folder.
* [`DatawaveHttpAuthenticationMechanism`](src/main/java/datawave/security/auth/DatawaveHttpAuthenticationMechanism.java): An implementation of HttpServerAuthenticationMechanism. This class is responsible for extracting evidence from incoming HTTP requests (JWT, trusted headers, PKI certs) and submitting them to the backing security domain for authentication and authorization.
* [`DatawaveEvidenceDecoder`](src/main/java/datawave/security/realm/DatawaveEvidenceDecoder.java): An implementation of EvidenceDecoder that will convert an incoming Evidence to a DatawavePrincipal.
* [`DatawaveSecurityRealm`](src/main/java/datawave/security/realm/DatawaveSecurityRealm.java):: An implementation of SecurityRealm that is responsible for providing a RealmIdentity for authentication that is associated with a DatawavePrincipal. This realm is also responsible for extracting any information required for determining the roles of a user, and storing them within the RealmIdentity's Attributes.
* [`DatawaveRoleDecoder`](src/main/java/datawave/security/realm/DatawaveRoleDecoder.java): An implementation of RoleDecoder that will accept an AuthorizationIdentity provided by a RealmIdentity from the DatawaveSecurityRealm, and return a set of roles to be associated with the final SecurityIdentity established within the security domain.

## Basic Workflow

This workflow description assumes that a security domain has been configured to:
  - Pass HTTP requests to `DatawaveHttpAuthenticationMechanism`.
  - Decode evidence using `DatawaveEvidenceDecoder`.
  - Obtain a RealmIdentity using `DatawaveSecurityRealm`.
  - Decode roles using `DatawaveRoleDecoder`.
  - Use the default permissions mapper provided by Wildfly.

1. An incoming request is received and sent to `DatawaveHttpAuthenticationMechanism.evaluateRequest(HttpServerRequest)`.
2. If configured, the session scope for the request is examined for a previously cached identity. If one is found, we authorize using that identity. (Step 5, but using the cached identity's principal instead of a principal decoded from evidence).
3. If an identity is not cached, we extract a piece of DatawaveEvidence (JWT, trusted headers, PKI cert) identifying the user from the HttpServerRequest.
4. The Evidence is verified. This triggers the following:
   1. The Evidence is passed to `DatawaveEvidenceDecoder.getPrincipal(Evidence)` to obtain a DatawavePrincipal. This DatawavePrincipal is set as the Evidence's decoded principal.
   2. The DatawavePrincipal is passed to `DatawaveSecurityRealm.getRealmIdentity(Principal)` to obtain a RealmIdentity.
   3. Roles are extracted from the DatawavePrincipal and mapped to an Attributes that will be part of the AuthorizationIdentity returned from the RealmIdentity.
   4. The Evidence is passed to the RealmIdentity's `verifyEvidence(Evidence)`. If this returns false, i.e., fails verification, the user cannot be authorized, and login fails.
5. If verification succeeds, we then attempt to authorize the request using the DatawaveEvidence's decoded principal, which will be a DatawavePrincipal. This triggers the following:
   1. The RealmIdentity associated with the DatawavePrincipal is obtained from the security realm.
   2. The AuthorizationIdentity from the RealmIdentity is passed to `DatawaveRoleDecoder.decodeRoles(AuthorizationIdentity)` to obtain a set of Roles for the user.
   3. If a non-empty role set was returned, the user is granted LoginPermission and BatchPermissions by the default permissions mapper, and login succeeds. If an empty role set is returned, login fails.

This workflow is primarily executed through a series of Callbacks passed to the Wildfly framework. These callbacks are passed to the Wildfly class `org.wildfly.security.auth.server.ServerAuthenticationContext` which manages a state machine and handles the workflow steps described above. The Wildfly Elytron source code can be viewed at [Github](https://github.com/wildfly-security/wildfly-elytron).

## Configuration Highlights

### EJB JNDI Lookup
One of the limitations of implementing custom Wildfly Elytron components is that they must be deployed as a separate Wildfly module, and cannot be bundled with the EAR deployment. Wildfly does not support injecting EJBs from a deployment into a separate Wildfly module, so we are forced to use JNDI to look up a [SecurityEJBProvider](../security-elytron/src/main/java/datawave/security/system/SecurityEJBProvider.java) instance that should be created and bound from the Datawave deployment. By default, this will be the singleton instance of [SecurityEJBProviderImpl](../security/src/main/java/datawave/security/system/SecurityEJBProviderImpl.java). The JNDI name must be configured via the system property `dw.security.ejb.provider.jndi`. For example:
```text
/system-property=dw.security.ejb.provider.jndi:add(value=java:global/datawave-ws-deploy-application-${project.version}-compose/gov.nsa.datawave.webservices-datawave-ws-security-${project.version}/SecurityEJBProviderImpl)
```
The elytron module can then access EJBs created by the Datawave deployment through the EJB provider.

### Request Session Identity Caching
The DatawaveHttpAuthenticationMechanism can be configured to cache an identity in the request's session, and to change the session ID via the configuration properties `enableRestoreIdentity` and `enableSessionIdChange`. These options are enabled by default.

### JWT and Trusted Header Authentication
JSON Web Token (JWT) and trusted header authentication is disabled by default, unless enabled in the DatawaveEvidenceDecoder configuration via the properties `jwtEnabled` and `trustedHeaderEnabled`. The header that the mechanism will extract a trusted subject DN from will be loaded in priority from:
1. The DatawaveHttpAuthenticationMechanism configuration properties `trustedSubjectDnHeader`. If not specified, then:
2. The system properties `dw.trusted.header.subjectDn`. If not specified, then:
3. The header `X-SSL-ClientCert-Subject` will be used. 

The header that the mechanism will extract a trusted issuer DN from will be loaded in priority from:
1. The DatawaveHttpAuthenticationMechanism configuration properties `trustedIssuerDnHeader`. If not specified, then:
2. The system properties `dw.trusted.header.issuerDn`. If not specified, then:
3. The header `X-SSL-ClientCert-Issuer` will be used.


### PKI Certificate Validation
A PKI certificate validator class can be set via the DatawaveSecurityRealm configuration property `certVerifier`. The class must implement `datawave.security.cert.X509CertificateVerifier`. If the class is an instance of `datawave.security.cert.DatawaveCertVerifier`, the OSCP level for the verifier must be set via the DatawaveSecurityRealm configuration property `oscpLevel`.

### Local Roles
In addition to the roles returned by the Datawave user service, local roles can be supplied for authenticated users via a properties file where the keys match either a DatawavePrincipal name, or a DatawaveEvidence username (case-insensitive), and the values are comma-delimited roles. To load these roles, the exact path of the role properties file must be specified via the DatawaveSecurityRealm configuration property `roleProperties`. Any local roles for matching users will be part of their final SecurityIdentity.

### Caching
Both `DatawaveEvidenceDecoder` and `DatawaveSecurityRealm` maintain caches for improving performances when returning DatawavePrincipal and RealmIdentity instances. The maximum size of the caches and the time to live in milliseconds for entries in the cache can be specified via the configuration properties `maxCacheEntries` and `maxCacheAge`  for both DatawaveEvidenceDecoder and DatawaveSecurityRealm. The default values for both is `-1`, and negative values imply no limit.

Additionally, both the DatawaveUser cache in DatawaveEvidenceDecoder and the RealmIdentity cache in DatawaveSecurityRealm will be added to the ElytronCacheManager if available via the configured SecurityEJBProvider from JNDI. These caches can then subsequently be used to fetch DatawaveUsers in the CredentialsCacheBean. 

## Client to Client Authentication
See the class [ClientAuthenticationExample](../security-examples/src/main/java/datawave/security/examples/ClientAuthenticationExample.java) for an example of how to programmatically obtain a SecurityIdentity for executing secured operations from an unsecured context.
