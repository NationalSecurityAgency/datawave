package datawave.security.websocket;

import java.security.Principal;
import java.security.acl.Group;

import javax.interceptor.AroundInvoke;
import javax.interceptor.InvocationContext;
import javax.security.auth.Subject;
import javax.websocket.Session;
import javax.websocket.server.ServerEndpoint;

import org.jboss.security.SecurityContext;
import org.jboss.security.SecurityContextAssociation;
import org.jboss.security.identity.Identity;
import org.jboss.security.identity.Role;
import org.jboss.security.identity.extensions.CredentialIdentityFactory;
import org.jboss.security.identity.plugins.SimpleRoleGroup;

/**
 * A JBoss AS/Wildfly-specific interceptor that looks for saved security context information in the WebSocket user {@link Session} and uses that information to
 * execute a WebSocket handler method in the saved security context. This workaround is necessary to cover a shortcoming in the WebSocket specification. See
 * <a href="https://java.net/jira/browse/WEBSOCKET_SPEC-238">WEBSOCKET_SPEC-238</a> for more details.
 * <p>
 * To use this interceptor, annotate your {@link ServerEndpoint}-annotated class with
 * 
 * <pre>
 * <code>
 *     {@literal @}Interceptors({ WebsocketSecurityInterceptor.class })
 * </code>
 * </pre>
 * 
 * and also ensure that the server enpoint annotation sets {@link WebsocketSecurityConfigurator} as the {@link ServerEndpoint#configurator()} class.
 */
public class WebsocketSecurityInterceptor {
    public static final String SESSION_PRINCIPAL = "websocket.security.principal";
    public static final String SESSION_SUBJECT = "websocket.security.subject";
    public static final String SESSION_CREDENTIAL = "websocket.security.credential";
    
    @AroundInvoke
    public Object intercept(InvocationContext ctx) throws Exception {
        Session session = findSessionParameter(ctx);
        if (session != null) {
            final Principal principal = (Principal) session.getUserProperties().get(SESSION_PRINCIPAL);
            final Subject subject = (Subject) session.getUserProperties().get(SESSION_SUBJECT);
            final Object credential = session.getUserProperties().get(SESSION_CREDENTIAL);
            
            if (principal != null && subject != null) {
                setSubjectInfo(principal, subject, credential);
            }
        }
        
        return ctx.proceed();
    }
    
    protected Session findSessionParameter(InvocationContext ctx) {
        Session session = null;
        for (Object param : ctx.getParameters()) {
            if (param instanceof Session) {
                session = (Session) param;
                break;
            }
        }
        return session;
    }
    
    protected void setSubjectInfo(final Principal principal, final Subject subject, final Object credential) {
        SecurityContext securityContext = SecurityContextAssociation.getSecurityContext();
        Role roleGroup = getRoleGroup(subject);
        Identity identity = CredentialIdentityFactory.createIdentity(principal, credential, roleGroup);
        securityContext.getUtil().createSubjectInfo(identity, subject);
    }
    
    protected Role getRoleGroup(final Subject subject) {
        Role roleGroup = null;
        for (Group group : subject.getPrincipals(Group.class)) {
            if ("Roles".equals(group.getName())) {
                roleGroup = new SimpleRoleGroup(group);
                break;
            }
        }
        return roleGroup;
    }
}
