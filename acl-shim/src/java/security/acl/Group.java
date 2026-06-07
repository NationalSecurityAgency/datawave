package java.security.acl;

import java.security.Principal;
import java.util.Enumeration;

/**
 * JDK-17 migration shim: re-provides the {@code java.security.acl.Group} interface removed in JDK 14,
 * patched into java.base via --patch-module so the EOL PicketBox login-module API still resolves.
 */
public interface Group extends Principal {
    public boolean addMember(Principal user);

    public boolean removeMember(Principal user);

    public boolean isMember(Principal member);

    public Enumeration<? extends Principal> members();
}
