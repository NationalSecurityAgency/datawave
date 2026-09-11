package datawave.security.realm;

import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;

/**
 * Encapsulates mappings of users to roles loaded from a local file. The following is enforced:
 * <ol>
 * <li>User keys will be trimmed and made lowercase.</li>
 * <li>Roles will be trimmed.</li>
 * <li>Blank roles will be dropped.</li>
 * </ol>
 */
class UserRoleMap {

    private final Multimap<String,String> usersToRoles;

    public UserRoleMap() {
        this.usersToRoles = ImmutableMultimap.of();
    }

    /**
     * Create a new {@link UserRoleMap} from the given map of users to roles. The given map will not be modified. The following will be enforced:
     * <ol>
     * <li>Null user keys will be dropped.</li>
     * <li>User keys will be trimmed and made lowercase.</li>
     * <li>Roles will be trimmed.</li>
     * <li>Null and blank roles will be dropped.</li>
     * </ol>
     *
     * @param map
     *            the initial map of users to roles
     */
    public UserRoleMap(Multimap<String,String> map) {
        Multimap<String,String> normalizedMap = HashMultimap.create();
        for (String user : map.keySet()) {
            if (user != null) {
                // @formatter:off
                Set<String> roles = map.get(user).stream()
                                .filter(Objects::nonNull)
                                .map(String::trim)
                                .filter(s -> !s.isBlank())
                                .collect(Collectors.toSet());
                // @formatter:on
                if (!roles.isEmpty()) {
                    normalizedMap.putAll(user.trim().toLowerCase(), roles);
                }
            }
        }
        this.usersToRoles = ImmutableMultimap.copyOf(normalizedMap);
    }

    /**
     * Return whether this {@link UserRoleMap} is empty.
     *
     * @return true if this {@link UserRoleMap} contains no mappings, or false otherwise
     */
    public boolean isEmpty() {
        return usersToRoles.isEmpty();
    }

    /**
     * Return the roles mapped to the given user. Lookup by user is case-insensitive.
     *
     * @param user
     *            the user
     * @return a collection of roles for the user, possibly empty, but never null
     */
    public Collection<String> get(String user) {
        return usersToRoles.get(user.toLowerCase());
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        UserRoleMap that = (UserRoleMap) o;
        return Objects.equals(usersToRoles, that.usersToRoles);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(usersToRoles);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", UserRoleMap.class.getSimpleName() + "[", "]").add("usersToRoles=" + usersToRoles).toString();
    }
}
