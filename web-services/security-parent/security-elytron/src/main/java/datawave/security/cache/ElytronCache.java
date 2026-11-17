package datawave.security.cache;

import java.util.Set;

import datawave.security.authorization.DatawaveUser;

/**
 * Represents a cache used by Wildfly that contains entries with {@link DatawaveUser} instances.
 */
public interface ElytronCache {

    /**
     * Return the set of all {@link DatawaveUser} instances found in the cache.
     *
     * @return the users
     */
    Set<DatawaveUser> getUsers();

    /**
     * Return the set of all {@link DatawaveUser} instances found in the cache where the name contains the given substring.
     *
     * @param substring
     *            the substring
     * @return the users
     */
    Set<DatawaveUser> getUsersWhereNameContains(String substring);

    /**
     * Return the first {@link DatawaveUser} found with the given name.
     *
     * @param name
     *            the name
     * @return the user
     */
    DatawaveUser getUserWithName(String name);

    /**
     * Evict all users from the cache that have the given name
     *
     * @param name
     *            the name
     */
    void evictUsersWithName(String name);

    /**
     * Clear the cache.
     */
    void clear();
}
