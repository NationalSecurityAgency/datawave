package datawave.security.cache;

import java.util.Set;

import datawave.security.authorization.DatawaveUser;

/**
 * Represents a manager that can delegate and aggregate operations for a collection of {@link ElytronCache} instances.
 */
public interface ElytronCacheManager {

    /**
     * Add a cache to this collection.
     *
     * @param cache
     *            the cache
     */
    void addCache(ElytronCache cache);

    /**
     * Return the set of all {@link DatawaveUser} instances found across all caches in this collection.
     *
     * @return the users
     */
    Set<DatawaveUser> getUsers();

    /**
     * Return the set of all {@link DatawaveUser} instances found across all caches in this collection where the name contains the given substring.
     *
     * @param substring
     *            the substring
     * @return the users
     */
    Set<DatawaveUser> getUsersWhereNameContains(String substring);

    /**
     * Return the first {@link DatawaveUser} found across all caches in this collection with the given name.
     *
     * @param name
     *            the name
     * @return the user
     */
    DatawaveUser getUserWithName(String name);

    /**
     * Evict all users with the given name from all caches in this collection.
     *
     * @param name
     *            the name
     */
    void evictUsersWithName(String name);

    /**
     * Clear all caches in this collection.
     */
    void clear();
}
