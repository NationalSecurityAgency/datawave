/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package datawave.util;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Null-safe utility methods to prevent NullPointerExceptions and fix string comparison issues.
 * <p>
 * This class addresses multiple SonarQube issues:
 * <ul>
 *   <li>Potential NPE vulnerabilities</li>
 *   <li>"use equals() instead of == for strings &amp; Boxed types"</li>
 * </ul>
 * </p>
 * <p>
 * For DataWave Issue #2321: Fix Major issues in SonarQube
 * </p>
 * 
 * <h2>Migration Examples</h2>
 * 
 * <h3>Before (NPE Risk):</h3>
 * <pre>{@code
 * String data = result.getData();
 * if (data.length() > 0) { ... }  // NPE if getData() returns null!
 * }</pre>
 * 
 * <h3>After (Null-Safe):</h3>
 * <pre>{@code
 * if (NullSafeUtils.isNotEmpty(result.getData())) { ... }
 * }</pre>
 * 
 * <h3>Before (Reference Comparison):</h3>
 * <pre>{@code
 * if (role == "ADMIN") { ... }  // WRONG: compares references
 * if (flag == Boolean.TRUE) { ... }  // WRONG for boxed types
 * }</pre>
 * 
 * <h3>After (Value Comparison):</h3>
 * <pre>{@code
 * if (NullSafeUtils.equals(role, "ADMIN")) { ... }
 * if (NullSafeUtils.isTrue(flag)) { ... }
 * }</pre>
 */
public final class NullSafeUtils {
    
    private NullSafeUtils() {
        // Utility class - prevent instantiation
    }
    
    // ==================== String Operations ====================
    
    /**
     * Null-safe check if a string is null or empty.
     *
     * @param str the string to check
     * @return true if null or empty
     */
    public static boolean isEmpty(String str) {
        return str == null || str.isEmpty();
    }
    
    /**
     * Null-safe check if a string is not null and not empty.
     *
     * @param str the string to check
     * @return true if not null and not empty
     */
    public static boolean isNotEmpty(String str) {
        return str != null && !str.isEmpty();
    }
    
    /**
     * Null-safe check if a string is null, empty, or contains only whitespace.
     *
     * @param str the string to check
     * @return true if null, empty, or whitespace only
     */
    public static boolean isBlank(String str) {
        if (str == null) {
            return true;
        }
        for (int i = 0; i < str.length(); i++) {
            if (!Character.isWhitespace(str.charAt(i))) {
                return false;
            }
        }
        return true;
    }
    
    /**
     * Null-safe check if a string is not blank.
     *
     * @param str the string to check
     * @return true if contains non-whitespace characters
     */
    public static boolean isNotBlank(String str) {
        return !isBlank(str);
    }
    
    /**
     * Null-safe string trim.
     *
     * @param str the string to trim
     * @return trimmed string or null if input was null
     */
    public static String trim(String str) {
        return str == null ? null : str.trim();
    }
    
    /**
     * Null-safe trim that returns empty string for null.
     *
     * @param str the string to trim
     * @return trimmed string or empty string if null
     */
    public static String trimToEmpty(String str) {
        return str == null ? "" : str.trim();
    }
    
    /**
     * Null-safe trim that returns null for blank strings.
     *
     * @param str the string to trim
     * @return trimmed string, or null if blank
     */
    public static String trimToNull(String str) {
        String trimmed = trim(str);
        return isBlank(trimmed) ? null : trimmed;
    }
    
    /**
     * Return the string or a default if null.
     *
     * @param str the string to check
     * @param defaultValue the default to return if null
     * @return the string or default
     */
    public static String defaultIfNull(String str, String defaultValue) {
        return str == null ? defaultValue : str;
    }
    
    /**
     * Return the string or empty string if null.
     *
     * @param str the string to check
     * @return the string or empty string
     */
    public static String defaultIfNull(String str) {
        return defaultIfNull(str, "");
    }
    
    /**
     * Return the string or a default if blank.
     *
     * @param str the string to check
     * @param defaultValue the default to return if blank
     * @return the string or default
     */
    public static String defaultIfBlank(String str, String defaultValue) {
        return isBlank(str) ? defaultValue : str;
    }
    
    /**
     * Null-safe string length.
     *
     * @param str the string
     * @return the length, or 0 if null
     */
    public static int length(String str) {
        return str == null ? 0 : str.length();
    }
    
    // ==================== Equality Operations ====================
    
    /**
     * Null-safe equals comparison using Objects.equals().
     * <p>
     * This fixes the SonarQube issue: "use equals() instead of == for strings &amp; Boxed types"
     * </p>
     *
     * @param a first object
     * @param b second object
     * @return true if both null or both equal via equals()
     */
    public static boolean equals(Object a, Object b) {
        return Objects.equals(a, b);
    }
    
    /**
     * Null-safe string equals, ignoring case.
     *
     * @param a first string
     * @param b second string
     * @return true if equal ignoring case, or both null
     */
    public static boolean equalsIgnoreCase(String a, String b) {
        if (a == b) {
            return true;
        }
        if (a == null || b == null) {
            return false;
        }
        return a.equalsIgnoreCase(b);
    }
    
    /**
     * Compare string to a constant safely (constant-first pattern).
     * <p>
     * This pattern prevents NPE by putting the known non-null constant first.
     * </p>
     * 
     * <pre>{@code
     * // Instead of: role.equals("ADMIN")  // NPE if role is null
     * // Use: NullSafeUtils.equalsConstant("ADMIN", role)
     * }</pre>
     *
     * @param constant the non-null constant
     * @param value the potentially null value
     * @return true if equal
     * @throws NullPointerException if constant is null
     */
    public static boolean equalsConstant(String constant, String value) {
        Objects.requireNonNull(constant, "Constant cannot be null");
        return constant.equals(value);
    }
    
    /**
     * Compare string to a constant safely, ignoring case.
     *
     * @param constant the non-null constant
     * @param value the potentially null value
     * @return true if equal ignoring case
     * @throws NullPointerException if constant is null
     */
    public static boolean equalsConstantIgnoreCase(String constant, String value) {
        Objects.requireNonNull(constant, "Constant cannot be null");
        return constant.equalsIgnoreCase(value);
    }
    
    // ==================== Boxed Type Operations ====================
    
    /**
     * Null-safe Boolean check (treats null as false).
     * <p>
     * Fixes: "use equals() instead of == for Boxed types"
     * </p>
     *
     * @param value the Boolean to check
     * @return true only if value is Boolean.TRUE
     */
    public static boolean isTrue(Boolean value) {
        return Boolean.TRUE.equals(value);
    }
    
    /**
     * Null-safe Boolean check for false.
     *
     * @param value the Boolean to check
     * @return true only if value is Boolean.FALSE (not null)
     */
    public static boolean isFalse(Boolean value) {
        return Boolean.FALSE.equals(value);
    }
    
    /**
     * Check if Boolean is not true (false or null).
     *
     * @param value the Boolean to check
     * @return true if value is null or FALSE
     */
    public static boolean isNotTrue(Boolean value) {
        return !isTrue(value);
    }
    
    /**
     * Check if Boolean is not false (true or null).
     *
     * @param value the Boolean to check
     * @return true if value is null or TRUE
     */
    public static boolean isNotFalse(Boolean value) {
        return !isFalse(value);
    }
    
    /**
     * Null-safe Boolean to primitive conversion.
     *
     * @param value the Boolean (may be null)
     * @param defaultValue the default if null
     * @return the boolean value or default
     */
    public static boolean toBoolean(Boolean value, boolean defaultValue) {
        return value == null ? defaultValue : value;
    }
    
    /**
     * Null-safe Integer to int conversion.
     *
     * @param value the Integer (may be null)
     * @param defaultValue the default if null
     * @return the int value or default
     */
    public static int toInt(Integer value, int defaultValue) {
        return value == null ? defaultValue : value;
    }
    
    /**
     * Null-safe Integer to int conversion with zero default.
     *
     * @param value the Integer (may be null)
     * @return the int value or 0
     */
    public static int toInt(Integer value) {
        return toInt(value, 0);
    }
    
    /**
     * Null-safe Long to long conversion.
     *
     * @param value the Long (may be null)
     * @param defaultValue the default if null
     * @return the long value or default
     */
    public static long toLong(Long value, long defaultValue) {
        return value == null ? defaultValue : value;
    }
    
    /**
     * Null-safe Long to long conversion with zero default.
     *
     * @param value the Long (may be null)
     * @return the long value or 0
     */
    public static long toLong(Long value) {
        return toLong(value, 0L);
    }
    
    /**
     * Null-safe Double to double conversion.
     *
     * @param value the Double (may be null)
     * @param defaultValue the default if null
     * @return the double value or default
     */
    public static double toDouble(Double value, double defaultValue) {
        return value == null ? defaultValue : value;
    }
    
    // ==================== Collection Operations ====================
    
    /**
     * Check if a collection is null or empty.
     *
     * @param collection the collection to check
     * @return true if null or empty
     */
    public static boolean isEmpty(Collection<?> collection) {
        return collection == null || collection.isEmpty();
    }
    
    /**
     * Check if a collection is not null and not empty.
     *
     * @param collection the collection to check
     * @return true if not null and not empty
     */
    public static boolean isNotEmpty(Collection<?> collection) {
        return collection != null && !collection.isEmpty();
    }
    
    /**
     * Check if a map is null or empty.
     *
     * @param map the map to check
     * @return true if null or empty
     */
    public static boolean isEmpty(Map<?, ?> map) {
        return map == null || map.isEmpty();
    }
    
    /**
     * Check if a map is not null and not empty.
     *
     * @param map the map to check
     * @return true if not null and not empty
     */
    public static boolean isNotEmpty(Map<?, ?> map) {
        return map != null && !map.isEmpty();
    }
    
    /**
     * Get size of a collection, returning 0 for null.
     *
     * @param collection the collection
     * @return the size, or 0 if null
     */
    public static int size(Collection<?> collection) {
        return collection == null ? 0 : collection.size();
    }
    
    /**
     * Get size of a map, returning 0 for null.
     *
     * @param map the map
     * @return the size, or 0 if null
     */
    public static int size(Map<?, ?> map) {
        return map == null ? 0 : map.size();
    }
    
    // ==================== Object Operations ====================
    
    /**
     * Get a value or default if null.
     *
     * @param value the value to check
     * @param defaultValue the default if null
     * @param <T> the type
     * @return the value or default
     */
    public static <T> T getOrDefault(T value, T defaultValue) {
        return value == null ? defaultValue : value;
    }
    
    /**
     * Get a value or compute default if null.
     *
     * @param value the value to check
     * @param defaultSupplier supplier for the default value
     * @param <T> the type
     * @return the value or computed default
     */
    public static <T> T getOrCompute(T value, Supplier<T> defaultSupplier) {
        return value == null ? defaultSupplier.get() : value;
    }
    
    /**
     * Apply a function if the input is not null.
     *
     * @param input the input object (may be null)
     * @param mapper the function to apply
     * @param <T> input type
     * @param <R> result type
     * @return the result or null if input was null
     */
    public static <T, R> R mapIfNotNull(T input, Function<T, R> mapper) {
        return input == null ? null : mapper.apply(input);
    }
    
    /**
     * Apply a function if input is not null, with default for null.
     *
     * @param input the input object (may be null)
     * @param mapper the function to apply
     * @param defaultValue value to return if input is null
     * @param <T> input type
     * @param <R> result type
     * @return the result or default
     */
    public static <T, R> R mapOrDefault(T input, Function<T, R> mapper, R defaultValue) {
        return input == null ? defaultValue : mapper.apply(input);
    }
    
    /**
     * Execute action if value is not null.
     *
     * @param value the value to check
     * @param action the action to execute
     * @param <T> the type
     */
    public static <T> void ifNotNull(T value, Consumer<T> action) {
        if (value != null) {
            action.accept(value);
        }
    }
    
    /**
     * Execute one action if not null, another if null.
     *
     * @param value the value to check
     * @param ifPresent action if not null
     * @param ifNull action if null
     * @param <T> the type
     */
    public static <T> void ifNotNullOrElse(T value, Consumer<T> ifPresent, Runnable ifNull) {
        if (value != null) {
            ifPresent.accept(value);
        } else {
            ifNull.run();
        }
    }
    
    /**
     * Require non-null with custom message.
     *
     * @param obj the object to check
     * @param message the error message
     * @param <T> the type
     * @return the object if not null
     * @throws NullPointerException if null
     */
    public static <T> T requireNonNull(T obj, String message) {
        return Objects.requireNonNull(obj, message);
    }
    
    /**
     * Require non-null with message supplier.
     *
     * @param obj the object to check
     * @param messageSupplier supplier for the error message
     * @param <T> the type
     * @return the object if not null
     * @throws NullPointerException if null
     */
    public static <T> T requireNonNull(T obj, Supplier<String> messageSupplier) {
        return Objects.requireNonNull(obj, messageSupplier);
    }
    
    /**
     * Check if object matches predicate, treating null as non-match.
     *
     * @param obj the object to test (may be null)
     * @param predicate the test to apply
     * @param <T> the type
     * @return true if obj is non-null and matches predicate
     */
    public static <T> boolean matchesIfPresent(T obj, Predicate<T> predicate) {
        return obj != null && predicate.test(obj);
    }
    
    /**
     * Wrap a potentially null value in Optional.
     *
     * @param value the value (may be null)
     * @param <T> the type
     * @return Optional containing the value or empty
     */
    public static <T> Optional<T> toOptional(T value) {
        return Optional.ofNullable(value);
    }
    
    /**
     * Return first non-null value.
     *
     * @param values the values to check
     * @param <T> the type
     * @return first non-null value, or null if all null
     */
    @SafeVarargs
    public static <T> T firstNonNull(T... values) {
        for (T value : values) {
            if (value != null) {
                return value;
            }
        }
        return null;
    }
    
    /**
     * Check if all values are non-null.
     *
     * @param values the values to check
     * @return true if all non-null
     */
    public static boolean allNonNull(Object... values) {
        for (Object value : values) {
            if (value == null) {
                return false;
            }
        }
        return true;
    }
    
    /**
     * Check if any value is null.
     *
     * @param values the values to check
     * @return true if any is null
     */
    public static boolean anyNull(Object... values) {
        return !allNonNull(values);
    }
}
