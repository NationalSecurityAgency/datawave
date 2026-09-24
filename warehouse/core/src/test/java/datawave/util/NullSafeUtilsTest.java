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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive tests for NullSafeUtils.
 */
@DisplayName("NullSafeUtils Tests")
class NullSafeUtilsTest {
    
    // ==================== String Operations Tests ====================
    
    @Test
    @DisplayName("isEmpty: returns true for null and empty strings")
    void testIsEmpty() {
        assertTrue(NullSafeUtils.isEmpty(null));
        assertTrue(NullSafeUtils.isEmpty(""));
        assertFalse(NullSafeUtils.isEmpty(" "));
        assertFalse(NullSafeUtils.isEmpty("hello"));
    }
    
    @Test
    @DisplayName("isNotEmpty: returns true only for non-null non-empty strings")
    void testIsNotEmpty() {
        assertFalse(NullSafeUtils.isNotEmpty(null));
        assertFalse(NullSafeUtils.isNotEmpty(""));
        assertTrue(NullSafeUtils.isNotEmpty(" "));
        assertTrue(NullSafeUtils.isNotEmpty("hello"));
    }
    
    @Test
    @DisplayName("isBlank: returns true for null, empty, and whitespace-only strings")
    void testIsBlank() {
        assertTrue(NullSafeUtils.isBlank(null));
        assertTrue(NullSafeUtils.isBlank(""));
        assertTrue(NullSafeUtils.isBlank(" "));
        assertTrue(NullSafeUtils.isBlank("  \t\n  "));
        assertFalse(NullSafeUtils.isBlank("hello"));
        assertFalse(NullSafeUtils.isBlank(" hello "));
    }
    
    @Test
    @DisplayName("isNotBlank: returns true only for strings with non-whitespace content")
    void testIsNotBlank() {
        assertFalse(NullSafeUtils.isNotBlank(null));
        assertFalse(NullSafeUtils.isNotBlank(""));
        assertFalse(NullSafeUtils.isNotBlank(" "));
        assertTrue(NullSafeUtils.isNotBlank("hello"));
        assertTrue(NullSafeUtils.isNotBlank(" hello "));
    }
    
    @Test
    @DisplayName("trim: handles null safely")
    void testTrim() {
        assertNull(NullSafeUtils.trim(null));
        assertEquals("", NullSafeUtils.trim(""));
        assertEquals("", NullSafeUtils.trim("  "));
        assertEquals("hello", NullSafeUtils.trim(" hello "));
    }
    
    @Test
    @DisplayName("trimToEmpty: returns empty string for null")
    void testTrimToEmpty() {
        assertEquals("", NullSafeUtils.trimToEmpty(null));
        assertEquals("", NullSafeUtils.trimToEmpty(""));
        assertEquals("", NullSafeUtils.trimToEmpty("  "));
        assertEquals("hello", NullSafeUtils.trimToEmpty(" hello "));
    }
    
    @Test
    @DisplayName("trimToNull: returns null for blank strings")
    void testTrimToNull() {
        assertNull(NullSafeUtils.trimToNull(null));
        assertNull(NullSafeUtils.trimToNull(""));
        assertNull(NullSafeUtils.trimToNull("  "));
        assertEquals("hello", NullSafeUtils.trimToNull(" hello "));
    }
    
    @Test
    @DisplayName("defaultIfNull: returns default for null strings")
    void testDefaultIfNull() {
        assertEquals("default", NullSafeUtils.defaultIfNull(null, "default"));
        assertEquals("value", NullSafeUtils.defaultIfNull("value", "default"));
        assertEquals("", NullSafeUtils.defaultIfNull(null));
    }
    
    @Test
    @DisplayName("defaultIfBlank: returns default for blank strings")
    void testDefaultIfBlank() {
        assertEquals("default", NullSafeUtils.defaultIfBlank(null, "default"));
        assertEquals("default", NullSafeUtils.defaultIfBlank("", "default"));
        assertEquals("default", NullSafeUtils.defaultIfBlank("  ", "default"));
        assertEquals("value", NullSafeUtils.defaultIfBlank("value", "default"));
    }
    
    @Test
    @DisplayName("length: returns 0 for null")
    void testLength() {
        assertEquals(0, NullSafeUtils.length(null));
        assertEquals(0, NullSafeUtils.length(""));
        assertEquals(5, NullSafeUtils.length("hello"));
    }
    
    // ==================== Equality Tests ====================
    
    @Test
    @DisplayName("equals: null-safe comparison")
    void testEquals() {
        assertTrue(NullSafeUtils.equals(null, null));
        assertFalse(NullSafeUtils.equals(null, "hello"));
        assertFalse(NullSafeUtils.equals("hello", null));
        assertTrue(NullSafeUtils.equals("hello", "hello"));
        assertFalse(NullSafeUtils.equals("hello", "world"));
        
        // Works with boxed types too
        assertTrue(NullSafeUtils.equals(Integer.valueOf(5), Integer.valueOf(5)));
        assertFalse(NullSafeUtils.equals(Integer.valueOf(5), Integer.valueOf(6)));
    }
    
    @Test
    @DisplayName("equalsIgnoreCase: case-insensitive null-safe comparison")
    void testEqualsIgnoreCase() {
        assertTrue(NullSafeUtils.equalsIgnoreCase(null, null));
        assertFalse(NullSafeUtils.equalsIgnoreCase(null, "hello"));
        assertFalse(NullSafeUtils.equalsIgnoreCase("hello", null));
        assertTrue(NullSafeUtils.equalsIgnoreCase("hello", "HELLO"));
        assertTrue(NullSafeUtils.equalsIgnoreCase("Hello", "hElLo"));
        assertFalse(NullSafeUtils.equalsIgnoreCase("hello", "world"));
    }
    
    @Test
    @DisplayName("equalsConstant: constant-first pattern prevents NPE")
    void testEqualsConstant() {
        assertTrue(NullSafeUtils.equalsConstant("ADMIN", "ADMIN"));
        assertFalse(NullSafeUtils.equalsConstant("ADMIN", "USER"));
        assertFalse(NullSafeUtils.equalsConstant("ADMIN", null));
        
        assertThrows(NullPointerException.class, 
            () -> NullSafeUtils.equalsConstant(null, "value"));
    }
    
    @Test
    @DisplayName("equalsConstantIgnoreCase: case-insensitive constant comparison")
    void testEqualsConstantIgnoreCase() {
        assertTrue(NullSafeUtils.equalsConstantIgnoreCase("ADMIN", "admin"));
        assertFalse(NullSafeUtils.equalsConstantIgnoreCase("ADMIN", null));
    }
    
    // ==================== Boxed Type Tests ====================
    
    @Test
    @DisplayName("isTrue: null-safe Boolean check")
    void testIsTrue() {
        assertTrue(NullSafeUtils.isTrue(Boolean.TRUE));
        assertFalse(NullSafeUtils.isTrue(Boolean.FALSE));
        assertFalse(NullSafeUtils.isTrue(null));
    }
    
    @Test
    @DisplayName("isFalse: null-safe Boolean check")
    void testIsFalse() {
        assertTrue(NullSafeUtils.isFalse(Boolean.FALSE));
        assertFalse(NullSafeUtils.isFalse(Boolean.TRUE));
        assertFalse(NullSafeUtils.isFalse(null));
    }
    
    @Test
    @DisplayName("isNotTrue: returns true for FALSE and null")
    void testIsNotTrue() {
        assertFalse(NullSafeUtils.isNotTrue(Boolean.TRUE));
        assertTrue(NullSafeUtils.isNotTrue(Boolean.FALSE));
        assertTrue(NullSafeUtils.isNotTrue(null));
    }
    
    @Test
    @DisplayName("isNotFalse: returns true for TRUE and null")
    void testIsNotFalse() {
        assertTrue(NullSafeUtils.isNotFalse(Boolean.TRUE));
        assertFalse(NullSafeUtils.isNotFalse(Boolean.FALSE));
        assertTrue(NullSafeUtils.isNotFalse(null));
    }
    
    @Test
    @DisplayName("toBoolean: converts with default")
    void testToBoolean() {
        assertTrue(NullSafeUtils.toBoolean(Boolean.TRUE, false));
        assertFalse(NullSafeUtils.toBoolean(Boolean.FALSE, true));
        assertTrue(NullSafeUtils.toBoolean(null, true));
        assertFalse(NullSafeUtils.toBoolean(null, false));
    }
    
    @Test
    @DisplayName("toInt: converts with default")
    void testToInt() {
        assertEquals(5, NullSafeUtils.toInt(Integer.valueOf(5), 10));
        assertEquals(10, NullSafeUtils.toInt(null, 10));
        assertEquals(0, NullSafeUtils.toInt(null));
    }
    
    @Test
    @DisplayName("toLong: converts with default")
    void testToLong() {
        assertEquals(5L, NullSafeUtils.toLong(Long.valueOf(5), 10L));
        assertEquals(10L, NullSafeUtils.toLong(null, 10L));
        assertEquals(0L, NullSafeUtils.toLong(null));
    }
    
    @Test
    @DisplayName("toDouble: converts with default")
    void testToDouble() {
        assertEquals(5.5, NullSafeUtils.toDouble(Double.valueOf(5.5), 10.0), 0.001);
        assertEquals(10.0, NullSafeUtils.toDouble(null, 10.0), 0.001);
    }
    
    // ==================== Collection Tests ====================
    
    @Test
    @DisplayName("isEmpty(Collection): null-safe check")
    void testIsEmptyCollection() {
        assertTrue(NullSafeUtils.isEmpty((Collection<?>) null));
        assertTrue(NullSafeUtils.isEmpty(new ArrayList<>()));
        assertFalse(NullSafeUtils.isEmpty(Arrays.asList("item")));
    }
    
    @Test
    @DisplayName("isNotEmpty(Collection): null-safe check")
    void testIsNotEmptyCollection() {
        assertFalse(NullSafeUtils.isNotEmpty((Collection<?>) null));
        assertFalse(NullSafeUtils.isNotEmpty(new ArrayList<>()));
        assertTrue(NullSafeUtils.isNotEmpty(Arrays.asList("item")));
    }
    
    @Test
    @DisplayName("isEmpty(Map): null-safe check")
    void testIsEmptyMap() {
        assertTrue(NullSafeUtils.isEmpty((Map<?, ?>) null));
        assertTrue(NullSafeUtils.isEmpty(new HashMap<>()));
        Map<String, String> map = new HashMap<>();
        map.put("key", "value");
        assertFalse(NullSafeUtils.isEmpty(map));
    }
    
    @Test
    @DisplayName("size: returns 0 for null collections")
    void testSizeCollection() {
        assertEquals(0, NullSafeUtils.size((Collection<?>) null));
        assertEquals(0, NullSafeUtils.size(new ArrayList<>()));
        assertEquals(2, NullSafeUtils.size(Arrays.asList("a", "b")));
    }
    
    @Test
    @DisplayName("size: returns 0 for null maps")
    void testSizeMap() {
        assertEquals(0, NullSafeUtils.size((Map<?, ?>) null));
        assertEquals(0, NullSafeUtils.size(new HashMap<>()));
    }
    
    // ==================== Object Operation Tests ====================
    
    @Test
    @DisplayName("getOrDefault: returns default for null")
    void testGetOrDefault() {
        assertEquals("default", NullSafeUtils.getOrDefault(null, "default"));
        assertEquals("value", NullSafeUtils.getOrDefault("value", "default"));
    }
    
    @Test
    @DisplayName("getOrCompute: computes default only when null")
    void testGetOrCompute() {
        int[] computeCount = {0};
        
        assertEquals("value", NullSafeUtils.getOrCompute("value", () -> {
            computeCount[0]++;
            return "computed";
        }));
        assertEquals(0, computeCount[0], "Supplier should not be called");
        
        assertEquals("computed", NullSafeUtils.getOrCompute(null, () -> {
            computeCount[0]++;
            return "computed";
        }));
        assertEquals(1, computeCount[0], "Supplier should be called once");
    }
    
    @Test
    @DisplayName("mapIfNotNull: applies function only to non-null")
    void testMapIfNotNull() {
        assertNull(NullSafeUtils.mapIfNotNull(null, String::toUpperCase));
        assertEquals("HELLO", NullSafeUtils.mapIfNotNull("hello", String::toUpperCase));
    }
    
    @Test
    @DisplayName("mapOrDefault: returns default for null input")
    void testMapOrDefault() {
        assertEquals("default", NullSafeUtils.mapOrDefault(null, String::toUpperCase, "default"));
        assertEquals("HELLO", NullSafeUtils.mapOrDefault("hello", String::toUpperCase, "default"));
    }
    
    @Test
    @DisplayName("ifNotNull: executes only for non-null")
    void testIfNotNull() {
        int[] callCount = {0};
        
        NullSafeUtils.ifNotNull(null, s -> callCount[0]++);
        assertEquals(0, callCount[0]);
        
        NullSafeUtils.ifNotNull("value", s -> callCount[0]++);
        assertEquals(1, callCount[0]);
    }
    
    @Test
    @DisplayName("ifNotNullOrElse: executes appropriate branch")
    void testIfNotNullOrElse() {
        int[] presentCount = {0};
        int[] nullCount = {0};
        
        NullSafeUtils.ifNotNullOrElse("value", 
            s -> presentCount[0]++, 
            () -> nullCount[0]++);
        assertEquals(1, presentCount[0]);
        assertEquals(0, nullCount[0]);
        
        NullSafeUtils.ifNotNullOrElse(null, 
            s -> presentCount[0]++, 
            () -> nullCount[0]++);
        assertEquals(1, presentCount[0]);
        assertEquals(1, nullCount[0]);
    }
    
    @Test
    @DisplayName("requireNonNull: throws with message for null")
    void testRequireNonNull() {
        assertEquals("value", NullSafeUtils.requireNonNull("value", "message"));
        
        NullPointerException ex = assertThrows(NullPointerException.class, 
            () -> NullSafeUtils.requireNonNull(null, "custom message"));
        assertEquals("custom message", ex.getMessage());
    }
    
    @Test
    @DisplayName("matchesIfPresent: returns false for null")
    void testMatchesIfPresent() {
        assertFalse(NullSafeUtils.matchesIfPresent(null, s -> true));
        assertTrue(NullSafeUtils.matchesIfPresent("hello", s -> s.length() > 3));
        assertFalse(NullSafeUtils.matchesIfPresent("hi", s -> s.length() > 3));
    }
    
    @Test
    @DisplayName("toOptional: wraps in Optional")
    void testToOptional() {
        assertTrue(NullSafeUtils.toOptional(null).isEmpty());
        assertEquals("value", NullSafeUtils.toOptional("value").orElse("default"));
    }
    
    @Test
    @DisplayName("firstNonNull: returns first non-null value")
    void testFirstNonNull() {
        assertEquals("first", NullSafeUtils.firstNonNull("first", "second"));
        assertEquals("second", NullSafeUtils.firstNonNull(null, "second"));
        assertEquals("third", NullSafeUtils.firstNonNull(null, null, "third"));
        assertNull(NullSafeUtils.firstNonNull(null, null, null));
    }
    
    @Test
    @DisplayName("allNonNull: checks all values")
    void testAllNonNull() {
        assertTrue(NullSafeUtils.allNonNull("a", "b", "c"));
        assertFalse(NullSafeUtils.allNonNull("a", null, "c"));
        assertFalse(NullSafeUtils.allNonNull(null, null, null));
    }
    
    @Test
    @DisplayName("anyNull: checks for any null")
    void testAnyNull() {
        assertFalse(NullSafeUtils.anyNull("a", "b", "c"));
        assertTrue(NullSafeUtils.anyNull("a", null, "c"));
        assertTrue(NullSafeUtils.anyNull(null, null, null));
    }
}
