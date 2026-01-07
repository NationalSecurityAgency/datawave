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
package datawave.accumulo.inmemory;

import java.util.function.Predicate;
import java.util.regex.Pattern;

/**
 * Validators for Accumulo table and namespace names. Replaces non-public class: {@code org.apache.accumulo.core.util.Validators}
 */
public final class AccumuloValidators {

    private AccumuloValidators() {
        // utility class
    }

    // Pattern for valid namespace/table name characters
    private static final Pattern VALID_NAME_PATTERN = Pattern.compile("^[a-zA-Z0-9_]+$");

    /**
     * Validator for new table names. Checks that the name is non-null, non-empty, and contains only valid characters.
     */
    public static final Validator NEW_TABLE_NAME = new Validator("new table name", name -> {
        if (name == null || name.isEmpty()) {
            return false;
        }
        // Allow fully qualified names with dots (namespace.table)
        String tablePart = name.contains(".") ? name.substring(name.lastIndexOf('.') + 1) : name;
        return VALID_NAME_PATTERN.matcher(tablePart).matches();
    });

    /**
     * Validator for new namespace names.
     */
    public static final Validator NEW_NAMESPACE_NAME = new Validator("new namespace name", name -> {
        if (name == null || name.isEmpty()) {
            return false;
        }
        return VALID_NAME_PATTERN.matcher(name).matches();
    });

    /**
     * Validator for existing namespace names (allows empty for default namespace).
     */
    public static final Validator EXISTING_NAMESPACE_NAME = new Validator("existing namespace name", name -> {
        if (name == null) {
            return false;
        }
        // Empty string is valid (default namespace)
        if (name.isEmpty()) {
            return true;
        }
        return VALID_NAME_PATTERN.matcher(name).matches();
    });

    /**
     * A simple validator that checks a predicate and throws IllegalArgumentException if validation fails.
     */
    public static class Validator {
        private final String description;
        private final Predicate<String> predicate;

        Validator(String description, Predicate<String> predicate) {
            this.description = description;
            this.predicate = predicate;
        }

        /**
         * Validates the given value.
         *
         * @param value
         *            the value to validate
         * @throws IllegalArgumentException
         *             if validation fails
         */
        public void validate(String value) {
            if (!predicate.test(value)) {
                throw new IllegalArgumentException("Invalid " + description + ": " + value);
            }
        }
    }
}
