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

import java.util.regex.Pattern;

/**
 * Validators for Accumulo table and namespace names. Replaces non-public class: {@code org.apache.accumulo.core.util.Validators}
 */
public final class AccumuloValidators {

    private AccumuloValidators() {
        throw new UnsupportedOperationException();
    }

    private static final int MAX_NAME_LENGTH = 1024;

    // Pattern for valid namespace/table name characters
    private static final Pattern VALID_NAME_PATTERN = Pattern.compile("\\w+");

    /**
     * Validator for new table names. Checks that the name is non-null, non-empty, does not start with a dot, is not longer than 1024 characters, and contains
     * only valid characters. For qualified names (namespace.table), both parts must be non-blank and match the valid name pattern.
     */
    public static final Validator NEW_TABLE_NAME = new Validator("new table name", name -> {
        if (name == null || name.isEmpty()) {
            return false;
        }
        if (name.startsWith(".")) {
            return false;
        }
        if (name.length() > MAX_NAME_LENGTH) {
            return false;
        }
        if (name.contains(".")) {
            String namespacePart = name.substring(0, name.lastIndexOf('.'));
            String tablePart = name.substring(name.lastIndexOf('.') + 1);
            if (namespacePart.isEmpty() || tablePart.isEmpty()) {
                return false;
            }
            return VALID_NAME_PATTERN.matcher(namespacePart).matches() && VALID_NAME_PATTERN.matcher(tablePart).matches();
        }
        return VALID_NAME_PATTERN.matcher(name).matches();
    });

    /**
     * Validator for new namespace names.
     */
    public static final Validator NEW_NAMESPACE_NAME = new Validator("new namespace name", name -> {
        if (name == null || name.isEmpty()) {
            return false;
        }
        if (name.length() > MAX_NAME_LENGTH) {
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
        if (name.length() > MAX_NAME_LENGTH) {
            return false;
        }
        return VALID_NAME_PATTERN.matcher(name).matches();
    });
}
