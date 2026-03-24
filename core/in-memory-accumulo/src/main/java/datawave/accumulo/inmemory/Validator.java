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

import java.util.Optional;
import java.util.function.Function;

/**
 * A validator that checks a value and throws IllegalArgumentException with a detailed error description if validation fails. Modeled after Accumulo's internal
 * Validator pattern.
 */
public class Validator {
    private final String description;
    private final Function<String,Optional<String>> validation;

    Validator(String description, Function<String,Optional<String>> validation) {
        this.description = description;
        this.validation = validation;
    }

    /**
     * Validates the given value.
     *
     * @param value
     *            the value to validate
     * @throws IllegalArgumentException
     *             if validation fails, with a detailed error description
     */
    public void validate(String value) {
        Optional<String> error = validation.apply(value);
        if (error.isPresent()) {
            throw new IllegalArgumentException("Invalid " + description + ": " + error.get());
        }
    }
}
