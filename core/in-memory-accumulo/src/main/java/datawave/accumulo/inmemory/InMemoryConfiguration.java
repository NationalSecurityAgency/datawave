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

import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;

import com.google.common.base.Predicate;

class InMemoryConfiguration extends AccumuloConfiguration {
    Map<String,String> map;
    
    InMemoryConfiguration(Map<String,String> settings) {
        map = settings;
    }
    
    public void put(String k, String v) {
        map.put(k, v);
    }
    
    @Override
    public String get(Property property) {
        return map.get(property.getKey());
    }
    
    @Override
    public void getProperties(Map<String,String> props, java.util.function.Predicate<String> filter) {
        map.keySet().forEach(k -> {
            if (filter.test(k)) {
                props.put(k, map.get(k));
            }
        });
    }
    
    @Override
    public boolean isPropertySet(Property property) {
        return map.containsKey(property);
    }
}
