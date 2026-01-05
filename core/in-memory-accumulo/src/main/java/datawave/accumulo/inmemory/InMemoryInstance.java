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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

/**
 * InMemory Accumulo provides an in memory implementation of the Accumulo client API. It is possible that the behavior of this implementation may differ subtly
 * from the behavior of Accumulo. This could result in unit tests that pass on InMemory Accumulo and fail on Accumulo or visa-versa. Documenting the differences
 * would be difficult and is not done.
 *
 * <p>
 * An alternative to InMemory Accumulo called MiniAccumuloCluster was introduced in Accumulo 1.5. MiniAccumuloCluster spins up actual Accumulo server processes,
 * can be used for unit testing, and its behavior should match Accumulo. The drawback of MiniAccumuloCluster is that it starts more slowly than InMemory
 * Accumulo.
 *
 */
public class InMemoryInstance {

    static final String genericAddress = "localhost:1234";
    static final Map<String,InMemoryAccumulo> instances = new HashMap<>();
    InMemoryAccumulo acu;
    String instanceName;

    public InMemoryInstance() {
        acu = new InMemoryAccumulo(getDefaultFileSystem());
        instanceName = "mock-instance";
    }

    static FileSystem getDefaultFileSystem() {
        try {
            Configuration conf = CachedConfiguration.getInstance();
            conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
            conf.set("fs.default.name", "file:///");
            return FileSystem.get(CachedConfiguration.getInstance());
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public InMemoryInstance(String instanceName) {
        this(instanceName, getDefaultFileSystem());
    }

    public InMemoryInstance(String instanceName, FileSystem fs) {
        synchronized (instances) {
            if (instances.containsKey(instanceName))
                acu = instances.get(instanceName);
            else
                instances.put(instanceName, acu = new InMemoryAccumulo(fs));
        }
        this.instanceName = instanceName;
    }

    public String getRootTabletLocation() {
        return genericAddress;
    }

    public List<String> getMasterLocations() {
        return Collections.singletonList(genericAddress);
    }

    public String getInstanceID() {
        return "mock-instance-id";
    }

    public String getInstanceName() {
        return instanceName;
    }

    public String getZooKeepers() {
        return "localhost";
    }

    public int getZooKeepersSessionTimeOut() {
        return 30 * 1000;
    }

    public static class CachedConfiguration {
        private static Configuration configuration = null;

        public static synchronized Configuration getInstance() {
            if (configuration == null)
                setInstance(new Configuration());
            return configuration;
        }

        public static synchronized Configuration setInstance(Configuration update) {
            Configuration result = configuration;
            configuration = update;
            return result;
        }
    }

}
