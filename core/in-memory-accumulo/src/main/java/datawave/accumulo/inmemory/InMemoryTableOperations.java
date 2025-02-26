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

import static com.google.common.base.Preconditions.checkArgument;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Consumer;

import org.apache.accumulo.core.classloader.ClassLoaderUtil;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.CloneConfiguration;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.DiskUsage;
import org.apache.accumulo.core.client.admin.FindMax;
import org.apache.accumulo.core.client.admin.ImportConfiguration;
import org.apache.accumulo.core.client.admin.Locations;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.clientImpl.TableOperationsHelper;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.crypto.CryptoFactoryLoader;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.TabletIdImpl;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.spi.crypto.CryptoEnvironment;
import org.apache.accumulo.core.spi.crypto.CryptoService;
import org.apache.accumulo.core.util.Validators;
import org.apache.accumulo.core.util.tables.TableNameUtil;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.accumulo.inmemory.impl.InMemoryTabletLocator;

class InMemoryTableOperations extends TableOperationsHelper {
    private static final Logger log = LoggerFactory.getLogger(InMemoryTableOperations.class);
    private static final byte[] ZERO = {0};
    private final InMemoryAccumulo acu;
    private final String username;
    
    InMemoryTableOperations(InMemoryAccumulo acu, String username) {
        this.acu = acu;
        this.username = username;
    }
    
    @Override
    public SortedSet<String> list() {
        return new TreeSet<>(acu.tables.keySet());
    }
    
    @Override
    public boolean exists(String tableName) {
        return acu.tables.containsKey(tableName);
    }
    
    private boolean namespaceExists(String namespace) {
        return acu.namespaces.containsKey(namespace);
    }
    
    @Override
    public void create(String tableName) throws AccumuloException, AccumuloSecurityException, TableExistsException {
        create(tableName, new NewTableConfiguration());
    }
    
    @Override
    public void create(String tableName, boolean versioningIter) throws AccumuloException, AccumuloSecurityException, TableExistsException {
        create(tableName, versioningIter, TimeType.MILLIS);
    }
    
    @Override
    public void create(String tableName, boolean versioningIter, TimeType timeType) throws AccumuloException, AccumuloSecurityException, TableExistsException {
        NewTableConfiguration ntc = new NewTableConfiguration().setTimeType(timeType);
        
        if (versioningIter)
            create(tableName, ntc);
        else
            create(tableName, ntc.withoutDefaultIterators());
    }
    
    @Override
    public void create(String tableName, NewTableConfiguration ntc) throws AccumuloException, AccumuloSecurityException, TableExistsException {
        String namespace = TableNameUtil.qualify(tableName).getFirst();
        Validators.NEW_TABLE_NAME.validate(tableName);
        if (exists(tableName))
            throw new TableExistsException(tableName, tableName, "");
        checkArgument(namespaceExists(namespace), "Namespace (" + namespace + ") does not exist, create it first");
        acu.createTable(username, tableName, ntc.getTimeType(), ntc.getProperties());
    }
    
    @Override
    public void addSplits(String tableName, SortedSet<Text> partitionKeys) throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
        if (!exists(tableName))
            throw new TableNotFoundException(tableName, tableName, "");
        acu.addSplits(tableName, partitionKeys);
    }
    
    @Override
    public Collection<Text> getSplits(String tableName) throws TableNotFoundException {
        return listSplits(tableName);
    }
    
    @Override
    public Collection<Text> getSplits(String tableName, int maxSplits) throws TableNotFoundException {
        return listSplits(tableName);
    }
    
    @Override
    public Collection<Text> listSplits(String tableName) throws TableNotFoundException {
        if (!exists(tableName))
            throw new TableNotFoundException(tableName, tableName, "");
        return acu.getSplits(tableName);
    }
    
    @Override
    public Collection<Text> listSplits(String tableName, int maxSplits) throws TableNotFoundException {
        return listSplits(tableName);
    }
    
    @Override
    public void delete(String tableName) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        if (!exists(tableName))
            throw new TableNotFoundException(tableName, tableName, "");
        acu.tables.remove(tableName);
    }
    
    @Override
    public void rename(String oldTableName, String newTableName)
                    throws AccumuloSecurityException, TableNotFoundException, AccumuloException, TableExistsException {
        if (!exists(oldTableName))
            throw new TableNotFoundException(oldTableName, oldTableName, "");
        if (exists(newTableName))
            throw new TableExistsException(newTableName, newTableName, "");
        InMemoryTable t = acu.tables.remove(oldTableName);
        String namespace = TableNameUtil.qualify(newTableName).getFirst();
        InMemoryNamespace n = acu.namespaces.get(namespace);
        if (n == null) {
            n = new InMemoryNamespace();
        }
        t.setNamespaceName(namespace);
        t.setNamespace(n);
        acu.namespaces.put(namespace, n);
        acu.tables.put(newTableName, t);
    }
    
    @Override
    public void flush(String tableName) throws AccumuloException, AccumuloSecurityException {}
    
    @Override
    public void setProperty(String tableName, String property, String value) throws AccumuloException, AccumuloSecurityException {
        acu.tables.get(tableName).settings.put(property, value);
    }
    
    @Override
    public Map<String,String> modifyProperties(String tableName, Consumer<Map<String,String>> mapMutator)
                    throws AccumuloException, AccumuloSecurityException, IllegalArgumentException, ConcurrentModificationException {
        mapMutator.accept(acu.tables.get(tableName).settings);
        return acu.tables.get(tableName).settings;
    }
    
    @Override
    public void removeProperty(String tableName, String property) throws AccumuloException, AccumuloSecurityException {
        acu.tables.get(tableName).settings.remove(property);
    }
    
    @Override
    public Iterable<Entry<String,String>> getProperties(String tableName) throws AccumuloException, TableNotFoundException {
        return getConfiguration(tableName).entrySet();
    }
    
    @Override
    public Map<String,String> getConfiguration(String tableName) throws AccumuloException, TableNotFoundException {
        String namespace = TableNameUtil.qualify(tableName).getFirst();
        if (!exists(tableName)) {
            if (!namespaceExists(namespace))
                throw new TableNotFoundException(tableName, new NamespaceNotFoundException(null, namespace, null));
            throw new TableNotFoundException(null, tableName, null);
        }
        Map<String,String> conf = new HashMap<>(acu.namespaces.get(namespace).settings);
        Map<String,String> tableConf = acu.tables.get(tableName).settings;
        for (Entry<String,String> e : tableConf.entrySet()) {
            if (conf.containsKey(e.getKey())) {
                conf.remove(e);
            }
            conf.put(e.getKey(), e.getValue());
        }
        return conf;
    }
    
    @Override
    public Map<String,String> getTableProperties(String tableName) throws AccumuloException, TableNotFoundException {
        return getConfiguration(tableName);
    }
    
    @Override
    public void setLocalityGroups(String tableName, Map<String,Set<Text>> groups) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        if (!exists(tableName))
            throw new TableNotFoundException(tableName, tableName, "");
        acu.tables.get(tableName).setLocalityGroups(groups);
    }
    
    @Override
    public Map<String,Set<Text>> getLocalityGroups(String tableName) throws AccumuloException, TableNotFoundException {
        if (!exists(tableName))
            throw new TableNotFoundException(tableName, tableName, "");
        return acu.tables.get(tableName).getLocalityGroups();
    }
    
    @Override
    public Set<Range> splitRangeByTablets(String tableName, Range range, int maxSplits)
                    throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        if (!exists(tableName))
            throw new TableNotFoundException(tableName, tableName, "");
        return Collections.singleton(range);
    }
    
    @Override
    public void importDirectory(String tableName, String dir, String failureDir, boolean setTime)
                    throws IOException, AccumuloException, AccumuloSecurityException, TableNotFoundException {
        long time = System.currentTimeMillis();
        InMemoryTable table = acu.tables.get(tableName);
        if (table == null) {
            throw new TableNotFoundException(null, tableName, "The table was not found");
        }
        Path importPath = new Path(dir);
        Path failurePath = new Path(failureDir);
        
        FileSystem fs = acu.getFileSystem();
        /*
         * check preconditions
         */
        // directories are directories
        if (fs.isFile(importPath)) {
            throw new IOException("Import path must be a directory.");
        }
        if (fs.isFile(failurePath)) {
            throw new IOException("Failure path must be a directory.");
        }
        // failures are writable
        Path createPath = failurePath.suffix("/.createFile");
        FSDataOutputStream createStream = null;
        try {
            createStream = fs.create(createPath);
        } catch (IOException e) {
            throw new IOException("Error path is not writable.");
        } finally {
            if (createStream != null) {
                createStream.close();
            }
        }
        fs.delete(createPath, false);
        // failures are empty
        FileStatus[] failureChildStats = fs.listStatus(failurePath);
        if (failureChildStats.length > 0) {
            throw new IOException("Error path must be empty.");
        }
        /*
         * Begin the import - iterate the files in the path
         */
        for (FileStatus importStatus : fs.listStatus(importPath)) {
            try {
                CryptoService cs = CryptoFactoryLoader.getServiceForClient(CryptoEnvironment.Scope.TABLE, table.settings);
                FileSKVIterator importIterator = FileOperations.getInstance().newReaderBuilder()
                                .forFile(importStatus.getPath().toString(), fs, fs.getConf(), cs).withTableConfiguration(DefaultConfiguration.getInstance())
                                .seekToBeginning().build();
                while (importIterator.hasTop()) {
                    Key key = importIterator.getTopKey();
                    Value value = importIterator.getTopValue();
                    if (setTime) {
                        key.setTimestamp(time);
                    }
                    Mutation mutation = new Mutation(key.getRow());
                    if (!key.isDeleted()) {
                        mutation.put(key.getColumnFamily(), key.getColumnQualifier(), new ColumnVisibility(key.getColumnVisibilityData().toArray()),
                                        key.getTimestamp(), value);
                    } else {
                        mutation.putDelete(key.getColumnFamily(), key.getColumnQualifier(), new ColumnVisibility(key.getColumnVisibilityData().toArray()),
                                        key.getTimestamp());
                    }
                    table.addMutation(mutation);
                    importIterator.next();
                }
            } catch (Exception e) {
                FSDataOutputStream failureWriter = null;
                DataInputStream failureReader = null;
                try {
                    failureWriter = fs.create(failurePath.suffix("/" + importStatus.getPath().getName()));
                    failureReader = fs.open(importStatus.getPath());
                    int read = 0;
                    byte[] buffer = new byte[1024];
                    while (-1 != (read = failureReader.read(buffer))) {
                        failureWriter.write(buffer, 0, read);
                    }
                } finally {
                    if (failureReader != null)
                        failureReader.close();
                    if (failureWriter != null)
                        failureWriter.close();
                }
            }
            fs.delete(importStatus.getPath(), true);
        }
    }
    
    @Override
    public void offline(String tableName) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        offline(tableName, false);
    }
    
    @Override
    public void offline(String tableName, boolean wait) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        if (!exists(tableName))
            throw new TableNotFoundException(tableName, tableName, "");
    }
    
    @Override
    public void online(String tableName) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        online(tableName, false);
    }
    
    @Override
    public void online(String tableName, boolean wait) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        if (!exists(tableName))
            throw new TableNotFoundException(tableName, tableName, "");
    }
    
    @Override
    public boolean isOnline(String s) throws AccumuloException, TableNotFoundException {
        return false;
    }
    
    @Override
    public void clearLocatorCache(String tableName) throws TableNotFoundException {
        if (!exists(tableName))
            throw new TableNotFoundException(tableName, tableName, "");
    }
    
    @Override
    public Map<String,String> tableIdMap() {
        Map<String,String> result = new HashMap<>();
        for (Entry<String,InMemoryTable> entry : acu.tables.entrySet()) {
            String table = entry.getKey();
            if (RootTable.NAME.equals(table))
                result.put(table, RootTable.ID.canonical());
            else if (MetadataTable.NAME.equals(table))
                result.put(table, MetadataTable.ID.canonical());
            else
                result.put(table, entry.getValue().getTableId());
        }
        return result;
    }
    
    @Override
    public List<DiskUsage> getDiskUsage(Set<String> tables) throws AccumuloException, AccumuloSecurityException {
        
        List<DiskUsage> diskUsages = new ArrayList<>();
        diskUsages.add(new DiskUsage(new TreeSet<>(tables), 0l));
        
        return diskUsages;
    }
    
    @Override
    public void merge(String tableName, Text start, Text end) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        if (!exists(tableName))
            throw new TableNotFoundException(tableName, tableName, "");
        acu.merge(tableName, start, end);
    }
    
    @Override
    public void deleteRows(String tableName, Text start, Text end) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        if (!exists(tableName))
            throw new TableNotFoundException(tableName, tableName, "");
        InMemoryTable t = acu.tables.get(tableName);
        Text startText = start != null ? new Text(start) : new Text();
        if (startText.getLength() == 0 && end == null) {
            t.table.clear();
            return;
        }
        Text endText = end != null ? new Text(end) : new Text(t.table.lastKey().getRow().getBytes());
        startText.append(ZERO, 0, 1);
        endText.append(ZERO, 0, 1);
        Set<Key> keep = new TreeSet<>(t.table.subMap(new Key(startText), new Key(endText)).keySet());
        t.table.keySet().removeAll(keep);
    }
    
    @Override
    public void compact(String tableName, Text start, Text end, boolean flush, boolean wait)
                    throws AccumuloSecurityException, TableNotFoundException, AccumuloException {
        if (!exists(tableName))
            throw new TableNotFoundException(tableName, tableName, "");
    }
    
    @Override
    public void compact(String tableName, Text start, Text end, List<IteratorSetting> iterators, boolean flush, boolean wait)
                    throws AccumuloSecurityException, TableNotFoundException, AccumuloException {
        if (!exists(tableName))
            throw new TableNotFoundException(tableName, tableName, "");
        
        if (iterators != null && iterators.size() > 0)
            throw new UnsupportedOperationException();
    }
    
    @Override
    public void compact(String tableName, CompactionConfig config) throws AccumuloSecurityException, TableNotFoundException, AccumuloException {
        if (!exists(tableName))
            throw new TableNotFoundException(tableName, tableName, "");
        
        if (config.getIterators().size() > 0 || config.getCompactionStrategy() != null)
            throw new UnsupportedOperationException("InMemory does not support iterators or compaction strategies for compactions");
    }
    
    @Override
    public void cancelCompaction(String tableName) throws AccumuloSecurityException, TableNotFoundException, AccumuloException {
        if (!exists(tableName))
            throw new TableNotFoundException(tableName, tableName, "");
    }
    
    @Override
    public void clone(String srcTableName, String newTableName, boolean flush, Map<String,String> propertiesToSet, Set<String> propertiesToExclude)
                    throws AccumuloException, AccumuloSecurityException, TableNotFoundException, TableExistsException {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void clone(String s, String s1, CloneConfiguration cloneConfiguration)
                    throws AccumuloException, AccumuloSecurityException, TableNotFoundException, TableExistsException {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void flush(String tableName, Text start, Text end, boolean wait) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        if (!exists(tableName))
            throw new TableNotFoundException(tableName, tableName, "");
    }
    
    @Override
    public Text getMaxRow(String tableName, Authorizations auths, Text startRow, boolean startInclusive, Text endRow, boolean endInclusive)
                    throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
        InMemoryTable table = acu.tables.get(tableName);
        if (table == null)
            throw new TableNotFoundException(tableName, tableName, "no such table");
        
        return FindMax.findMax(new InMemoryScanner(table, auths), startRow, startInclusive, endRow, endInclusive);
    }
    
    @Override
    public void importTable(String tableName, String exportDir) throws TableExistsException, AccumuloException, AccumuloSecurityException {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void importTable(String tableName, Set<String> importDirs, ImportConfiguration ic)
                    throws TableExistsException, AccumuloException, AccumuloSecurityException {
        throw new UnsupportedOperationException();
    }
    
    public void importTable(String tableName, String importDir, boolean keepMappings, boolean skipOnline)
                    throws TableExistsException, AccumuloException, AccumuloSecurityException {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void exportTable(String tableName, String exportDir) throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public boolean testClassLoad(String tableName, String className, String asTypeName)
                    throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        try {
            ClassLoaderUtil.loadClass(className, Class.forName(asTypeName));
        } catch (ClassNotFoundException e) {
            log.warn("Could not load class '" + className + "' with type name '" + asTypeName + "' in testClassLoad().", e);
            return false;
        }
        return true;
    }
    
    @Override
    public void setSamplerConfiguration(String tableName, SamplerConfiguration samplerConfiguration)
                    throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void clearSamplerConfiguration(String tableName) throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public SamplerConfiguration getSamplerConfiguration(String tableName) throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public Locations locate(String tableName, Collection<Range> ranges) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        Map<String,Map<KeyExtent,List<Range>>> binnedRanges = new HashMap<>();
        InMemoryTabletLocator locator = new InMemoryTabletLocator();
        List<Range> ignore = locator.binRanges(null, new ArrayList<>(ranges), binnedRanges);
        return new LocationsImpl(binnedRanges);
    }
    
    private static class LocationsImpl implements Locations {
        
        private Map<Range,List<TabletId>> groupedByRanges;
        private Map<TabletId,List<Range>> groupedByTablets;
        private Map<TabletId,String> tabletLocations;
        
        public LocationsImpl(Map<String,Map<KeyExtent,List<Range>>> binnedRanges) {
            groupedByTablets = new HashMap<>();
            groupedByRanges = null;
            tabletLocations = new HashMap<>();
            
            for (Entry<String,Map<KeyExtent,List<Range>>> entry : binnedRanges.entrySet()) {
                String location = entry.getKey();
                
                for (Entry<KeyExtent,List<Range>> entry2 : entry.getValue().entrySet()) {
                    TabletIdImpl tabletId = new TabletIdImpl(entry2.getKey());
                    tabletLocations.put(tabletId, location);
                    List<Range> prev = groupedByTablets.put(tabletId, Collections.unmodifiableList(entry2.getValue()));
                    if (prev != null) {
                        throw new RuntimeException("Unexpected : tablet at multiple locations : " + location + " " + tabletId);
                    }
                }
            }
            
            groupedByTablets = Collections.unmodifiableMap(groupedByTablets);
        }
        
        @Override
        public String getTabletLocation(TabletId tabletId) {
            return tabletLocations.get(tabletId);
        }
        
        @Override
        public Map<Range,List<TabletId>> groupByRange() {
            if (groupedByRanges == null) {
                Map<Range,List<TabletId>> tmp = new HashMap<>();
                
                for (Entry<TabletId,List<Range>> entry : groupedByTablets.entrySet()) {
                    for (Range range : entry.getValue()) {
                        List<TabletId> tablets = tmp.get(range);
                        if (tablets == null) {
                            tablets = new ArrayList<>();
                            tmp.put(range, tablets);
                        }
                        
                        tablets.add(entry.getKey());
                    }
                }
                
                Map<Range,List<TabletId>> tmp2 = new HashMap<>();
                for (Entry<Range,List<TabletId>> entry : tmp.entrySet()) {
                    tmp2.put(entry.getKey(), Collections.unmodifiableList(entry.getValue()));
                }
                
                groupedByRanges = Collections.unmodifiableMap(tmp2);
            }
            
            return groupedByRanges;
        }
        
        @Override
        public Map<TabletId,List<Range>> groupByTablet() {
            return groupedByTablets;
        }
    }
}
