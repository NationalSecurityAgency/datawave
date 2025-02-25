package datawave.microservice.metadata;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.SortedMap;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import datawave.data.ColumnFamilyConstants;
import datawave.marking.MarkingFunctions;
import datawave.microservice.Connection;
import datawave.microservice.dictionary.config.ResponseObjectFactory;
import datawave.security.util.ScannerHelper;
import datawave.webservice.dictionary.data.DefaultDataDictionary;
import datawave.webservice.dictionary.data.DefaultDescription;
import datawave.webservice.dictionary.data.DefaultDictionaryField;
import datawave.webservice.dictionary.data.DefaultFields;
import datawave.webservice.metadata.DefaultMetadataField;

public class DefaultMetadataFieldScanner {
    
    private static final Logger log = LoggerFactory.getLogger(DefaultMetadataFieldScanner.class);
    private static final String TIMESTAMP_FORMAT = "yyyyMMddHHmmss";
    
    private final MarkingFunctions markingFunctions;
    private final ResponseObjectFactory<DefaultDescription,?,DefaultMetadataField,?,?> responseObjectFactory;
    private final Map<String,String> normalizationMap;
    private final Connection connectionConfig;
    private final int numThreads;
    
    public DefaultMetadataFieldScanner(MarkingFunctions markingFunctions,
                    ResponseObjectFactory<DefaultDescription,DefaultDataDictionary,DefaultMetadataField,DefaultDictionaryField,DefaultFields> responseObjectFactory,
                    Map<String,String> normalizationMap, Connection connectionConfig, int numThreads) {
        this.markingFunctions = markingFunctions;
        this.responseObjectFactory = responseObjectFactory;
        this.normalizationMap = normalizationMap;
        this.connectionConfig = connectionConfig;
        this.numThreads = numThreads;
    }
    
    public Collection<DefaultMetadataField> getFields(Map<String,String> aliases, Collection<String> datatypeFilters) throws TableNotFoundException {
        BatchScanner scanner = createScanner();
        Transformer transformer = new Transformer(scanner.iterator(), aliases, datatypeFilters);
        Collection<DefaultMetadataField> fields = transformer.transform();
        scanner.close();
        return fields;
    }
    
    /**
     * Create and return a scanner that will aggregate metadata entries by their row.
     * 
     * @return the scanner
     * @throws TableNotFoundException
     *             if the metadata table is not found
     */
    private BatchScanner createScanner() throws TableNotFoundException {
        BatchScanner scanner = ScannerHelper.createBatchScanner(connectionConfig.getAccumuloClient(), connectionConfig.getMetadataTable(),
                        connectionConfig.getAuths(), numThreads);
        // Ensure rows for the same field are grouped into a single iterator entry.
        scanner.addScanIterator(new IteratorSetting(21, WholeRowIterator.class));
        // Do not limit the scanner based on ranges.
        scanner.setRanges(Collections.singletonList(new Range()));
        scanner.fetchColumnFamily(ColumnFamilyConstants.COLF_E);
        scanner.fetchColumnFamily(ColumnFamilyConstants.COLF_I);
        scanner.fetchColumnFamily(ColumnFamilyConstants.COLF_RI);
        scanner.fetchColumnFamily(ColumnFamilyConstants.COLF_DESC);
        scanner.fetchColumnFamily(ColumnFamilyConstants.COLF_H);
        scanner.fetchColumnFamily(ColumnFamilyConstants.COLF_T);
        scanner.fetchColumnFamily(ColumnFamilyConstants.COLF_TF);
        return scanner;
    }
    
    private class Transformer {
        
        private final Iterator<Map.Entry<Key,Value>> iterator;
        private final Map<String,String> aliases;
        private final Collection<String> dataTypeFilters;
        private final boolean acceptAllDataTypes;
        
        private final Map<String,Map<String,DefaultMetadataField>> fields; // Map of field names to data types to transformed fields.
        private final Text currRow = new Text(); // Used to copy the current row into.
        private final Text currColumnFamily = new Text(); // Used to copy the current column family into.
        
        private Key currKey;
        private Value currValue;
        private String currColumnQualifier;
        private DefaultMetadataField currField;
        
        private Transformer(Iterator<Map.Entry<Key,Value>> iterator, Map<String,String> aliases, Collection<String> dataTypeFilters) {
            this.iterator = iterator;
            this.aliases = aliases;
            this.dataTypeFilters = dataTypeFilters;
            this.acceptAllDataTypes = dataTypeFilters.isEmpty();
            fields = new HashMap<>();
        }
        
        /**
         * Transform the iterator entries into {@link DefaultMetadataField} and return them.
         * 
         * @return the transformed fields
         */
        private Collection<DefaultMetadataField> transform() {
            while (iterator.hasNext()) {
                Map.Entry<Key,Value> entry = iterator.next();
                try {
                    // Handles a batch scanner bug where an entry with a null key and value may be in the iterator.
                    if (entry.getKey() == null && entry.getValue() == null)
                        // TODO - return an empty collection instead to avoid NPE?
                        return null;
                    // Check if either the key or value are null, and throw an exception if so.
                    if (null == entry.getKey() || null == entry.getValue()) {
                        throw new IllegalArgumentException("Null key or value. Key:" + entry.getKey() + ", Value: " + entry.getValue());
                    }
                    transformEntry(entry);
                } catch (IOException e) {
                    throw new IllegalStateException("Unable to decode row " + entry.getKey());
                } catch (MarkingFunctions.Exception e) {
                    throw new IllegalStateException("Unable to decode visibility " + entry.getKey(), e);
                }
            }
            // @formatter::off
            return fields.values().stream().map(Map::values).flatMap(Collection::stream).collect(Collectors.toCollection(LinkedList::new));
            // @formatter::on
        }
        
        private void transformEntry(Map.Entry<Key,Value> currEntry) throws IOException, MarkingFunctions.Exception {
            SortedMap<Key,Value> rowEntries = WholeRowIterator.decodeRow(currEntry.getKey(), currEntry.getValue());
            
            for (Map.Entry<Key,Value> entry : rowEntries.entrySet()) {
                setCurrentVars(entry);
                
                // If this row is a hidden event, do not continue transforming it and ensure any
                // previously transformed entries for this row are not in the final results.
                if (isColumnFamly(ColumnFamilyConstants.COLF_H)) {
                    currField = null;
                    fields.remove(currRow.toString());
                    break;
                }
                
                // Verify that the row should not be filtered out due to its datatype.
                String dataType = getDataType();
                if (hasAllowedDataType(dataType)) {
                    
                    // Set the current transformed field that will be modified from here on.
                    setCurrentField(dataType);
                    
                    // If this an event field, then this is not an indexed field. Use the field name and timestamp of this entry.
                    if (isColumnFamly(ColumnFamilyConstants.COLF_E)) {
                        currField.setIndexOnly(false);
                        setFieldNameAndAlias();
                        setLastUpdated();
                        // Check if this is a forward-indexed field.
                    } else if (isColumnFamly(ColumnFamilyConstants.COLF_I)) {
                        currField.setForwardIndexed(true);
                        // Check if this is a reversed-indexed field
                    } else if (isColumnFamly(ColumnFamilyConstants.COLF_RI)) {
                        currField.setReverseIndexed(true);
                        // If this is an description entry, extract the description and add it to the transformed field.
                    } else if (isColumnFamly(ColumnFamilyConstants.COLF_DESC)) {
                        setDescriptions();
                        // If this is a type entry, add it to the field.
                    } else if (isColumnFamly(ColumnFamilyConstants.COLF_T)) {
                        setType();
                        // Check if the field is tokenized.
                    } else if (isColumnFamly(ColumnFamilyConstants.COLF_TF)) {
                        currField.setTokenized(true);
                    } else {
                        log.warn("Unknown entry with key={}, value={}", currKey, currValue);
                    }
                    
                    // If the field name is null, this is potentially an index-only field which has no event ("e") entry.
                    // Use the field name of this entry. These values will be replaced if an event entry is encountered later on.
                    if (currField.getFieldName() == null) {
                        setFieldNameAndAlias();
                    }
                    // Determine the lastUpdated value for index-only fields without including timestamps from description rows.
                    if (currField.isIndexOnly() && !isColumnFamly(ColumnFamilyConstants.COLF_DESC)) {
                        setLastUpdated();
                    }
                }
            }
        }
        
        // Set the current variables from the specified entry.
        private void setCurrentVars(Map.Entry<Key,Value> entry) {
            currKey = entry.getKey();
            currValue = entry.getValue();
            currKey.getRow(currRow);
            currKey.getColumnFamily(currColumnFamily);
            currColumnQualifier = currKey.getColumnQualifier().toString();
        }
        
        // Return true if the current column family is equal to the specified text, or false otherwise.
        private boolean isColumnFamly(Text columnFamily) {
            return columnFamily.equals(currColumnFamily);
        }
        
        // Return true if all data types are allowed or if the given data type is in the set of data type filters, or false otherwise.
        private boolean hasAllowedDataType(String dataType) {
            return acceptAllDataTypes || dataTypeFilters.contains(dataType);
        }
        
        // Return the data type from the current column qualifier.
        private String getDataType() {
            int nullPos = currColumnQualifier.indexOf('\0');
            return (nullPos < 0) ? currColumnQualifier : currColumnQualifier.substring(0, nullPos);
        }
        
        // Set the current {@link DefaultMetadataField}. If a field already exists for the given field name and data type combination, it will be reused,
        // otherwise a new {@link DefaultMetadataField} will be created.
        private void setCurrentField(final String dataType) {
            Map<String,DefaultMetadataField> dataTypes = fields.computeIfAbsent(currRow.toString(), k -> Maps.newHashMap());
            currField = dataTypes.get(dataType);
            
            // If a field does not yet exist for the field name and data type, create a new one that
            // defaults to index-only, which shall be cleared if we see an event entry.
            if (currField == null) {
                currField = new DefaultMetadataField();
                currField.setIndexOnly(true);
                currField.setDataType(dataType);
                dataTypes.put(dataType, currField);
            }
        }
        
        // Set the field name for the current {@link DefaultMetadataField}. If an alias exists for the field name, the alias will be used as the primary field
        // name while the original field name is relegated to the internal field name.
        private void setFieldNameAndAlias() {
            String fieldName = currKey.getRow().toString();
            if (aliases.containsKey(fieldName)) {
                currField.setFieldName(aliases.get(fieldName));
                currField.setInternalFieldName(fieldName);
            } else {
                currField.setFieldName(fieldName);
            }
        }
        
        // Extract the description from the current value and add it to the current {@link DefaultMetadataField}.
        private void setDescriptions() throws MarkingFunctions.Exception {
            DefaultDescription description = responseObjectFactory.getDescription();
            description.setDescription(currValue.toString());
            description.setMarkings(markingFunctions.translateFromColumnVisibility(currKey.getColumnVisibilityParsed()));
            currField.getDescriptions().add(description);
        }
        
        // Set the normalized type for the current {@link DefaultMetadataField}. If no normalized version can be found for the type, the type will default to
        // "Unknown".
        private void setType() {
            int nullPos = currColumnQualifier.indexOf('\0');
            String type = currColumnQualifier.substring(nullPos + 1);
            String normalizedType = normalizationMap.get(type);
            currField.addType(normalizedType != null ? normalizedType : "Unknown");
        }
        
        // Set the last updated date for the current {@link DefaultMetadataField} based on the timestamp of the current entry.
        private void setLastUpdated() {
            String formattedCurrentKeyTimeStamp = Instant.ofEpochMilli(currKey.getTimestamp()).atZone(ZoneId.systemDefault()).toLocalDateTime()
                            .format(DateTimeFormatter.ofPattern(TIMESTAMP_FORMAT));
            if (currField.getLastUpdated() != null) {
                if (Long.parseLong(currField.getLastUpdated()) < Long.parseLong(formattedCurrentKeyTimeStamp)) {
                    currField.setLastUpdated(formattedCurrentKeyTimeStamp);
                }
            } else {
                currField.setLastUpdated(formattedCurrentKeyTimeStamp);
            }
        }
    }
}
