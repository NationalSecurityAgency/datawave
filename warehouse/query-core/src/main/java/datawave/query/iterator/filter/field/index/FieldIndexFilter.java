package datawave.query.iterator.filter.field.index;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import datawave.data.type.Type;
import datawave.ingest.mapreduce.handler.shard.FieldIndexData;
import datawave.query.attributes.ValueTuple;
import datawave.query.jexl.ArithmeticJexlEngines;
import datawave.query.jexl.DatawaveJexlContext;
import datawave.query.jexl.DatawaveJexlEngine;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.util.TypeMetadata;
import io.protostuff.ProtobufIOUtil;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.jexl2.JexlArithmetic;
import org.apache.commons.jexl2.Script;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class FieldIndexFilter {
    private static final Logger log = Logger.getLogger(FieldIndexFilter.class);
    
    protected Map<String,Multimap<String,String>> fieldIndexFilterMapByType;
    protected TypeMetadata typeMetadata;
    protected DatawaveJexlEngine engine;
    
    // Map of fields to JexlNodes
    protected Multimap<String,JexlNode> fieldIndexFilterNodes;
    
    public FieldIndexFilter(Map<String,Multimap<String,String>> fieldIndexFilterMapByType, TypeMetadata typeMetadata, JexlArithmetic arithmetic) {
        this(fieldIndexFilterMapByType, typeMetadata, arithmetic, HashMultimap.create());
    }
    
    public FieldIndexFilter(Map<String,Multimap<String,String>> fieldIndexFilterMapByType, TypeMetadata typeMetadata, JexlArithmetic arithmetic,
                    Multimap<String,JexlNode> fieldIndexFilterNodes) {
        this.fieldIndexFilterMapByType = fieldIndexFilterMapByType;
        this.typeMetadata = typeMetadata;
        this.engine = ArithmeticJexlEngines.getEngine(arithmetic);
        this.fieldIndexFilterNodes = fieldIndexFilterNodes;
    }
    
    // NOTE: The goal with this logic is never to produce a false negative. That is to say that we should never filter
    // out a particular data entry unless we are absolutely sure that it doesn't belong. Because of this, the
    // evaluation defaults to true.
    public boolean keep(String ingestType, String fieldName, Value value) {
        // Check the value to see if there is any data to filter against
        if (fieldIndexFilterMapByType != null && !fieldIndexFilterMapByType.isEmpty() && fieldIndexFilterNodes != null && !fieldIndexFilterNodes.isEmpty()
                        && ingestType != null && value != null && value.get().length > 0) {
            Multimap<String,String> fieldValuesMap = decodeValue(value).getFilterData().getFieldValueMapping();
            
            // make sure that this field mapping is present for the given ingest type
            Collection<String> filterFields = null;
            Multimap<String,String> fieldIndexFilterMap = fieldIndexFilterMapByType.get(ingestType);
            if (fieldIndexFilterMap != null) {
                filterFields = new HashSet<>(fieldIndexFilterMap.get(fieldName));
                if (filterFields != null) {
                    filterFields.retainAll(fieldValuesMap.keys());
                    filterFields.retainAll(fieldIndexFilterNodes.keySet());
                }
            }
            
            if (filterFields != null && !filterFields.isEmpty()) {
                DatawaveJexlContext datawaveJexlContext = new DatawaveJexlContext();
                List<JexlNode> nodes = new ArrayList<>();
                
                // Iterate over the intersected set of fields, and get a value tuple for each of the configured types
                for (String filterField : filterFields) {
                    Set<ValueTuple> valueTuples = new HashSet<>();
                    for (String dataType : typeMetadata.getTypeMetadata(filterField, ingestType))
                        // @formatter:off
                        valueTuples.addAll(
                                fieldValuesMap.get(filterField)
                                        .stream()
                                        .map(fieldValue -> createValueTuple(filterField, dataType, fieldValue))
                                        .filter(valueTuple -> valueTuple != null)
                                        .collect(Collectors.toSet()));
                        // @formatter:on
                    if (!valueTuples.isEmpty()) {
                        datawaveJexlContext.set(filterField, valueTuples);
                        nodes.addAll(fieldIndexFilterNodes.get(filterField));
                    }
                }
                
                if (!nodes.isEmpty()) {
                    // AND all of the JEXL nodes together, create a script, and run the evaluation
                    JexlNode rootNode = JexlNodeFactory.createAndNode(nodes);
                    
                    // Evaluate the JexlContext against the Script
                    Script script = engine.createScript(JexlStringBuildingVisitor.buildQuery(rootNode));
                    
                    if (!ArithmeticJexlEngines.isMatched(script.execute(datawaveJexlContext))) {
                        if (log.isTraceEnabled())
                            log.trace("Filtered out an entry using the field index filter: " + datawaveJexlContext);
                        return false;
                    }
                }
            }
        }
        return true;
    }
    
    private FieldIndexData decodeValue(Value value) {
        FieldIndexData fieldIndexData = new FieldIndexData();
        ProtobufIOUtil.mergeFrom(value.get(), fieldIndexData, fieldIndexData.cachedSchema());
        return fieldIndexData;
    }
    
    private ValueTuple createValueTuple(String fieldName, String dataType, String value) {
        Type type = null;
        try {
            type = (Type) Class.forName(dataType).newInstance();
            type.setDelegateFromString(value);
        } catch (Exception e) {
            log.warn("Unable to create instance of type: " + dataType, e);
        }
        if (type != null)
            return new ValueTuple(fieldName, type, type.getNormalizedValue(), null);
        return null;
    }
    
    public Multimap<String,JexlNode> getFieldIndexFilterNodes() {
        return fieldIndexFilterNodes;
    }
    
    public void addFieldIndexFilterNodes(Multimap<String,JexlNode> fieldIndexFilterNodes) {
        this.fieldIndexFilterNodes.putAll(fieldIndexFilterNodes);
    }
}
