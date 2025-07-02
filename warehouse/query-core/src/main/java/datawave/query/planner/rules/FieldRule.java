package datawave.query.planner.rules;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.jexl3.parser.JexlNode;

import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.query.util.MetadataHelper;

public abstract class FieldRule {
    Set<String> pruneFields = new HashSet<>();
    Map<String,Set<String>> pruneFVPairs = new HashMap<>();

    public FieldRule(GenericQueryConfiguration config) {
        parseRules(config);
    }

    public abstract void parseRules(GenericQueryConfiguration config);

    public abstract boolean shouldPrune(JexlNode node, MetadataHelper helper);

    public abstract boolean shouldModify(JexlNode node, MetadataHelper helper);

    public abstract JexlNode modify(JexlNode node, MetadataHelper helper);
}
