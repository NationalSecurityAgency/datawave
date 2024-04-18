package datawave.age.off.util;

import javax.xml.transform.stream.StreamResult;

import org.apache.xerces.dom.DocumentImpl;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.ProcessingInstruction;

import datawave.ingest.util.cache.watch.AgeOffRuleLoader;

/**
 * Creats a transformable node from an AgeOffRuleLoader.RuleConfig
 */
public class RuleConfigDocument extends DocumentImpl {
    private static final String FILTER_CLASS_ELEMENT_NAME = "filterClass";
    private static final String MATCH_PATTERN_ELEMENT_NAME = "matchPattern";
    private static final String IS_MERGE_ELEMENT_NAME = "ismerge";
    private static final String TTL_ELEMENT_NAME = "ttl";
    private static final String TTL_UNITS_ATTRIBUTE_NAME = "units";
    private static final String RULE_ELEMENT_NAME = "rule";
    private static final String LABEL_ATTRIBUTE_NAME = "label";
    private static final String MODE_ATTRIBUTE_NAME = "mode";
    private static final String MERGE_ATTRIBUTE_VALUE = "merge";
    private static final char[] COMMENT_ESCAPE_CHARACTERS = new char[] {'<', '>'};

    private final Element rule;
    private final AgeOffRuleLoader.RuleConfig ruleConfig;

    public RuleConfigDocument(AgeOffRuleLoader.RuleConfig ruleConfig) {
        super();

        this.ruleConfig = ruleConfig;

        this.rule = createRuleElement();
        super.appendChild(this.rule);

        appendElementsToRule();
    }

    private Element createRuleElement() {
        Element rule = this.createElement(RULE_ELEMENT_NAME);

        if (null != this.ruleConfig.label) {
            rule.setAttribute(LABEL_ATTRIBUTE_NAME, this.ruleConfig.label);
        }

        if (this.ruleConfig.isMerge) {
            rule.setAttribute(MODE_ATTRIBUTE_NAME, MERGE_ATTRIBUTE_VALUE);
        }
        return rule;
    }

    private void appendElementsToRule() {
        appendFilterClassElement();
        appendMergeElement();
        appendTtlElement();
        appendMatchPatternElement();
        appendCustomElements();
    }

    private void appendFilterClassElement() {
        Element filterClassElement = super.createElement(FILTER_CLASS_ELEMENT_NAME);
        filterClassElement.setTextContent(this.ruleConfig.filterClassName);
        rule.appendChild(filterClassElement);
    }

    private void appendCustomElements() {
        if (null != this.ruleConfig.customElements) {
            for (Element customElement : this.ruleConfig.customElements) {
                Node importedNode = super.importNode(customElement, true);
                rule.appendChild(importedNode);
            }
        }
    }

    private void appendMatchPatternElement() {
        if (null != this.ruleConfig.matchPattern && !this.ruleConfig.matchPattern.isBlank()) {
            disableCommentEscaping(rule);

            Element matchPatternElement = super.createElement(MATCH_PATTERN_ELEMENT_NAME);
            matchPatternElement.setTextContent("\n" + this.ruleConfig.matchPattern);
            rule.appendChild(matchPatternElement);

            enableCommentEscaping(rule);
        }
    }

    private void appendTtlElement() {
        if (null != this.ruleConfig.ttlValue) {
            Element ttlElement = super.createElement(TTL_ELEMENT_NAME);
            ttlElement.setAttribute(TTL_UNITS_ATTRIBUTE_NAME, this.ruleConfig.ttlUnits);
            ttlElement.setTextContent(this.ruleConfig.ttlValue);
            rule.appendChild(ttlElement);
        }
    }

    private void appendMergeElement() {
        if (this.ruleConfig.isMerge) {
            Element mergeElement = super.createElement(IS_MERGE_ELEMENT_NAME);
            mergeElement.setTextContent(Boolean.TRUE.toString());
            rule.appendChild(mergeElement);
        }
    }

    private void enableCommentEscaping(Element rule) {
        adjustEscaping(rule, StreamResult.PI_ENABLE_OUTPUT_ESCAPING);
    }

    private void disableCommentEscaping(Element rule) {
        adjustEscaping(rule, StreamResult.PI_DISABLE_OUTPUT_ESCAPING);
    }

    private void adjustEscaping(Element rule, String piEnableOutputEscaping) {
        for (char specialCharacter : COMMENT_ESCAPE_CHARACTERS) {
            ProcessingInstruction escapeInstruction = super.createProcessingInstruction(piEnableOutputEscaping, String.valueOf(specialCharacter));
            rule.appendChild(escapeInstruction);
        }
    }
}
