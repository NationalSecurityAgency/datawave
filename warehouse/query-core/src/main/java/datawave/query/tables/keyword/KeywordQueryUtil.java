package datawave.query.tables.keyword;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Content;
import datawave.query.attributes.TypeAttribute;
import datawave.util.keyword.language.YakeLanguage;

public class KeywordQueryUtil {
    /**
     * Choose the first identifier from a non-null, non-empty list of identifiers, otherwise return null.
     *
     * @param identifiers
     *            a list to choose from
     * @return the first identifier or null.
     */
    public static String chooseBestIdentifier(List<String> identifiers) {
        if (identifiers == null || identifiers.isEmpty()) {
            return null;
        }
        return identifiers.get(0);
    }

    /**
     * Choose the best language from a non-null, non-empty list of languages, otherwise return null.
     *
     * @param languages
     *            a list to choose from
     * @return the best identifier or null.
     */
    public static String chooseBestLanguage(List<String> languages) {
        if (languages == null || languages.isEmpty()) {
            return null;
        }

        for (String language : languages) {
            // if the language can't be found in the language registry, the language
            // registry will return English. So, if the language name returned by the
            // registry and the input language name match - it confirms we have
            // a good positive match, so just return that. If they don't match,
            // it's a good chance that we're just getting the default value from the registry.
            YakeLanguage yakeLanguage = YakeLanguage.Registry.find(language);
            if (yakeLanguage.getLanguageName().equalsIgnoreCase(language)) {
                return language;
            }
        }

        // if we get here, we couldn't find an ideal language, just return the first value, yake will default
        // to processing the data as if it were English.
        return languages.get(0);
    }

    /**
     * Read strings from a couple known Attribute types - TypeAttribute and Content. Relies on any TypeAttribute to have a delegate that produces a string in
     * order to render properly. Handles multivalued field by unpacking and flattening the Attributes class.
     * <p>
     * Generally LANGUAGE is expected to be a TypeAttribute, while HIT_TERM (which exposes the identifier) is expected to be Content.
     * </p>
     *
     * @param inputAttribute
     *            the attribute to extract strings from
     * @return the string version of the attribute value.
     */
    public static List<String> getStringValuesFromAttribute(Attribute<?> inputAttribute) {
        final List<String> values = new ArrayList<>();
        final Queue<Attribute<?>> attributeQueue = new LinkedList<>();
        attributeQueue.add(inputAttribute);
        Attribute<?> attribute;
        while ((attribute = attributeQueue.poll()) != null) {
            String value = "";
            if (attribute.getClass().isAssignableFrom(Attributes.class)) {
                Attributes attributes = (Attributes) attribute;
                attributeQueue.addAll(attributes.getAttributes());
            } else if (attribute.getClass().isAssignableFrom(TypeAttribute.class)) {
                value = attribute.toString();
            } else if (attribute.getClass().isAssignableFrom(Content.class)) {
                value = attribute.getData().toString();
            }
            // add the attribute string if we got one and it's non-blank
            if (!value.isBlank()) {
                values.add(value);
            }
        }
        return values;
    }
}
