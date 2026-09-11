# Query Validation API

Datawave supports the ability to pre-validate a query and check for common issues. Typically, a query is validated by submitting it to the `/<logicName>/validate` REST endpoint, which will return a [QueryValidationResponse](../../../../../../../../web-services/client/src/main/java/datawave/webservice/result/QueryValidationResponse.java).

## Activating Rules
To activate a new validation rule for use with [ShardQueryLogic](../tables/ShardQueryLogic.java), you must define beans for each [QueryRule](QueryRule.java) instance you want to have available, and then add the bean to list of rules configured for the `validationRules` property of the bean for the logic you want the rules activated for. See the [QueryLogicFactory.xml](../../../../../../../../web-services/deploy/configuration/src/main/resources/datawave/query/QueryLogicFactory.xml) for examples.

## Adding New Rules
Adding a new rule is simple. 
- If adding a new rule to use with [ShardQueryLogic](../tables/ShardQueryLogic.java), add a new class that extends [ShardQueryRule](ShardQueryRule.java) and implement your custom rule logic therein. Your rule must define whether it supports LUCENE or JEXL queries via `ShardQueryRule.getSupportedSyntax()`. 
- If adding a new rule for a logic that cannot be satisfied by (or does not inherit) the default implementation of `ShardQueryLogic.validateQuery()`, you will need to add custom implementations of [QueryRule](QueryRule.java) and [QueryValidationConfiguration](QueryValidationConfiguration.java). You may be able to use or extend [QueryConfigurationValidationImpl](QueryValidationConfigurationImpl.java). 

Once the rule is ready for use, follow the steps above for activating the rule.

## Available Rules

### [AmbiguousNotRule](AmbiguousNotRule.java)
Checks a LUCENE query for any ambiguous usages of NOT with OR'd/AND'd terms before it that are unwrapped.
  - `FIELD1:abc OR FIELD2:def NOT FIELD3:123` should be `(FIELD1:abc OR FIELD2:def) NOT FIELD3:123`.

### [AmbiguousOrPhrasesRule](AmbiguousOrPhrasesRule.java)
Checks a LUCENE query for any fielded terms with unfielded terms directly OR'd with them afterward.
  - `FOO:abc OR def` should be `FOO:(abc OR def)`
  - `(FOO:abc OR def)` should be `FOO:(abc OR def)`
  - `FOO:abc OR (def OR ghi)` should be `FOO:(abc OR def OR ghi)`

### [AmbiguousUnquotedPhrasesRule](AmbiguousUnquotedPhrasesRule.java)
Checks a LUCENE query for any unquoted phrases that are implicitly AND'd with a preceding fielded term.
  - `FOO:term1 term2 term3` should be `FOO:"term1 term2 term3"`.

### [FieldExistenceRule](FieldExistenceRule.java)
Checks a JEXL query for any non-existent fields, i.e., not present in the data dictionary.

### [FieldPatternPresenceRule](FieldPatternPresenceRule.java)
This rule can be configured with the following:
  - A map of field names to descriptive messages.
  - A map fo regex patterns to descriptive messages.

The rule will check a JEXL query for the presence of the configured fields or regex patterns. If any are found, the corresponding descriptive messages will be returned.

### [GroupedInterpretationRule](GroupedInterpretationRule.java)
Checks a LUCENE query for any grouped phrases with fields.
- `FOO:(aaa bbb ccc)` will result in a warning that it will be interpreted as `FOO:(aaa AND bbb AND ccc)`.

### [IncludeExcludeArgsRule](IncludeExcludeArgsRule.java)
Checks a LUCENE query for any of the following issues with any #INCLUDE or #EXCLUDE functions in the query:
- Supplied with no arguments.
- Supplied with an uneven number of arguments.
- Supplied with no arguments after the first boolean argument.
- Supplied with an uneven number of arguments after the first boolean argument.

### ~~[IncludeExcludeIndexFieldsRule](IncludeExcludeIndexFieldsRule.java)~~ (Deprecated)
Checks a JEXL query for the usage of any indexed fields as arguments for any `filter:includeRegex` or `filter:excludeRegex` functions in the query. 

### [IncludeExcludeIndexOnlyFieldsRule](IncludeExcludeIndexOnlyFieldsRule.java)
Checks a JEXL query for the usage of any indexed fields as arguments for any `filter:includeRegex` or `filter:excludeRegex` functions in the query.

### [InvalidQuoteRule](InvalidQuoteRule.java)
Checks a LUCENE query for any instances of `` ` `` instead of `'` being used to quote a phrase.

### [MinimumSlopProximityRule](MinimumSlopProximityRule.java)
Checks a LUCENE query for any slop phrases where the number is smaller than the number of terms.
- `FIELD:\"term1 term2 term3\"~1`: The number 1 should be 3 or greater.

### [NumericValueRule](NumericValueRule.java)
Checks a JEXL query for the usage of numeric values for non-numeric fields.

### [TimeFunctionRule](TimeFunctionRule.java)
Checks a JEXL query for any usage of non-date field arguments for the function `filter:timeFunction`.

### [UnescapedSpecialCharsRule](UnescapedSpecialCharsRule.java)
Checks a JEXL query for the presence of any unescaped special characters in literals or regex patterns. This rule supports configuring the following:
- Characters that should not be considered special characters in literals.
- Characters that should not be considered special characters in regex patterns. The following reserved regex characters will always be considered special characters: `. + * ? ^ $ ( ) [ ] { } | \`.
- Whether whitespaces are required to be escaped in literals (defaults to false).
- Whether whitespaces are required to be escaped in regex patterns (defaults to false). 

### [UnescapedWildcardsInPhrasesRule](UnescapedWildcardsInPhrasesRule.java)
Checks a LUCENE query for the presence of any unescaped wildcard characters in a quoted message.
- `FOO:"*abc" OR FOO:"de*f" OR FOO: "efg*"` will result in warning messages about each quoted phrase.

### [UnfieldedTermsRule](UnfieldedTermsRule.java)
Checks a LUCENE query for any unfielded terms.
- `FOO:123 643 OR abc 'bef'` will result in warning messages about the terms `643`, `abc`, and `'bef'`.

