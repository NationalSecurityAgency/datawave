---
title: "Query Syntax Guide"
tags: [getting_started, query]
summary: This page gives an overview of DataWave's JEXL and Lucene query syntax options
---
<h2>Introduction</h2>
<p>DataWave will typically accept query expressions conforming to either JEXL syntax (the default) or a modified Lucene syntax.
   As for JEXL, DataWave supports a subset of the language elements in the <a href="https://commons.apache.org/proper/commons-jexl/reference/syntax.html"
   target="_blank">Apache Commons JEXL grammar</a> and also provides several custom JEXL functions. DataWave has
   enabled support for Lucene expressions as a convenience and will provide equivalent functionality to JEXL,
   except where noted below.
</p>
<h2>JEXL Query Syntax</h2>
<hr />
<h3>Supported JEXL Operators</h3>
<ul>
    <li>==</li>
    <li>!=</li>
    <li>&lt;</li>
    <li>&le;</li>
    <li>&gt;</li>
    <li>&ge;</li>
    <li>=~ (regex)</li>
    <li>!~ (negative regex)</li>
    <li>|| , or</li>
    <li>&amp;&amp; , and</li>
</ul>
<hr />
<h3>Custom JEXL Functions</h3>
<ul>
    <li>Content functions<ul><li>phrase()</li><li>adjacent()</li><li>within()</li></ul></li>
    <li>Geospatial Functions
       <ul>
         <li>within_bounding_box()</li>
         <li>within_circle()</li>
         <li>intersects_bounding_box()</li>
         <li>intersects_radius_km()</li>
         <li>contains()</li>
         <li>covers()</li>
         <li>coveredBy()</li>
         <li>crosses()</li>
         <li>intersects()</li>
         <li>overlaps()</li>
         <li>within()</li>
       </ul>
    </li>
    <li>Utility Functions<ul><li>between()</li><li>length()</li></ul></li>
</ul>
<hr />
<h2>A Note About Field Names</h2>
<p>
Field names in DataWave are required to conform to JEXL naming conventions. That is, a field name must contain only alphanumeric
characters and underscores, and the name must begin with either an alphabetic character or an underscore.
</p>
<h3>Structured vs Unstructured Objects</h3>
<p>By extension, this would seem to imply that a given data object in DataWave, which is largely just a collection of field
name/value pairs, is strictly a flat data structure, given that there is no apparent way to encode hierarchical structure
within a JEXL field name.
</p>
<p>While DataWave does adhere to 'flat object' semantics in most respects, it does allow the natural
hierarchical structure of a field to be encoded and retained during data ingest, if needed. In fact, DataWave Query clients
can retrieve such objects by leveraging both the flattened view <em>and</em> the hierarchical view of an object
via their query expressions.</p>

See the section on <a href="#support-for-hierarchical-data">hierarchical data</a>
below for more information.

<h3>JEXL Unfielded Queries</h3>
<p>
  JEXL is an expression language and not a text-query language per se, so JEXL doesn't natively support the notion of an 
  'unfielded' query, that is, a query expression containing only search terms and no specific field names to search within.
</p>
<p>
  As a convenience, DataWave does provide support for unfielded JEXL queries, at least for the subset of 
  <a href="development#query-logic-components">query logic</a> types that are designed to retrieve objects from the 
  <a href="../getting-started/data-model#primary-data-table">primary data table</a>. To achieve this with JEXL, the user must add the 
  internally-recognized pseudo field, <b>_ANYFIELD_</b>, to the query in order for it to pass syntax validation.
  See the examples below for usage.
</p>
<hr/>
<h2>Lucene Query Syntax</h2>
<p>DataWave provides a slightly modified Lucene syntax, such that the <b>NOT</b> operator is not unary, <b>AND</b> operators are not
fuzzy, and the implicit operator is <b>AND</b> instead of <b>OR</b>.
</p>
<p>Our Lucene syntax has the following form</p>
<pre>
    ModClause ::= DisjQuery [NOT DisjQuery]*
    DisjQuery ::= ConjQuery [ OR ConjQuery ]*
    ConjQuery ::= Query [ AND Query ]*
    Query ::= Clause [ Clause ]*
    Clause ::= Term | [ ModClause ]
    Term ::=
        field:selector |
        field:selec* |
        field:selec*or |
        field:*lector |
        field:selec?or |
        selector | (can use wildcards)
        field:[begin TO end] |
        field:{begin TO end} |
        "quick brown dog" |
        "quick brown dog"~20 |
        #FUNCTION(ARG1, ARG2)
</pre>
<p>Note that to search for punctuation characters within a term, you need to escape it with a backslash.</p>
<hr/>
<h2>JEXL and Lucene Examples</h2>

<h3>Example Queries</h3>
<table>
    <tr><th>JEXL Query</th><th>Lucene Query</th></tr>
    <tr class="highlight">
       <td>_ANYFIELD_ == 'SomeValue'</td><td>SomeValue</td>
    </tr>
    <tr>
        <td>(_ANYFIELD_ == 'AAA' &amp;&amp; _ANYFIELD_ == 'BBB') &amp;&amp; (_ANYFIELD_ == 'CCC' || _ANYFIELD_ == 'DDD')</td><td>(AAA BBB) (CCC OR DDD)</td>
    </tr>   
    <tr class="highlight">
        <td>FIELDNAME == 'SomeValue'</td><td>FIELDNAME:SomeValue</td>
    </tr>
    <tr>
        <td>FIELDNAME =~ 'SomeVal.*'</td><td>FIELDNAME:SomeVal*</td>
    </tr>
    <tr class="highlight">
        <td>(FIELD1 == 'AAA' &amp;&amp; FIELD2 == 'BBB') &amp;&amp; (FIELD3 == 'CCC' || FIELD3 == 'DDD')</td><td>(FIELD1:AAA FIELD2:BBB) (FIELD3:CCC OR FIELD3:DDD)</td>
    </tr>
    <tr>
        <td>TEXT_FIELD == 'TextValue' &amp;&amp; f:between(NUMBER_FIELD,1, 10)</td><td>TEXT_FIELD:TextValue AND NUMBER_FIELD:[1 TO 10]</td>
    </tr>
</table>

<h3>Support for Hierarchical Data</h3>

DataWave also allows for queries that leverage the hierarchical context of structured data types,
provided that the structure of the data is preserved during ingest. This requires that a special "grouping"
notation be applied during ingest to any nested fields that are parsed.

For example, consider the XML objects below:

**Data Object 1**
```xml
   <foo>
      <parent>
         <child>
            <field1>A</field1>
         </child>
         <child>
            <field2>B</field2>
         </child>
      </parent>
   </foo>
```
**Data Object 2**
```xml
   <bar>
      <field1>A</field1>
      <field2>B</field2>
   </bar>
```

At ingest time, the field name/value pairs above may be stored logically in Accumulo as follows:

**{FIELD NAME}.{GROUPING CONTEXT} = {VALUE}**

Here, the *grouping context* preserves the fully-qualified path of the name/value pair and also conveys its relative position
within the hierarchy.

Thus, given the XML above, we could flatten the data objects during ingest, transforming the name/value pairs as follows:

```
  # Data Object 1 flattened, including grouping context
  FIELD1.FOO_0.PARENT_0.CHILD_0.FIELD1_0 = A
  FIELD2.FOO_0.PARENT_0.CHILD_1.FIELD2_0 = B
  -------------------------
  # Data Object 2 flattened, including grouping context
  FIELD1.BAR_0.FIELD1_0 = A
  FIELD2.BAR_0.FIELD2_0 = B
```

As a result, the objects are flattened into simple maps with each consisting of two fields, 'FIELD1' and 'FIELD2'. By default,
the grouping context (i.e., all characters beyond the field name itself) will be ignored by the query API. Therefore, the
following simple query could be used to return *both* objects as distinct search results:

<table>
    <tr><th>JEXL Query</th><th>Lucene Query</th></tr>
    <tr class="highlight"><td>FIELD1 == 'A' &amp;&amp; FIELD2 == 'B'</td><td>FIELD1:A FIELD2:B</td></tr>
</table>

However, if the objects originated from distinct XML schemas having entirely different semantics for their respective
fields, then we might not want both to appear in our search results. To disambiguate the two objects, we can use the
following function:

<table>
    <tr><th>JEXL Function</th><th>Lucene Function</th></tr>
    <tr class="highlight"><td>grouping:matchesInGroupLeft(F1, 'V1', F2, 'V2', ..., Fn, 'Vn', INTEGER)</td><td>#MATCHES_IN_GROUP_LEFT(F1, 'V1', F2, 'V2', ..., Fn, 'Vn', INTEGER)</td></tr>
</table>

The INTEGER parameter denotes the level in the tree where the matching field name/value pairs must exist in order to
constitute a match. Its values are defined as follows:

* 0 : Fields are siblings / same parent element
* 1 : Fields are cousins / same grandparent element
* 2 : Fields are 2nd cousins / same great-grandparent element
* And so on...

For example, to retrieve only *Data Object 1* above, we might use the following query:

<table>
    <tr><th>JEXL Function</th><th>Lucene Function</th></tr>
    <tr class="highlight"><td>FIELD1 == 'A' &amp;&amp; grouping:matchesInGroupLeft(FIELD1, 'A', FIELD2, 'B', 1)</td><td>FIELD1:A AND #MATCHES_IN_GROUP_LEFT(FIELD1, 'A', FIELD2, 'B', 1)</td></tr>
</table>

Likewise, to return only *Data Object 2*, we could do the following:

<table>
    <tr><th>JEXL Function</th><th>Lucene Function</th></tr>
    <tr class="highlight"><td>FIELD1 == 'A' &amp;&amp; grouping:matchesInGroupLeft(FIELD1, 'A', FIELD2, 'B', 0)</td><td>FIELD1:A AND #MATCHES_IN_GROUP_LEFT(FIELD1, 'A', FIELD2, 'B', 0)</td></tr>
</table>

<h3>Custom Lucene Functions</h3>
<p>DataWave has augmented Lucene to provide support for several JEXL features that were not supported natively. 
   The table below maps the JEXL operators to the supported Lucene syntax
</p>
<table>
    <tr><th>JEXL Operator</th><th>Lucene Operator</th></tr>
    <tr class="highlight"><td>filter:includeRegex(field, regex)</td><td>#INCLUDE(field, regex)</td></tr>
    <tr><td>filter:excludeRegex(field, regex)</td><td>#EXCLUDE(field, regex)</td></tr>
    <tr class="highlight"><td>filter:includeRegex(field1, regex1) &lt;op&gt; filter:includeRegex(field2, regex2) ...</td><td>#INCLUDE(op, field1, regex1, field2, regex2 ...) where op is 'or' or 'and'</td></tr>
    <tr><td>filter:excludeRegex(field1, regex1) &lt;op&gt; filter:excludeRegex(field2, regex2) ...</td><td>#EXCLUDE(op, field1, regex1, field2, regex2 ...) where op is 'or' or 'and'</td></tr>
    <tr class="highlight"><td>filter:isNull(field)</td><td>#ISNULL(field)</td></tr>
    <tr><td>not(filter:isNull(field))</td><td>#ISNOTNULL(field)</td></tr>
    <tr class="highlight"><td>filter:occurrence(field,operator,count))</td><td>#OCCURRENCE(field,operator,count)</td></tr>
    <tr><td>filter:timeFunction(field1,field2,operator,equivalence,goal)</td><td>#TIME_FUNCTION(field1,field2,operator,equivalence,goal)</td></tr>
</table>
<p>Notes:</p>
<ol>
   <li>None of these filter functions can be applied against index-only fields.</li>
   <li>The occurrence function is used to count the number of instances of a field in the event.  Valid operators are '==' (or '='),'>','>=','<','<=', and '!='. </li>
</ol>
<h3>Basic Geospatial Functions</h3>
<p>
Some geo functions are supplied as well that may prove useful although the within_bounding_box function may be done with a simple range comparison (i.e. LAT_LON_USER &lt;= &lt;lat1&gt;_&lt;lon1&gt; and LAT_LON_USER &gt;= &lt;lat2&gt;_&lt;lon2&gt;.
</p>
<table>
    <tr><th>JEXL Operator</th><th>Lucene Operator</th></tr>
    <tr class="highlight"><td>geo:within_bounding_box(latLonField, lowerLeft, upperRight)</td><td>#GEO(bounding_box, latLonField, 'lowerLeft', 'upperRight')</td></tr>
    <tr><td>geo:within_bounding_box(lonField, latField, minLon, minLat, maxLon, maxLat)</td><td>#GEO(bounding_box, lonField, latField, minLon, minLat, maxLon, maxLat)</td></tr>
    <tr class="highlight"><td>geo:within_circle(latLonField, center, radius)</td><td>#GEO(circle, latLonField, center, radius)</td></tr>
</table>
<p>Notes:</p>
<ol>
   <li>All lat and lon values are in decimal.</li>
   <li>The lowerLeft, upperRight, and center are of the form &lt;lat&gt;_&lt;lon&gt; and must be surrounded by single quotes.</li>
   <li>The radius is in decimal degrees as well.</li>
</ol>
<h3>GeoWave Functions</h3>
<a href="https://locationtech.github.io/geowave" target="_blank">GeoWave</a> is an optional component that provides the following
functions when enabled
<table>
    <tr><th>JEXL Operator</th><th>Lucene Operator</th></tr>
    <tr class="highlight"><td>geowave:intersects_bounding_box(geometryField, westLon, eastLon, southLat, northLat)</td><td>#INTERSECTS_BOUNDING_BOX(geometryField, westLon, eastLon, southLat, northLat)</td></tr><tr>
    <td>geowave:intersects_radius_km(geometryField, centerLon, centerLat, radiusKm)</td><td>#INTERSECTS_RADIUS_KM(geometryField, centerLon, centerLat, radiusKm)</td></tr>
    <tr class="highlight"><td>geowave:contains(geometryField, Well-Known Text)</td><td>#CONTAINS(geometryField, centerLon, centerLat, radiusDegrees)</td></tr>
    <tr><td>geowave:covers(geometryField, Well-Known Text)</td><td>#COVERS(geometryField, Well-Known Text)</td></tr>
    <tr class="highlight"><td>geowave:coveredBy(geometryField, Well-Known Text)</td><td>#COVERED_BY(geometryField, Well-Known Text)</td></tr>
    <tr><td>geowave:crosses(geometryField, Well-Known Text)</td><td>#CROSSES(geometryField, Well-Known Text)</td></tr>
    <tr class="highlight"><td>geowave:intersects(geometryField, Well-Known Text)</td><td>#INTERSECTS(geometryField, Well-Known Text)</td></tr>
    <tr><td>geowave:overlaps(geometryField, Well-Known Text)</td><td>#OVERLAPS(geometryField, Well-Known Text)</td></tr>
    <tr class="highlight"><td>geowave:within(geometryField, Well-Known Text)</td><td>#WITHIN(geometryField, Well-Known Text)</td></tr>
</table>
<p>Notes:</p>
<ol>
    <li>All lat and lon values are in decimal degrees.</li>
    <li>The lowerLeft, upperRight, and center are of the form &lt;lat&gt;_&lt;lon&gt; and must be surrounded by single quotes.</li>
    <li>Geometry is represented according to the Open Geospatial Consortium standard for Well-Known Text. It is in decimal degrees longitude for x, amd latitude for y.  For example, a point at New York can be represented as 'POINT (-74.01 40.71)' and a box at New York can be repesented as 'POLYGON(( -74.1 40.75, -74.1 40.69, -73.9 40.69, -73.9 40.75, -74.1 40.75)); </li>
</ol>
<h3>Date Functions</h3>
<p>There are some additional functions that are supplied to handle dates more smoothly.  It is intended that the need for these functions
    may go away in future versions (<b>bolded</b> parameters are literal, other parameters are substituted with appropriate values):
</p>
<table>
    <tr><th>JEXL Operator</th><th>Lucene Operator</th></tr>
    <tr class="highlight"><td>filter:betweenDates(field, start date, end date)</td><td>#DATE(field, start date, end date) or #DATE(field, <b>between</b>, start date, end date)</td></tr>
    <tr><td>filter:betweenDates(field, start date, end date, start/end date format)</td><td>#DATE(field, start date, end date, start/end date format) or #DATE(field, <b>between</b>, start date, end date, start/end date format)</td></tr>
    <tr class="highlight"><td>filter:betweenDates(field, field date format, start date, end date, start/end date format)</td><td>#DATE(field, field date format, start date, end date, start/end date format) or #DATE(field, <b>between</b>, field date format, start date, end date, start/end date format)</td></tr>
    <tr><td>filter:afterDate(field, date)</td><td>#DATE(field, <b>after</b>, date)</td></tr>
    <tr class="highlight"><td>filter:afterDate(field, date, date format)</td><td>#DATE(field, <b>after</b>, date, date format)</td></tr>
    <tr><td>filter:afterDate(field, field date format, date, date format)</td><td>#DATE(field, <b>after</b>, field date format, date, date format)</td></tr>
    <tr class="highlight"><td>filter:beforeDate(field, date)</td><td>#DATE(field, <b>before</b>, date)</td></tr>
    <tr><td>filter:beforeDate(field, date, date format)</td><td>#DATE(field, <b>before</b>, date, date format)</td></tr>
    <tr class="highlight"><td>filter:beforeDate(field, field date format, date, date format)</td><td>#DATE(field, <b>before</b>, field date format, date, date format)</td></tr>
    <tr><td>filter:betweenLoadDates(<b>LOAD_DATE</b>, start date, end date)</td><td>#LOADED(start date, end date) or #LOADED(<b>between</b>, start date, end date)</td></tr>
    <tr class="highlight"><td>filter:betweenLoadDates(<b>LOAD_DATE</b>, start date, end date, start/end date format)</td><td>#LOADED(start date, end date, start/end date format) or #LOADED(<b>between</b>, start date, end date, start/end date format)</td></tr>
    <tr><td>filter:afterLoadDate(<b>LOAD_DATE</b>, date)</td><td>#LOADED(<b>after</b>, date)</td></tr>
    <tr class="highlight"><td>filter:afterLoadDate(<b>LOAD_DATE</b>, date, date format)</td><td>#LOADED(<b>after</b>, date, date format)</td></tr>
    <tr><td>filter:beforeLoadDate(<b>LOAD_DATE</b>, date)</td><td>#LOADED(<b>before</b>, date)</td></tr>
    <tr class="highlight"><td>filter:beforeLoadDate(<b>LOAD_DATE</b>, date, date format)</td><td>#LOADED(<b>before</b>, date, date format)</td></tr>
    <tr><td>filter:timeFunction(<b>DOWNTIME</b>, <b>UPTIME</b>, '-', '>', 2522880000000L)</td><td>#TIME_FUNCTION(<b>DOWNTIME</b>, <b>UPTIME</b>, '-', '>', '2522880000000L')</td></tr>
</table>
<p>Notes:</p>
<ol>
    <li>None of these filter functions can be applied against index-only fields.</li>
    <li>Between functions are inclusive, and the other functions are exclusive of the entered dates.</li>
    <li>Date formats must be entered in the Java SimpleDateFormat object format.</li>
    <li>If the entered date format is not specified, then the following list of date formats will be tried:</li>
    <ul>
        <li>yyyyMMdd:HH:mm:ss:SSSZ</li>
        <li>yyyyMMdd:HH:mm:ss:SSS</li>
        <li>EEE MMM dd HH:mm:ss zzz yyyy</li>
        <li>d MMM yyyy HH:mm:ss 'GMT'</li>
        <li>yyyy-MM-dd HH:mm:ss.SSS Z</li>
        <li>yyyy-MM-dd HH:mm:ss.SSS</li>
        <li>yyyy-MM-dd HH:mm:ss.S Z</li>
        <li>yyyy-MM-dd HH:mm:ss.S</li>
        <li>yyyy-MM-dd HH:mm:ss Z</li>
        <li>yyyy-MM-dd HH:mm:ssz</li>
        <li>yyyy-MM-dd HH:mm:ss</li>
        <li>yyyyMMdd HHmmss</li>
        <li>yyyy-MM-dd'T'HH'|'mm</li>
        <li>yyyy-MM-dd'T'HH':'mm':'ss'.'SSS'Z'</li>
        <li>yyyy-MM-dd'T'HH':'mm':'ss'Z'</li>
        <li>MM'/'dd'/'yyyy HH':'mm':'ss</li>
        <li>E MMM d HH:mm:ss z yyyy</li>
        <li>E MMM d HH:mm:ss Z yyyy</li>
        <li>yyyyMMdd_HHmmss</li>
        <li>yyyy-MM-dd</li>
        <li>MM/dd/yyyy</li>
        <li>yyyy-MMMM</li>
        <li>yyyy-MMM</li>
        <li>yyyyMMddHHmmss</li>
        <li>yyyyMMddHHmm</li>
        <li>yyyyMMddHH</li>
        <li>yyyyMMdd</li>
    </ul>
    <li>A special date format of 'e' can be supplied to mean milliseconds since epoch.</li>
</ol>
