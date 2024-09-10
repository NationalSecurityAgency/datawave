package datawave.ingest.json.config.helper;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.NormalizedFieldAndValue;
import datawave.ingest.data.config.ingest.ContentBaseIngestHelper;
import datawave.ingest.data.normalizer.SimpleGroupFieldNameParser;
import datawave.ingest.json.util.JsonObjectFlattener;

/**
 * Utilized by EventMapper to produce all the key/value pairs from each raw record, i.e, {@link RawRecordContainer}
 */
public class JsonIngestHelper extends ContentBaseIngestHelper {

    private static final Logger log = Logger.getLogger(JsonIngestHelper.class);

    protected JsonDataTypeHelper helper = null;
    protected JsonObjectFlattener flattener = null;
    protected SimpleGroupFieldNameParser groupNormalizer = new SimpleGroupFieldNameParser();

    @Override
    public void setup(Configuration config) {
        super.setup(config);
        helper = new JsonDataTypeHelper();
        helper.setup(config);
        this.setEmbeddedHelper(helper);
        flattener = helper.newFlattener();
    }

    @Override
    public Multimap<String,NormalizedContentInterface> getEventFields(RawRecordContainer event) {

        if (null == flattener) {
            throw new IllegalStateException("JsonObjectFlattener was not initialized. Method 'setup' must be invoked first");
        }

        HashMultimap<String,String> fields = HashMultimap.create();
        String jsonString = new String(event.getRawData());

        JsonParser parser = new JsonParser();
        JsonElement jsonElement = parser.parse(jsonString);
        flattener.flatten(jsonElement.getAsJsonObject(), fields);

        return normalizeMap(getGroupNormalizedMap(fields));
    }

    protected Multimap<String,NormalizedContentInterface> getGroupNormalizedMap(HashMultimap<String,String> fields) {
        Multimap<String,NormalizedContentInterface> results = HashMultimap.create();
        for (Map.Entry<String,String> e : fields.entries()) {
            if (e.getValue() != null) {
                results.put(e.getKey(), new NormalizedFieldAndValue(e.getKey(), e.getValue()));
            }
        }
        return groupNormalizer.extractFieldNameComponents(results);
    }
}
