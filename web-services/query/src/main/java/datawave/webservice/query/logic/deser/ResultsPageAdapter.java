package datawave.webservice.query.logic.deser;

import com.google.common.base.Joiner;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.util.Objects;

public class ResultsPageAdapter extends TypeAdapter<JsonResultsPage> {

    @Override
    public void write(JsonWriter jsonWriter, JsonResultsPage resultsPage) throws IOException {
        Objects.requireNonNull(resultsPage);
        // outside object
        jsonWriter.beginObject();
            // size
            jsonWriter.name("pageNumber");
            jsonWriter.value(resultsPage.getPageNumber());
            // size end
            // size
            jsonWriter.name("size");
            jsonWriter.value(resultsPage.getPage().getResults().size());
            // size end
            // events
            jsonWriter.name("events");
            jsonWriter.beginArray();
            jsonWriter.jsonValue(Joiner.on(",").join(resultsPage.getPage().getResults()));
            jsonWriter.endArray();
            // events end
        // outside object
        jsonWriter.endObject();

    }

    @Override
    public JsonResultsPage read(JsonReader jsonReader) throws IOException {
        return null;
    }
}
