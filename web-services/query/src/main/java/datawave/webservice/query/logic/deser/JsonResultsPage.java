package datawave.webservice.query.logic.deser;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.JsonAdapter;
import datawave.webservice.query.cache.ResultsPage;

/**
 * Reflects a Results Page sent as a raw JSON Page, when we expect the ResultsPage 'pages' to contain
 * JSON themselves.
 */
public class JsonResultsPage {
    @JsonAdapter(ResultsPageAdapter.class)
    static GsonBuilder gsonBuilder = new GsonBuilder();
    static Gson gson;
    static{
        gsonBuilder.registerTypeAdapter(JsonResultsPage.class,new ResultsPageAdapter());
        gson= gsonBuilder.create();
    }

    private final ResultsPage page;
    private final long pageNumber;


    public JsonResultsPage(ResultsPage page, long pageNumber){
        this.page=page;
        this.pageNumber=pageNumber;
    }

    public static String serialize(final JsonResultsPage page) {
        return gson.toJson(page);
    }

    public ResultsPage getPage() {
        return page;
    }

    public long getPageNumber() {
        return pageNumber;
    }
}
