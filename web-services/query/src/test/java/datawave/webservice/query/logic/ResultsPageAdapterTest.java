package datawave.webservice.query.logic;

import datawave.webservice.query.cache.ResultsPage;
import datawave.webservice.query.logic.deser.JsonResultsPage;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;

public class ResultsPageAdapterTest {

    @Test
    public void testPopulatedPage(){
        JsonResultsPage page = new JsonResultsPage(new ResultsPage(),1);
        var results = new ArrayList<>();
        results.add("{ \"field\" : \"value\" }");
        results.add("{ \"field\" : \"value2\" }");
        page.getPage().setResults(results);

        String res = JsonResultsPage.serialize(page);
        Assert.assertEquals("{\"pageNumber\":1,\"size\":2,\"events\":[{ \"field\" : \"value\" },{ \"field\" : \"value2\" }]}",res);
    }

    @Test
    public void testEmptyPage(){
        JsonResultsPage page = new JsonResultsPage(new ResultsPage(),1);
        var results = new ArrayList<>();
        page.getPage().setResults(results);

        String res = JsonResultsPage.serialize(page);
        Assert.assertEquals("{\"pageNumber\":1,\"size\":0,\"events\":[]}",res);
    }
}
