package datawave.query.language.functions.jexl;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

public class TestChainStart {
    
    @Test
    public void testQueryParam() {
        ChainStart chainStart = new ChainStart();
        List<String> parameterList = new ArrayList<>();
        parameterList.add("EventQuery");
        parameterList.add("F1:S1 AND F2:S2");
        parameterList.add("20140101");
        parameterList.add("20140131");
        parameterList.add("");
        chainStart.initialize(parameterList, 1, null);
    }
    
    @Test
    public void testStartParam() {
        ChainStart chainStart = new ChainStart();
        List<String> parameterList = new ArrayList<>();
        parameterList.add("EventQuery");
        parameterList.add("F1:S1 AND F2:S2");
        parameterList.add("20140101 000000");
        parameterList.add("20140131");
        parameterList.add("");
        chainStart.initialize(parameterList, 1, null);
    }
}
