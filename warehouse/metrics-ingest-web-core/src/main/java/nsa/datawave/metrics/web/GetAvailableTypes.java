package nsa.datawave.metrics.web;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map.Entry;

import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;

import com.google.gson.JsonArray;
import com.google.gson.JsonPrimitive;

@WebServlet({"services/gat", "services/gats"})
public class GetAvailableTypes extends MetricsServlet {
    
    private static final long serialVersionUID = 1580754645705891683L;
    
    @Override
    public void doGet(CloudContext ctx, HttpServletRequest req, HttpServletResponse resp) throws IOException {
        Scanner scan = ctx.createIngestScanner();
        scan.setRange(new Range("datatype", "datatype"));
        
        HashSet<String> dtypes = new HashSet<>();
        for (Entry<Key,Value> e : scan) {
            dtypes.add(e.getKey().getColumnFamily().toString());
        }
        
        JsonArray rtypes = new JsonArray();
        for (String type : dtypes) {
            rtypes.add(new JsonPrimitive(type));
        }
        
        resp.setContentType("application/json");
        resp.getWriter().write(rtypes.toString());
    }
}
