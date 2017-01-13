package nsa.datawave.metrics.web;

import java.io.IOException;

import javax.inject.Inject;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.util.Pair;

public abstract class MetricsServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;
    
    @Inject
    private CloudContext cloudContext;
    
    @Override
    protected final void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        
        doGet(cloudContext, req, resp);
    }
    
    public abstract void doGet(CloudContext connection, HttpServletRequest req, HttpServletResponse rsp) throws IOException;
    
    public static String getArg(String name, String defaultValue, HttpServletRequest req) {
        String val = req.getParameter(name);
        if (val == null) {
            return defaultValue;
        } else {
            return val;
        }
    }
    
    public static Pair<String,String> getStartAndEnd(HttpServletRequest req) throws IOException {
        String start = req.getParameter("start");
        String end = req.getParameter("end");
        try {
            if (end == null) {
                end = Long.toString(System.currentTimeMillis());
            }
            return new Pair<>(start, end);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
}
