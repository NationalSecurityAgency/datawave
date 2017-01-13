package nsa.datawave.metrics.web.dashboard;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.fasterxml.jackson.databind.ObjectMapper;
import nsa.datawave.metrics.analytic.LongArrayWritable;
import nsa.datawave.metrics.keys.AnalyticEntryKey;
import nsa.datawave.metrics.keys.InvalidKeyException;
import nsa.datawave.metrics.web.CloudContext;
import nsa.datawave.metrics.web.MetricsServlet;
import nsa.datawave.webservice.common.extjs.ExtJsResponse;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;
import org.joda.time.MutableDateTime;

/**
 * <code>
 * 
 * @Path("/DataWave/Ingest/Metrics/services/dashboard </code>
 */
@WebServlet("services/dashboard")
public class DashboardServlet extends MetricsServlet {
    
    private static final Logger log = Logger.getLogger(DashboardServlet.class);
    private static final long serialVersionUID = 1L;
    
    /**
     * Retrieve the last 6 hours of ingest statistics.
     *
     * @param ctx
     * @param req
     * @param resp
     *
     * @throws IOException
     */
    @Override
    public void doGet(CloudContext ctx, HttpServletRequest req, HttpServletResponse resp) throws IOException {
        resp.setContentType("application/json");
        resp.addHeader("Access-Control-Allow-Origin", req.getHeader("Origin"));
        resp.addHeader("Access-Control-Allow-Credentials", "true");
        List<IngestSummary> summary = createIngestSummaryList(ctx, 2, 15);
        new ObjectMapper().writeValue(resp.getWriter(), new ExtJsResponse<>(summary));
    }
    
    /**
     *
     * @param ctx
     * @param hours
     *            The number of hours of statistics to retrieve
     * @param interval
     *            The number of minutes to aggregate for each summary
     * @return a list of summaries
     */
    private List<IngestSummary> createIngestSummaryList(CloudContext ctx, int days, int interval) {
        Scanner scan = ctx.createScanner();
        MutableDateTime end = new MutableDateTime();
        MutableDateTime start = end.copy();
        start.addDays(-days);
        scan.setRange(new Range(Long.toString(start.toDate().getTime()), Long.toString(end.toDate().getTime())));
        
        TreeMap<Long,IngestDetail> detailsMap = new TreeMap<>();
        
        for (Map.Entry<Key,Value> entry : scan) {
            IngestDetail ingestDetail = parse(entry);
            if (ingestDetail != null) {
                detailsMap.put(ingestDetail.getTimestamp(), ingestDetail);
            }
        }
        
        return convertIngestDetailsToSummaries(detailsMap, start, end, interval);
    }
    
    /**
     * Convert raw data from the DatawaveMetrics table into a usable <code>IngestDetail</code> model object.
     *
     * @param entry
     *
     * @return null if any errors occurred during parsing
     */
    private IngestDetail parse(Map.Entry<Key,Value> entry) {
        AnalyticEntryKey aek;
        try {
            aek = new AnalyticEntryKey(entry.getKey());
        } catch (InvalidKeyException ex) {
            log.info("Could not parse ingest key.", ex);
            return null;
        }
        LongArrayWritable array = new LongArrayWritable();
        DataInputBuffer dip = new DataInputBuffer();
        byte[] bytes = entry.getValue().get();
        dip.reset(bytes, bytes.length);
        try {
            array.readFields(dip);
        } catch (IOException ex) {
            log.info("Could not unmarshall ingest value.", ex);
            return null;
        }
        
        List<LongWritable> l = array.convert();
        long timestamp = aek.getTimestamp();
        String dataType = aek.getDataType();
        String ingestType = aek.getIngestType();
        
        if (l.size() == 4) {
            return new IngestDetail(timestamp, dataType, ingestType, l.get(0).get(), l.get(1).get(), l.get(2).get(), l.get(3).get(), 0, 0);
        } else if (l.size() == 6) {
            return new IngestDetail(timestamp, dataType, ingestType, l.get(0).get(), l.get(1).get(), l.get(2).get(), l.get(3).get(), l.get(4).get(), l.get(5)
                            .get());
        }
        
        log.info("Ingest value contains wrong number of items: " + l.size());
        return null;
    }
    
    private List<IngestSummary> convertIngestDetailsToSummaries(TreeMap<Long,IngestDetail> results, MutableDateTime rangeStart, MutableDateTime end,
                    int interval) {
        List<IngestSummary> statList = new ArrayList<>(results.size() / interval);
        MutableDateTime rangeEnd = new MutableDateTime(rangeStart.toDate().getTime());
        rangeEnd.addMinutes(interval);
        
        while (end.isAfter(rangeStart)) {
            Date tmpStart = rangeStart.toDate();
            Date tmpEnd = rangeEnd.toDate();
            SortedMap<Long,IngestDetail> subMap = results.subMap(tmpStart.getTime(), tmpEnd.getTime());
            if (subMap.isEmpty()) {
                statList.add(new IngestSummary(rangeEnd.toDate().getTime()));
            } else {
                statList.add(buildIngestSummary(rangeEnd.toDate().getTime(), subMap, interval));
            }
            rangeStart.addMinutes(interval);
            rangeEnd.addMinutes(interval);
        }
        
        return statList;
    }
    
    private IngestSummary buildIngestSummary(long dateTime, SortedMap<Long,IngestDetail> details, int interval) {
        IngestSummary summary = new IngestSummary(dateTime);
        long liveTotalTime = 0;
        long livePollerTime = 0;
        long liveIngestWaitTime = 0;
        long liveIngestTime = 0;
        
        long bulkTotalTime = 0;
        long bulkPollerTime = 0;
        long bulkIngestWaitTime = 0;
        long bulkIngestTime = 0;
        long bulkLoaderWaitTime = 0;
        long bulkLoaderTime = 0;
        
        long liveEventCount = 0;
        long bulkEventCount = 0;
        
        for (Map.Entry<Long,IngestDetail> entry : details.entrySet()) {
            IngestDetail detail = entry.getValue();
            if (detail.isLive()) {
                liveEventCount += detail.getEventCount();
                liveTotalTime += detail.getPollerTime() + detail.getIngestWaitTime() + detail.getIngestTime();
                livePollerTime += detail.getPollerTime();
                liveIngestWaitTime += detail.getIngestWaitTime();
                liveIngestTime += detail.getIngestTime();
            } else {
                bulkEventCount += detail.getEventCount();
                bulkTotalTime += detail.getPollerTime() + detail.getIngestWaitTime() + detail.getIngestTime() + detail.getLoaderWaitTime()
                                + detail.getLoaderTime();
                bulkPollerTime += detail.getPollerTime();
                bulkIngestWaitTime += detail.getIngestWaitTime();
                bulkIngestTime += detail.getIngestTime();
                bulkLoaderWaitTime += detail.getLoaderWaitTime();
                bulkLoaderTime += detail.getLoaderTime();
            }
        }
        int count = details.size();
        if (count != 0) {
            summary.setBulkEventCount(bulkEventCount);
            summary.setLiveEventCount(liveEventCount);
            summary.setLiveAveTime(liveTotalTime / count);
            summary.setLiveAvePollerTime(livePollerTime / count);
            summary.setLiveAveIngestWaitTime(liveIngestWaitTime / count);
            summary.setLiveAveIngestTime(liveIngestTime / count);
            
            summary.setBulkAveTime(bulkTotalTime / count);
            summary.setBulkAvePollerTime(bulkPollerTime / count);
            summary.setBulkAveIngestWaitTime(bulkIngestWaitTime / count);
            summary.setBulkAveIngestTime(bulkIngestTime / count);
            summary.setBulkAveLoaderWaitTime(bulkLoaderWaitTime / count);
            summary.setBulkAveLoaderTime(bulkLoaderTime / count);
        }
        
        if (interval != 0) {
            summary.setLiveEventRate(liveEventCount / interval);
            summary.setBulkEventRate(bulkEventCount / interval);
        }
        
        return summary;
    }
}
