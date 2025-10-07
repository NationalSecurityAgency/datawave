package datawave.query.discovery;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.collect.Sets;

import datawave.marking.MarkingFunctions;

public class TermInfoAggregation implements Function<Collection<TermInfo>,DiscoveredThing> {

    private static final Logger log = Logger.getLogger(TermInfoAggregation.class);
    private Set<ColumnVisibility> columnVisibilities = Sets.newHashSet();
    private final boolean separateCountsByColumnVisibility;
    private boolean showReferenceCountInsteadOfTermCount = false;
    private boolean reverseIndex = false;

    public TermInfoAggregation() {
        this.separateCountsByColumnVisibility = false;

    }

    public TermInfoAggregation(boolean separateCountsByColumnVisibility) {
        this.separateCountsByColumnVisibility = separateCountsByColumnVisibility;
    }

    public TermInfoAggregation(boolean separateCountsByColVis, boolean showReferenceCount) {
        this.separateCountsByColumnVisibility = separateCountsByColVis;
        this.showReferenceCountInsteadOfTermCount = showReferenceCount;
    }

    public TermInfoAggregation(boolean separateCountsByColVis, boolean showReferenceCount, boolean reverseIndex) {
        this.separateCountsByColumnVisibility = separateCountsByColVis;
        this.showReferenceCountInsteadOfTermCount = showReferenceCount;
        this.reverseIndex = reverseIndex;
    }

    /*
     * TermInfos for any given data type should have the following in common: - term - date - data type
     *
     * Therefore, we need to aggregate: - count - columnVisibilities/markings
     */
    public DiscoveredThing apply(Collection<TermInfo> from) {
        if (from.isEmpty()) {
            return null;
        } else {
            TermInfo info = from.iterator().next();
            final String term, field = info.fieldName, type = info.datatype, date = info.date;
            if (reverseIndex) {
                term = new StringBuilder().append(info.fieldValue).reverse().toString();
            } else {
                term = info.fieldValue;
            }
            long count = 0L;
            Map<String,Long> counts = new HashMap<>();
            long termCount = 0L;
            long referenceCount = 0L;
            long chosenCount = 0L;
            for (TermInfo ti : from) {
                termCount = ti.count;
                referenceCount = ti.getListSize();

                chosenCount = showReferenceCountInsteadOfTermCount ? referenceCount : termCount;

                try {
                    MarkingFunctions.Factory.createMarkingFunctions().translateFromColumnVisibility(ti.vis); // just to test parsing
                    columnVisibilities.add(ti.vis);

                    // Keep track of counts for individual vis
                    if (separateCountsByColumnVisibility) {
                        Long cnt = 0L;
                        String vis = new String(ti.vis.flatten());
                        if (counts.containsKey(vis)) {
                            cnt = counts.get(vis);
                            cnt += chosenCount;
                            counts.remove(vis);
                        } else {
                            cnt = chosenCount;
                        }
                        counts.put(vis, cnt);
                    }

                } catch (Exception e1) {
                    if (log.isTraceEnabled())
                        log.trace(e1);
                    continue;
                }
                count += chosenCount;
            }
            // adjust it so that if we have zero or fewer records
            // do nothing
            if (count <= 0) {
                if (log.isTraceEnabled())
                    log.trace("Did not aggregate any counts for [" + term + "][" + field + "][" + type + "][" + date + "]. Returning null.");
                return null;
            } else {
                ColumnVisibility columnVisibility = null;
                try {

                    columnVisibility = MarkingFunctions.Factory.createMarkingFunctions().combine(columnVisibilities);

                } catch (Exception e) {
                    log.warn("Invalid columnvisibility after combining!", e);
                    return null;
                }

                MapWritable countsByVis = new MapWritable();
                for (Entry<String,Long> entry : counts.entrySet()) {
                    countsByVis.put(new Text(entry.getKey()), new VLongWritable(entry.getValue()));
                }

                return new DiscoveredThing(term, field, type, date, new String(columnVisibility.flatten()), count, countsByVis);
            }
        }
    }
}
