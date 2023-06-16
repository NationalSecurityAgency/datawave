package datawave.webservice.query.dashboard;

import java.util.Date;
import java.util.Objects;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;

/**
 * Data object used for the metrics dashboard.
 */
@XmlAccessorType(XmlAccessType.FIELD)
public final class DashboardSummary implements Comparable<DashboardSummary> {

    private final Date dateTime;
    private int upTo3Sec;
    private int upTo10Sec;
    private int upTo60Sec;
    private int moreThan60Sec;
    private int errorCount;
    private int zeroResults;
    private int upTo10KResults;
    private int upTo1MResults;
    private int upToINFResults;
    private int oneTerm;
    private int upTo16Terms;
    private int upTo100Terms;
    private int upTo1000Terms;
    private int upToInfTerms;
    private int resultCount;
    private int queryCount;
    private int selectorCount;

    public DashboardSummary(Date dateTime) {
        this.dateTime = dateTime;
    }

    public void addQuery(long latency, boolean error, int resultCount, int selectorCount) {
        queryCount++;
        this.resultCount += resultCount;
        this.selectorCount += selectorCount;

        if (error) {
            errorCount++;
        } else {

            int elapsed = Long.valueOf(latency).intValue();

            if (elapsed < 3_000) {
                upTo3Sec++;
            } else if (elapsed < 10_000) {
                upTo10Sec++;
            } else if (elapsed < 60_000) {
                upTo60Sec++;
            } else {
                moreThan60Sec++;
            }

            if (resultCount == 0) {
                zeroResults++;
            } else if (resultCount < 10_000) {
                upTo10KResults++;
            } else if (resultCount < 1_000_000) {
                upTo1MResults++;
            } else {
                upToINFResults++;
            }
        }

        // there shouldn't be values of 0, but just in-case
        if (selectorCount <= 1) {
            oneTerm++;
        } else if (selectorCount < 16) {
            upTo16Terms++;
        } else if (selectorCount < 100) {
            upTo100Terms++;
        } else if (selectorCount < 1_000) {
            upTo1000Terms++;
        } else {
            upToInfTerms++;
        }
    }

    public Date getDateTime() {
        return dateTime;
    }

    public int getErrorCount() {
        return errorCount;
    }

    public int getZeroResults() {
        return zeroResults;
    }

    public int getUpTo10KResults() {
        return upTo10KResults;
    }

    public int getUpTo1MResults() {
        return upTo1MResults;
    }

    public int getUpToINFResults() {
        return upToINFResults;
    }

    public int getResultCount() {
        return resultCount;
    }

    public int getQueryCount() {
        return queryCount;
    }

    public int getUpTo3Sec() {
        return upTo3Sec;
    }

    public int getUpTo10Sec() {
        return upTo10Sec;
    }

    public int getUpTo60Sec() {
        return upTo60Sec;
    }

    public int getMoreThan60Sec() {
        return moreThan60Sec;
    }

    public int getSelectorCount() {
        return selectorCount;
    }

    public int getOneTerm() {
        return oneTerm;
    }

    public int getUpTo16Terms() {
        return upTo16Terms;
    }

    public int getUpTo100Terms() {
        return upTo100Terms;
    }

    public int getUpTo1000Terms() {
        return upTo1000Terms;
    }

    public int getUpToInfTerms() {
        return upToInfTerms;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 73 * hash + Objects.hashCode(this.dateTime);
        hash = 73 * hash + this.upTo3Sec;
        hash = 73 * hash + this.upTo10Sec;
        hash = 73 * hash + this.upTo60Sec;
        hash = 73 * hash + this.moreThan60Sec;
        hash = 73 * hash + this.errorCount;
        hash = 73 * hash + this.zeroResults;
        hash = 73 * hash + this.upTo10KResults;
        hash = 73 * hash + this.upTo1MResults;
        hash = 73 * hash + this.upToINFResults;
        hash = 73 * hash + this.oneTerm;
        hash = 73 * hash + this.upTo16Terms;
        hash = 73 * hash + this.upTo100Terms;
        hash = 73 * hash + this.upTo1000Terms;
        hash = 73 * hash + this.upToInfTerms;
        hash = 73 * hash + this.resultCount;
        hash = 73 * hash + this.queryCount;
        hash = 73 * hash + this.selectorCount;
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final DashboardSummary other = (DashboardSummary) obj;
        if (!Objects.equals(this.dateTime, other.dateTime)) {
            return false;
        }
        if (this.upTo3Sec != other.upTo3Sec) {
            return false;
        }
        if (this.upTo10Sec != other.upTo10Sec) {
            return false;
        }
        if (this.upTo60Sec != other.upTo60Sec) {
            return false;
        }
        if (this.moreThan60Sec != other.moreThan60Sec) {
            return false;
        }
        if (this.errorCount != other.errorCount) {
            return false;
        }
        if (this.zeroResults != other.zeroResults) {
            return false;
        }
        if (this.upTo10KResults != other.upTo10KResults) {
            return false;
        }
        if (this.upTo1MResults != other.upTo1MResults) {
            return false;
        }
        if (this.upToINFResults != other.upToINFResults) {
            return false;
        }
        if (this.oneTerm != other.oneTerm) {
            return false;
        }
        if (this.upTo16Terms != other.upTo16Terms) {
            return false;
        }
        if (this.upTo100Terms != other.upTo100Terms) {
            return false;
        }
        if (this.upTo1000Terms != other.upTo1000Terms) {
            return false;
        }
        if (this.upToInfTerms != other.upToInfTerms) {
            return false;
        }
        if (this.resultCount != other.resultCount) {
            return false;
        }
        if (this.queryCount != other.queryCount) {
            return false;
        }

        return this.selectorCount == other.selectorCount;
    }

    @Override
    public int compareTo(DashboardSummary o) {
        return dateTime.compareTo(o.dateTime);
    }

}
