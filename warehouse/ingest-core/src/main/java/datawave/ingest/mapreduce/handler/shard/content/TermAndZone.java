package datawave.ingest.mapreduce.handler.shard.content;

import java.io.Serializable;

public class TermAndZone implements Serializable, Comparable<TermAndZone> {
    private static final long serialVersionUID = -5948574921922307149L;

    public String zone;
    public String term;

    public TermAndZone(String token) {
        int index = token.lastIndexOf(':');
        if (index < 0) {
            throw new IllegalArgumentException("Missing zone in token: " + token);
        }
        this.term = token.substring(0, index);
        this.zone = token.substring(index + 1);
    }

    public TermAndZone(String term, String zone) {
        this.zone = zone;
        this.term = term;
    }

    public String getToken() {
        StringBuilder token = new StringBuilder();
        token.append(term).append(':').append(zone);
        return token.toString();
    }

    @Override
    public String toString() {
        return getToken();
    }

    @Override
    public int hashCode() {
        return this.term.hashCode() + this.zone.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof TermAndZone) {
            return this.term.equals(((TermAndZone) o).term) && this.zone.equals(((TermAndZone) o).zone);
        }
        return false;
    }

    @Override
    public int compareTo(TermAndZone o) {
        int comparison = this.zone.compareTo(o.zone);
        if (comparison == 0) {
            comparison = this.term.compareTo(o.term);
        }
        return comparison;
    }
}
