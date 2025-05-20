package datawave.ingest.mapreduce.handler.edge.define;

import java.util.List;

public class EdgeGroup {

    List<EdgeNode> group1;

    List<EdgeNode> group2;

    public List<EdgeNode> getGroup1() {
        return group1;
    }

    public void setGroup1(List<EdgeNode> group1) {
        this.group1 = group1;
    }

    public List<EdgeNode> getGroup2() {
        return group2;
    }

    public void setGroup2(List<EdgeNode> group2) {
        this.group2 = group2;
    }
}
