package datawave.ingest.mapreduce.handler.edge.define;

public class EdgeNode {

    String selector;

    String realm;

    String relationship;

    String collection;

    public String getSelector() {
        return selector;
    }

    public void setSelector(String selector) {
        this.selector = selector;
    }

    public String getRealm() {
        return realm;
    }

    public void setRealm(String realm) {
        this.realm = realm;
    }

    public String getRelationship() {
        return relationship;
    }

    public void setRelationship(String relationship) {
        this.relationship = relationship;
    }

    public String getCollection() {
        return collection;
    }

    public void setCollection(String collection) {

        this.collection = collection;

    }
}
