package datawave.webservice.query.limit;

public interface QueryLogicsUpdateListener {

    void forCreate(String queryLogic);

    void forDelete(String queryLogic);
}
