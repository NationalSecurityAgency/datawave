package datawave.webservice.config;

public class InMemoryAccumuloConnectionFactoryProperties {
    private String userName = "root";
    private String authorizations;

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getAuthorizations() {
        return authorizations;
    }

    public void setAuthorizations(String authorizations) {
        this.authorizations = authorizations;
    }
}
