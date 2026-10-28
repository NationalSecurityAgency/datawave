package datawave.security.authorization;

public class DatawaveUserServiceFactory {
    private DatawaveUserService defaultService;
    private DatawaveUserService testService;
    private boolean useTestService = false;;

    public void setDefaultService(DatawaveUserService defaultService) {
        this.defaultService = defaultService;
    }

    public void setTestService(DatawaveUserService testService) {
        this.testService = testService;
    }

    public void setUseTestService(boolean useTestService) {
        this.useTestService = useTestService;
    }

    public DatawaveUserService getDatawaveUserService() {
        return useTestService ? testService : defaultService;
    }
}
