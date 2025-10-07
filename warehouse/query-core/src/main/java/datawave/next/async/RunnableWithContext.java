package datawave.next.async;

public interface RunnableWithContext extends Runnable {

    void setContext(String context);

    String getContext();
}
