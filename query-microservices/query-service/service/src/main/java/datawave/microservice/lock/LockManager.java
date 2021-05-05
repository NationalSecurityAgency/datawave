package datawave.microservice.lock;

public interface LockManager {

    Semaphore getSemaphore(String name, int permits) throws Exception;
}
