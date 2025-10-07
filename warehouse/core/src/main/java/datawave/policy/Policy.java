package datawave.policy;

public interface Policy<T> {

    void validate(T arg) throws Policy.Exception;

    @SuppressWarnings("serial")
    class Exception extends java.lang.Exception {

        public Exception() {
            super();
        }

        public Exception(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
            super(message, cause, enableSuppression, writableStackTrace);
        }

        public Exception(String message, Throwable cause) {
            super(message, cause);
        }

        public Exception(String message) {
            super(message);
        }

        public Exception(Throwable cause) {
            super(cause);
        }
    }

    class NoOp implements Policy<Object> {

        @Override
        public void validate(Object arg) throws Exception {
            // noop
        }

    }

}
