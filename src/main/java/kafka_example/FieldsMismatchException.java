package kafka_example;

public class FieldsMismatchException extends Exception {

    public FieldsMismatchException() {
        super();
    }

    public FieldsMismatchException(String message) {
        super(message);
    }

    public FieldsMismatchException(String message, Throwable cause) {
        super(message, cause);
    }

    public FieldsMismatchException(Throwable cause) {
        super(cause);
    }

}
