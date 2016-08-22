package com.bazaarvoice.emodb.sor.db.astyanax;

public class ThriftFramedTransportSizeException extends RuntimeException {
    public ThriftFramedTransportSizeException(String message, Throwable cause) {
        super(message, cause);
    }

    public ThriftFramedTransportSizeException(Throwable cause) {
        super("Thrift message is too large", cause);
    }
}
