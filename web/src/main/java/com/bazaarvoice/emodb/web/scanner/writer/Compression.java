package com.bazaarvoice.emodb.web.scanner.writer;

public enum Compression {
    NONE(""),
    GZIP(".gz"),
    SNAPPY(".snappy");

    private final String _extension;

    private Compression(String extension) {
        _extension = extension;
    }

    public String getExtension() {
        return _extension;
    }
}
