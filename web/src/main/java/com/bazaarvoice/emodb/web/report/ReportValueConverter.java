package com.bazaarvoice.emodb.web.report;

public interface ReportValueConverter<T extends Comparable<T>> {

    double toDouble(T value);

    T fromDouble(double value);
}
