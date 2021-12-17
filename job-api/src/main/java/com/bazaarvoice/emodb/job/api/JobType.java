package com.bazaarvoice.emodb.job.api;

import static java.util.Objects.requireNonNull;

/**
 * Base class which defines a job type.
 * @
 * @param <Q> The type used to generate job requests
 * @param <R> The type returned by completed jobs
 */
abstract public class JobType<Q, R> {

    private final String _name;
    private final Class<Q> _requestType;
    private final Class<R> _resultType;

    protected JobType(String name, Class<Q> requestType, Class<R> resultType) {
        _name = requireNonNull(name, "name");
        _requestType = requireNonNull(requestType, "requestType");
        _resultType = requireNonNull(resultType, "resultType");
    }

    public String getName() {
        return _name;
    }

    public Class<Q> getRequestType() {
        return _requestType;
    }

    public Class<R> getResultType() {
        return _resultType;
    }

    @Override
    public String toString() {
        return _name;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }

        if (!(other instanceof JobType)) {
            return false;
        }

        JobType t = (JobType) other;
        return _name.equals(t._name) &&
                _requestType.equals(t._requestType) &&
                _resultType.equals(t._resultType);
    }

    @Override
    public int hashCode() {
        return _name.hashCode();
    }
}
