package com.bazaarvoice.emodb.sor.db;

import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emodb.sor.delta.DeltaVisitor;
import com.bazaarvoice.emodb.sor.delta.deser.DeltaParser;
import com.bazaarvoice.emodb.sor.delta.deser.JsonTokener;
import com.bazaarvoice.emodb.sor.delta.impl.AbstractDelta;

import javax.annotation.Nullable;
import java.io.IOException;

import static java.util.Objects.requireNonNull;

/**
 * Delta implementation which takes an JSON token stream amd lazily deserializes it if needed.  There are numerous
 * circumstances where a delta is read but never used, such as if the delta is behind a compaction record but has
 * not yet been deleted.
 *
 * To avoid deserializing the delta to determine if it is constant the instance takes as a parameter whether the
 * deserialized delta is constant.  It is up to the caller to ensure that this is accurate, since there are no checks
 * for whether calling {@link Delta#isConstant()} on the deserialized delta matches the provided value.
 */
public class LazyDelta extends AbstractDelta {

    private volatile JsonTokener _tokener;
    private volatile Delta _delta;
    private final boolean _constant;

    public LazyDelta(JsonTokener tokener, boolean constant) {
        _tokener = requireNonNull(tokener, "tokener");
        _constant = constant;
    }

    private Delta getDelta() {
        if (_delta == null) {
            synchronized (this) {
                if (_delta == null) {
                    _delta = DeltaParser.parse(_tokener);
                    _tokener = null;
                }
            }
        }
        return _delta;
    }

    @Override
    public <T, V> V visit(DeltaVisitor<T, V> visitor, @Nullable T context) {
        return getDelta().visit(visitor, context);
    }

    @Override
    public boolean isConstant() {
        return _constant;
    }

    @Override
    public void appendTo(Appendable buf) throws IOException {
        getDelta().appendTo(buf);
    }
}
