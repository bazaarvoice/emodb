package com.bazaarvoice.emodb.web.privacy;

import com.bazaarvoice.emodb.sor.delta.ConditionalDelta;
import com.bazaarvoice.emodb.sor.delta.Delete;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emodb.sor.delta.DeltaVisitor;
import com.bazaarvoice.emodb.sor.delta.Literal;
import com.bazaarvoice.emodb.sor.delta.MapDelta;
import com.bazaarvoice.emodb.sor.delta.NoopDelta;
import com.bazaarvoice.emodb.sor.delta.SetDelta;
import com.bazaarvoice.emodb.sor.delta.impl.ConditionalDeltaImpl;
import com.bazaarvoice.emodb.sor.delta.impl.LiteralImpl;
import com.bazaarvoice.emodb.sor.delta.impl.MapDeltaImpl;
import com.bazaarvoice.emodb.sor.delta.impl.SetDeltaImpl;

import javax.annotation.Nullable;

import static com.bazaarvoice.emodb.web.privacy.FieldPrivacy.stripHidden;

class StrippingDeltaVisitor implements DeltaVisitor<Void, Delta> {

    @Nullable @Override public Delta visit(final Literal delta, @Nullable final Void context) {
        return new LiteralImpl(stripHidden(delta.getValue()));
    }

    @Nullable @Override public Delta visit(final NoopDelta delta, @Nullable final Void context) {
        return delta;
    }

    @Nullable @Override public Delta visit(final Delete delta, @Nullable final Void context) {
        return delta;
    }

    @Nullable @Override public Delta visit(final MapDelta delta, @Nullable final Void context) {
        return new MapDeltaImpl(
            delta.getRemoveRest(),
            stripHidden(delta.getEntries()),
            delta.getDeleteIfEmpty()
        );
    }

    @Nullable @Override public Delta visit(final SetDelta delta, @Nullable final Void context) {
        return new SetDeltaImpl(
            delta.getRemoveRest(),
            stripHidden(delta.getAddedValues()),
            stripHidden(delta.getRemovedValues()),
            delta.getDeleteIfEmpty()
        );
    }

    @Nullable @Override public Delta visit(final ConditionalDelta delta, @Nullable final Void context) {

        return new ConditionalDeltaImpl(
            delta.getTest().visit(new StrippingConditionVisitor(),null),
            delta.getThen().visit(new StrippingDeltaVisitor(),null),
            delta.getElse().visit(new StrippingDeltaVisitor(),null)
        );
    }
}
