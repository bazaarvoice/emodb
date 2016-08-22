package com.bazaarvoice.emodb.sor.delta;

import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.condition.ConstantCondition;
import com.bazaarvoice.emodb.sor.delta.deser.DeltaParser;
import com.bazaarvoice.emodb.sor.delta.deser.JsonTokener;
import com.bazaarvoice.emodb.sor.delta.impl.ConditionalDeltaBuilderImpl;
import com.bazaarvoice.emodb.sor.delta.impl.ConditionalDeltaImpl;
import com.bazaarvoice.emodb.sor.delta.impl.DeleteImpl;
import com.bazaarvoice.emodb.sor.delta.impl.LiteralImpl;
import com.bazaarvoice.emodb.sor.delta.impl.MapDeltaBuilderImpl;
import com.bazaarvoice.emodb.sor.delta.impl.NoopDeltaImpl;
import com.bazaarvoice.emodb.sor.delta.impl.SetDeltaBuilderImpl;

import java.io.Reader;
import java.util.Iterator;

public abstract class Deltas {

    public static Delta fromString(String string) {
        return DeltaParser.parse(string);
    }

    public static Delta fromString(JsonTokener jsonTokener) {
        return DeltaParser.parse(jsonTokener);
    }

    public static Literal literal(Object json) {
        return new LiteralImpl(json);
    }

    public static NoopDelta noop() {
        return NoopDeltaImpl.INSTANCE;
    }

    public static Delete delete() {
        return DeleteImpl.INSTANCE;
    }

    public static MapDeltaBuilder mapBuilder() {
        return new MapDeltaBuilderImpl();
    }

    public static SetDeltaBuilder setBuilder() {
        return new SetDeltaBuilderImpl();
    }

    public static Delta conditional(Condition condition, Delta delta) {
        return conditional(condition, delta, noop());
    }

    public static Delta conditional(Condition condition, Delta delta, Delta otherwise) {
        if (condition instanceof ConstantCondition) {
            return ((ConstantCondition) condition).getValue() ? delta : otherwise;
        }
        return new ConditionalDeltaImpl(condition, delta, otherwise);
    }

    public static ConditionalDeltaBuilder conditionalBuilder() {
        return new ConditionalDeltaBuilderImpl();
    }

    public static Iterator<Delta> fromStream(Reader in) {
        return DeltaParser.parseStream(in);
    }
}
