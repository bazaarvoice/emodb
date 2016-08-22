package com.bazaarvoice.emodb.databus.core;

import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.condition.AndCondition;
import com.bazaarvoice.emodb.sor.condition.ComparisonCondition;
import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.condition.ConditionVisitor;
import com.bazaarvoice.emodb.sor.condition.ConstantCondition;
import com.bazaarvoice.emodb.sor.condition.ContainsCondition;
import com.bazaarvoice.emodb.sor.condition.EqualCondition;
import com.bazaarvoice.emodb.sor.condition.InCondition;
import com.bazaarvoice.emodb.sor.condition.IntrinsicCondition;
import com.bazaarvoice.emodb.sor.condition.IsCondition;
import com.bazaarvoice.emodb.sor.condition.LikeCondition;
import com.bazaarvoice.emodb.sor.condition.MapCondition;
import com.bazaarvoice.emodb.sor.condition.NotCondition;
import com.bazaarvoice.emodb.sor.condition.OrCondition;

import javax.annotation.Nullable;
import java.util.Collection;

class TableFilterValidator implements ConditionVisitor<Void, Void> {

    /**
     * @throws IllegalArgumentException if the condition performs checks that are not allowed against table metadata.
     */
    static void checkAllowed(Condition condition) {
        condition.visit(new TableFilterValidator(), null);
    }

    private TableFilterValidator() {}

    public Void visit(ConstantCondition condition, Void context) {
        return null;
    }

    public Void visit(EqualCondition condition, Void context) {
        return null;
    }

    public Void visit(InCondition condition, @Nullable Void context) {
        return null;
    }

    public Void visit(ContainsCondition condition, @Nullable Void context) {
        return null;
    }

    public Void visit(LikeCondition condition, @Nullable Void context) {
        return null;
    }

    public Void visit(IntrinsicCondition condition, Void context) {
        // The only supported intrinsic is "~table" and "~placement".  None of the others can be evaluated from just table metadata.
        // This check should match the implementation of TableFilterIntrinsics.java.
        if (!Intrinsic.TABLE.equals(condition.getName()) && !Intrinsic.PLACEMENT.equals(condition.getName())) {
            throw new IllegalArgumentException("Intrinsic '" + condition.getName() + "' is not supported within table filters.");
        }
        condition.getCondition().visit(this, null);
        return null;
    }

    public Void visit(IsCondition condition, Void context) {
        return null;
    }

    public Void visit(ComparisonCondition condition, Void context) {
        return null;
    }

    public Void visit(NotCondition condition, Void context) {
        return condition.getCondition().visit(this, null);
    }

    public Void visit(AndCondition condition, Void context) {
        return visitAll(condition.getConditions());
    }

    public Void visit(OrCondition condition, Void context) {
        return visitAll(condition.getConditions());
    }

    public Void visit(MapCondition condition, Void context) {
        return visitAll(condition.getEntries().values());
    }
    
    private Void visitAll(Collection<Condition> conditions) {
        for (Condition condition : conditions) {
            condition.visit(this, null);
        }
        return null;
    }
}
