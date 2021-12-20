package com.bazaarvoice.emodb.sor.condition.impl;

import com.bazaarvoice.emodb.sor.condition.AndCondition;
import com.bazaarvoice.emodb.sor.condition.ComparisonCondition;
import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.condition.ConditionVisitor;
import com.bazaarvoice.emodb.sor.condition.Conditions;
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
import com.bazaarvoice.emodb.sor.condition.PartitionCondition;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class PartitionConditionImpl extends AbstractCondition implements PartitionCondition {

    private final int _numPartitions;
    private final Condition _condition;

    public PartitionConditionImpl(int numPartitions, Condition condition) {
        _numPartitions = numPartitions;
        _condition = requireNonNull(condition, "condition");
        checkArgument(numPartitions > 0, "Number of partitions must be at least 1");
        // Validate the condition
        condition.visit(new PartitionConditionValidationVisitor(), null);
    }

    /**
     * Validator for the condition, mostly checks that values are numbers within the partition range.  Some nonsensical
     * values are still permitted since the purpose is to exclude patently invalid conditions but not necessarily
     * meaningless ones.
     */
    private class PartitionConditionValidationVisitor implements ConditionVisitor<Void, Void> {
        @Nullable
        @Override
        public Void visit(ConstantCondition condition, @Nullable Void context) {
            return null;
        }

        @Nullable
        @Override
        public Void visit(EqualCondition condition, @Nullable Void context) {
            if (!(condition.getValue() instanceof Number)) {
                throw new IllegalArgumentException("Partition value must be an integer");
            }
            Number partitionNum = (Number) condition.getValue();
            int partition = partitionNum.intValue();
            if (partitionNum.doubleValue() != partition) {
                throw new IllegalArgumentException("Partition value must be an integer");
            }
            if (partition < 1 || partition > _numPartitions) {
                throw new IllegalArgumentException("Partition must be between 1 and " + _numPartitions);
            }
            return null;
        }

        @Nullable
        @Override
        public Void visit(ComparisonCondition condition, @Nullable Void context) {
            return null;
        }

        @Nullable
        @Override
        public Void visit(InCondition condition, @Nullable Void context) {
            condition.getValues().forEach(v -> Conditions.equal(v).visit(this, null));
            return null;
        }

        @Nullable
        @Override
        public Void visit(IsCondition condition, @Nullable Void context) {
            return null;
        }

        @Nullable
        @Override
        public Void visit(NotCondition condition, @Nullable Void context) {
            return condition.getCondition().visit(this, null);
        }

        @Nullable
        @Override
        public Void visit(AndCondition condition, @Nullable Void context) {
            condition.getConditions().forEach(c -> c.visit(this, null));
            return null;
        }

        @Nullable
        @Override
        public Void visit(OrCondition condition, @Nullable Void context) {
            condition.getConditions().forEach(c -> c.visit(this, null));
            return null;
        }

        @Nullable
        @Override
        public Void visit(IntrinsicCondition condition, @Nullable Void context) {
            throw new IllegalArgumentException("Invalid comparison of partition to intrinsic");
        }

        @Nullable
        @Override
        public Void visit(LikeCondition condition, @Nullable Void context) {
            throw new IllegalArgumentException("Invalid comparison of partition to string");
        }

        @Nullable
        @Override
        public Void visit(ContainsCondition condition, @Nullable Void context) {
            throw new IllegalArgumentException("Invalid comparison of partition to array");
        }

        @Nullable
        @Override
        public Void visit(MapCondition condition, @Nullable Void context) {
            throw new IllegalArgumentException("Invalid comparison of partition to object");
        }

        @Nullable
        @Override
        public Void visit(PartitionCondition condition, @Nullable Void context) {
            throw new IllegalArgumentException("Invalid nested use of partitions");
        }
    }

    @Override
    public <T, V> V visit(ConditionVisitor<T, V> visitor, @Nullable T context) {
        return visitor.visit(this, context);
    }

    @Override
    public void appendTo(Appendable buf) throws IOException {
        buf.append("partition(");
        buf.append(Integer.toString(_numPartitions));
        buf.append(":");
        appendSubCondition(buf, _condition);
        buf.append(")");
    }

    @Override
    public int getNumPartitions() {
        return _numPartitions;
    }

    @Override
    public Condition getCondition() {
        return _condition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PartitionConditionImpl)) {
            return false;
        }
        PartitionConditionImpl that = (PartitionConditionImpl) o;
        return _numPartitions == that._numPartitions &&
                Objects.equals(_condition, that._condition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(_numPartitions, _condition);
    }
}
