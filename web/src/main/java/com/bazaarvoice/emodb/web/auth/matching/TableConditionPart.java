package com.bazaarvoice.emodb.web.auth.matching;

import com.bazaarvoice.emodb.auth.permissions.matching.ConstantPart;
import com.bazaarvoice.emodb.auth.permissions.matching.Implier;
import com.bazaarvoice.emodb.auth.permissions.matching.MatchingPart;
import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.condition.AndCondition;
import com.bazaarvoice.emodb.sor.condition.ComparisonCondition;
import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.condition.ConditionVisitor;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.sor.condition.ConstantCondition;
import com.bazaarvoice.emodb.sor.condition.ContainsCondition;
import com.bazaarvoice.emodb.sor.condition.PartitionCondition;
import com.bazaarvoice.emodb.sor.condition.EqualCondition;
import com.bazaarvoice.emodb.sor.condition.InCondition;
import com.bazaarvoice.emodb.sor.condition.IntrinsicCondition;
import com.bazaarvoice.emodb.sor.condition.IsCondition;
import com.bazaarvoice.emodb.sor.condition.LikeCondition;
import com.bazaarvoice.emodb.sor.condition.MapCondition;
import com.bazaarvoice.emodb.sor.condition.NotCondition;
import com.bazaarvoice.emodb.sor.condition.OrCondition;
import com.bazaarvoice.emodb.sor.condition.eval.ConditionEvaluator;
import com.bazaarvoice.emodb.sor.condition.eval.SubsetEvaluator;
import com.bazaarvoice.emodb.web.auth.Permissions;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Base class for evaluating whether a table matches a permission by using conditions.  This is the same syntax and
 * approach used by the databus for matching tables to subscriptions.
 */
abstract public class TableConditionPart extends EmoMatchingPart {

    /**
     * SoR actions generally fall into two categories: actions that pertain to the table <em>metadata</em>
     * (e.g.; create, drop) and actions that pertain to the table <em>content</em> (e.g.; read, update, size).
     * Actions in the former category should be evaluated using the table's master placement while those in the latter
     * should use the local availability placement, which could be a table or facade.
     *
     * Note:
     *
     * <ol>
     *     <li>Create table uses it's own distinct matching part, {@link CreateTablePart}, so it doesn't need to
     *         be included in metadata condition.</li>
     *     <li>Updates to a facade use different APIs than to a table, so in the scenario where:
     *         <ol>
     *             <li>A user has permission to update tables in placement "content_us" only,</li>
     *             <li>Table T is mastered in placement "content_eu", and</li>
     *             <li>A facade for T exists in "content_us"</li>
     *         </ol>
     *         If the user attempts to update a record in T from "content_us" the permission check won't fail but
     *         since the local representation is a facade the data store will reject the update request.  The
     *         end result is the same but this approach gives the user more meaningful feedback.
     *     </li>
     * </ol>
     */
    private final static ConditionPart USE_MASTER_PLACEMENT_ACTION = new ConditionPart(
            Conditions.in(Permissions.SET_TABLE_ATTRIBUTES, Permissions.DROP_TABLE));

    private final static ConditionVisitor<Void, Boolean> _requiresMetadataVisitor = new ValidationVisitor();
    private final static String UNUSED = "unused";

    private final Condition _condition;
    private final boolean _requiresTableMetadata;

    public TableConditionPart(Condition condition) {
        _condition = checkNotNull(condition, "condition");
        _requiresTableMetadata = condition.visit(_requiresMetadataVisitor, null);
    }

    @Nullable
    abstract protected PlacementAndAttributes getPlacementAndAttributesForTable(String table, boolean useOptionsPlacement);

    public Condition getCondition() {
        return _condition;
    }

    @Override
    protected boolean impliedBy(Implier implier, List<MatchingPart> leadingParts) {
        return ((EmoImplier) implier).impliesTableCondition(this, leadingParts);
    }

    @Override
    public boolean impliesConstant(ConstantPart part, List<MatchingPart> leadingParts) {
        // The constant is evaluated as the table name
        String tableName = part.getValue();
        if (_requiresTableMetadata) {
            // Load the table metadata for evaluation against the condition
            boolean useMasterPlacement = actionImpliedBy(USE_MASTER_PLACEMENT_ACTION, leadingParts);

            PlacementAndAttributes metadata = getPlacementAndAttributesForTable(tableName, useMasterPlacement);
            if (metadata == null) {
                // Table metadata could not be accessed by the current server
                return false;
            }

            return evaluate(new AuthorizationIntrinsics(tableName, metadata.placement), metadata.attributes);
        } else {
            // The condition doesn't require any data from the table itself.  As an optimization don't load it
            // and use dummy values (which won't be factored into the condition's evaluation anyway).
            return evaluate(new AuthorizationIntrinsics(tableName, UNUSED), ImmutableMap.<String,Object>of());
        }
    }

    @Override
    public boolean impliesCreateTable(CreateTablePart part, List<MatchingPart> leadingParts) {
        return evaluate(new AuthorizationIntrinsics(part.getName(), part.getPlacement()), part.getAttributes());
    }

    private boolean evaluate(AuthorizationIntrinsics intrinsics, Map<String, ?> attributes) {
        return ConditionEvaluator.eval(_condition, attributes, intrinsics);
    }

    @Override
    public boolean impliesAny() {
        // Although there are an infinite number of conditions which satisfy all inputs we only check for the
        // most basic "alwaysTrue()" condition.
        return _condition.equals(Conditions.alwaysTrue());
    }

    @Override
    public boolean impliedPartExists() {
        // Although there are an infinite number of conditions which deny all inputs we only check for the
        // most basic "not alwaysFalse()" condition.
        return !_condition.equals(Conditions.alwaysFalse());
    }

    @Override
    public boolean impliesTableCondition(TableConditionPart part, List<MatchingPart> leadingParts) {
        // This condition implies the other condition only if it is a subset of this condition.
        return SubsetEvaluator.isSubset(part.getCondition(), _condition);
    }

    @Override
    public boolean isAssignable() {
        return true;
    }

    protected final static class PlacementAndAttributes {
        private final String placement;
        private final Map<String, ?> attributes;

        public PlacementAndAttributes(String placement, Map<String, ?> attributes) {
            this.placement = placement;
            this.attributes = attributes;
        }
    }

    /**
     * This visitor serves two purposes:
     *
     * <ol>
     *     <li>It validates that only supported conditions are contained in the expression.</li>
     *     <li>It determines whether the condition in this part requires any metadata which must be loaded for the
     *         table.  Namely, if any condition uses the table's placement or attributes then this visitor will
     *         return true.</li>
     * </ol>
     */
    private final static class ValidationVisitor implements ConditionVisitor<Void, Boolean> {
        @Nullable
        @Override
        public Boolean visit(IntrinsicCondition condition, @Nullable Void context) {
            boolean requiresMetadata;

            switch (condition.getName()) {
                case Intrinsic.TABLE:
                case Intrinsic.ID:
                    // Table name and document ID don't require metadata; check the contained condition
                    requiresMetadata = condition.getCondition().visit(this, null);
                    break;

                case Intrinsic.PLACEMENT:
                    // Placement always requires metadata, but still validate the contained condition
                    condition.getCondition().visit(this, null);
                    requiresMetadata = true;
                    break;

                default:
                    throw new IllegalArgumentException("Intrinsic not supported: " + condition.getName());
            }

            return requiresMetadata;
        }

        @Nullable
        @Override
        public Boolean visit(NotCondition condition, @Nullable Void context) {
            // Check the referenced condition
            return condition.getCondition().visit(this, null);
        }

        @Nullable
        @Override
        public Boolean visit(AndCondition condition, @Nullable Void context) {
            return visitAll(condition.getConditions());
        }

        @Nullable
        @Override
        public Boolean visit(OrCondition condition, @Nullable Void context) {
            return visitAll(condition.getConditions());
        }

        @Nullable
        @Override
        public Boolean visit(MapCondition condition, @Nullable Void context) {
            // A top-level map condition is used to test the table's attributes.  Therefore, if the part's condition
            // includes a map condition then the table attributes must be loaded for comparison.  Still validate
            // all contained conditions.
            visitAll(condition.getEntries().values());
            return true;
        }

        private boolean visitAll(Collection<Condition> conditions) {
            // Check each contained condition.
            boolean requiresMetadata = false;
            for (Condition condition : conditions) {
                if (condition.visit(this, null)) {
                    requiresMetadata = true;
                }
            }
            return requiresMetadata;
        }

        @Nullable
        @Override
        public Boolean visit(PartitionCondition condition, @Nullable Void context) {
            return condition.getCondition().visit(this, null);
        }

        // All remaining conditions are verifiable from the immediate context and do not require table metadata.

        @Nullable
        @Override
        public Boolean visit(ConstantCondition condition, @Nullable Void context) {
            return false;
        }

        @Nullable
        @Override
        public Boolean visit(EqualCondition condition, @Nullable Void context) {
            return false;
        }

        @Nullable
        @Override
        public Boolean visit(InCondition condition, @Nullable Void context) {
            return false;
        }

        @Nullable
        @Override
        public Boolean visit(LikeCondition condition, @Nullable Void context) {
            return false;
        }

        @Nullable
        @Override
        public Boolean visit(IsCondition condition, @Nullable Void context) {
            return false;
        }

        @Nullable
        @Override
        public Boolean visit(ComparisonCondition condition, @Nullable Void context) {
            return false;
        }

        @Nullable
        @Override
        public Boolean visit(ContainsCondition condition, @Nullable Void context) {
            return false;
        }
    };
}
