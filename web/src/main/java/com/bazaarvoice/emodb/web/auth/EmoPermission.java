package com.bazaarvoice.emodb.web.auth;

import com.bazaarvoice.emodb.auth.permissions.MatchingPermission;
import com.bazaarvoice.emodb.auth.permissions.matching.ConstantPart;
import com.bazaarvoice.emodb.auth.permissions.matching.MatchingPart;
import com.bazaarvoice.emodb.blob.api.BlobStore;
import com.bazaarvoice.emodb.common.json.RisonHelper;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.sor.condition.EqualCondition;
import com.bazaarvoice.emodb.web.auth.matching.BlobTableConditionPart;
import com.bazaarvoice.emodb.web.auth.matching.ConditionPart;
import com.bazaarvoice.emodb.web.auth.matching.CreateTablePart;
import com.bazaarvoice.emodb.web.auth.matching.EmoAnyPart;
import com.bazaarvoice.emodb.web.auth.matching.EmoConstantPart;
import com.bazaarvoice.emodb.web.auth.matching.SorTableConditionPart;

import javax.annotation.Nullable;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

/**
 * Permission used by EmoDB for authorization.  It is identical to {@link MatchingPermission} except that it allows
 * explicit table metadata to be compared against the table metadata for a table.
 * For example consider the following permission.
 *
 * <code>
 *      EmoPermission userPermission = new EmoPermission(dataStore, "sor|update|if({..,\"team\":\"EmoDB\",\"type\":\"review\"})");
 * </code>
 *
 * A permission is implied by this permission only if it references a table which matches on each item from the
 * table template.
 *
 * <code>
 *      EmoPermission checkedPermission = new EmoPermission(dataStore, "sor|update|review:testcustomer");
 *      Map&lt;String, Object&gt; template = dataStore.getTableTemplate("review:testcustomer");
 *
 *      assert userPermission.implies(checkedPermission) == (template.get("team").equals("EmoDB") && template.get("type").equals("review"));
 * </code>
 */
public class EmoPermission extends MatchingPermission {

    // All data store related permissions have this as the first part
    private final static ConstantPart SOR_PART = new EmoConstantPart(Permissions.SOR);
    // All blob store related permissions have this as the first part
    private final static ConstantPart BLOB_PART = new EmoConstantPart(Permissions.BLOB);
    // All role related permissions have this as the first part
    private final static ConstantPart ROLE_PART = new EmoConstantPart(Permissions.ROLE);

    private final DataStore _dataStore;
    private final BlobStore _blobStore;

    private enum PartType {
        CONTEXT,
        ACTION,
        SOR_TABLE,
        BLOB_TABLE,
        NAMED_RESOURCE
    }
    /**
     * General constructor that can handle any permission.
     */
    public EmoPermission(@Nullable DataStore dataStore, @Nullable BlobStore blobStore, String permission) {
        super(permission, false);
        _dataStore = dataStore;
        _blobStore = blobStore;
        initializePermission();
    }

    /**
     * General constructor that can handle any permission which is the sum of the given parts.
     */
    public EmoPermission(@Nullable DataStore dataStore, @Nullable BlobStore blobStore, String... parts) {
        super(parts);
        _dataStore = dataStore;
        _blobStore = blobStore;
    }

    /**
     * Override the logic in the parent to meet the requirements for EmoPermission.
     */
    @Override
    protected MatchingPart toPart(List<MatchingPart> leadingParts, String part) {
        switch (leadingParts.size()) {
            case 0:
                // The first part must either be a constant or a pure wildcard; expressions such as
                // <code>if(or("sor","blob"))</code> are not permitted.
                MatchingPart resolvedPart = createEmoPermissionPart(part, PartType.CONTEXT);
                checkArgument(resolvedPart instanceof ConstantPart || resolvedPart.impliesAny(),
                        "First part must be a constant or pure wildcard");
                return resolvedPart;

            case 1:
                // If the first part was a pure wildcard then there cannot be a second part; expressions such as
                // "*|create_table" are not permitted.
                checkArgument(!MatchingPart.getContext(leadingParts).impliesAny(), "Cannot narrow permission without initial scope");
                return createEmoPermissionPart(part, PartType.ACTION);

            case 2:
                // For sor and blob permissions the third part narrows to a table.  Otherwise it narrows to a
                // named resource such as a queue name or databus subscription.
                PartType partType;
                if (MatchingPart.contextImpliedBy(SOR_PART, leadingParts)) {
                    partType = PartType.SOR_TABLE;
                } else if (MatchingPart.contextImpliedBy(BLOB_PART, leadingParts)) {
                    partType = PartType.BLOB_TABLE;
                } else {
                    partType = PartType.NAMED_RESOURCE;
                }
                return createEmoPermissionPart(part, partType);

            case 3:
                // Only roles support four parts, where the group is the third part and the role ID is the fourth part.
                if (MatchingPart.contextImpliedBy(ROLE_PART, leadingParts)) {
                    return createEmoPermissionPart(part, PartType.NAMED_RESOURCE);
                }
                break;
        }

        throw new IllegalArgumentException(format("Too many parts for EmoPermission in \"%s\" context",
                MatchingPart.getContext(leadingParts)));
    }

    /**
     * Creates a TableIdentificationPart for the third value in a data or blob store permission.
     * @param part String representation of the part
     * @param partType What type of part is being evaluated
     */
    private MatchingPart createEmoPermissionPart(String part, PartType partType) {
        part = unescapeSeparators(part);

        // Superclass guarantees part will be non-empty; no need to validate length prior to evaluating the first character
        switch (part.charAt(0)) {
            case 'c':
                // "createTable" only applies to table resource parts
                if (isTableResource(partType) && part.startsWith("createTable(") && part.endsWith(")")) {
                    String rison = part.substring(12, part.length() - 1);
                    return RisonHelper.fromORison(rison, CreateTablePart.class);
                }
                break;

            case 'i':
                if (part.startsWith("if(") && part.endsWith(")")) {
                    String condition = part.substring(3, part.length() - 1);
                    switch (partType) {
                        case SOR_TABLE:
                            return new SorTableConditionPart(Conditions.fromString(condition), _dataStore);
                        case BLOB_TABLE:
                            return new BlobTableConditionPart(Conditions.fromString(condition), _blobStore);
                        default:
                            return new ConditionPart(Conditions.fromString(condition));
                    }
                }
                break;

            case '*':
                if (part.length() == 1) {
                    // This is a pure wildcard
                    return getAnyPart();
                }
                break;
        }

        // All other strings shortcuts for either a resource name or a wildcard expression for matching the name.
        Condition condition = Conditions.like(part);
        if (condition instanceof EqualCondition) {
            return new EmoConstantPart(part);
        }

        return new ConditionPart(condition);
    }

    private boolean isTableResource(PartType resource) {
        switch (resource) {
            case SOR_TABLE:
            case BLOB_TABLE:
                return true;

            default:
                return false;
        }
    }

    /**
     * Override to return a Constant part that implements {@link com.bazaarvoice.emodb.web.auth.matching.EmoImplier}
     */
    @Override
    protected EmoConstantPart createConstantPart(String value) {
        return new EmoConstantPart(value);
    }

    /**
     * Override to return an Any part that implements {@link com.bazaarvoice.emodb.web.auth.matching.EmoImplier}
     */
    @Override
    protected EmoAnyPart getAnyPart() {
        return EmoAnyPart.instance();
    }
}
