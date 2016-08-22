package com.bazaarvoice.emodb.auth.permissions;

import com.bazaarvoice.emodb.auth.permissions.matching.AnyPart;
import com.bazaarvoice.emodb.auth.permissions.matching.ConstantPart;
import com.bazaarvoice.emodb.auth.permissions.matching.MatchingPart;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.shiro.authz.Permission;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Similar to {@link org.apache.shiro.authz.permission.WildcardPermission} with the following differences:
 * <ol>
 *     <li>Parts are separated by "|" instead of ":", since ":" occurs commonly in emo table names</li>
 *     <li>Provides basic wildcard support, although the implementation is extensible to provide further wildcard capabilities.</li>
 *     <li>Provides methods for escaping separators and wildcards.</li>
 * </ol>
 */
public class MatchingPermission implements Permission, Serializable {

    private final static String SEPARATOR = "|";
    private final static String UNESCAPED_SEPARATOR_REGEX = "\\|";
    private final static String SEPARATOR_ESCAPE = "\\\\|";
    private final static String ESCAPED_SEPARATOR_REGEX = "\\\\\\|";
    private final static Pattern SEPARATOR_SPLIT_PATTERN = Pattern.compile("(?<!\\\\)\\|");
    private final static String UNESCAPED_WILDCARD_REGEX = "\\*";
    private final static String WILDCARD_ESCAPE = "\\\\\\*";
    private final static String ANY_INDICATOR = "*";

    private final String _permission;
    private List<MatchingPart> _parts;

    public MatchingPermission(String permission) {
        this(permission, true);
    }

    protected MatchingPermission(String permission, boolean initializePermission) {
        _permission = checkNotNull(permission, "permission");
        checkArgument(!"".equals(permission.trim()), "Permission must be a non-null, non-empty string");

        if (initializePermission) {
            initializePermission();
        }
    }

    /**
     * Parses and initializes the permission's parts.  By default this is performed by the constructor.  If a subclass
     * needs to perform its own initialization prior to initializing the permissions then it should call the constructor
     * with the initialization parameter set to false and then call this method when ready.
     */
    protected void initializePermission() {
        List<MatchingPart> parts = Lists.newArrayList();

        for (String partString : split(_permission)) {
            partString = partString.trim();
            checkArgument(!"".equals(partString), "Permission cannot contain empty parts");

            MatchingPart part = toPart(Collections.unmodifiableList(parts), partString);
            parts.add(part);
        }

        _parts = ImmutableList.copyOf(parts);
    }

    public MatchingPermission(String... parts) {
        this(Joiner.on(SEPARATOR).join(parts));
    }

    private Iterable<String> split(String permission) {
        // Use the split pattern to avoid splitting on escaped separators.
        return Arrays.asList(SEPARATOR_SPLIT_PATTERN.split(permission));
    }

    /**
     * Converts a String part into the corresponding {@link MatchingPart}.
     */
    protected MatchingPart toPart(List<MatchingPart> leadingParts, String part) {
        if (ANY_INDICATOR.equals(part)) {
            return getAnyPart();
        }
        // This part is a constant string
        return createConstantPart(part);
    }

    @Override
    public boolean implies(Permission p) {
        if (!(p instanceof MatchingPermission)) {
            return false;
        }

        MatchingPermission other = (MatchingPermission) p;

        int commonLength = Math.min(_parts.size(), other._parts.size());

        for (int i=0; i < commonLength; i++) {
            if (!_parts.get(i).implies(other._parts.get(i), other._parts.subList(0, i))) {
                return false;
            }
        }

        // If this had more parts than the other permission then only pass if all remaining parts are wildcards
        while (commonLength < _parts.size()) {
            if (!_parts.get(commonLength++).impliesAny()) {
                return false;
            }
        }

        // It's possible the other also has more parts, but in this case it's narrower than this permission and
        // hence is still implied by it.

        return true;
    }

    /**
     * Some permissions fall into one of the following categories:
     *
     * <ol>
     *     <li>The permission is intended for validation purposes only, such as for creating a table.</li>
     *     <li>The permission format is deprecated and no new permissions of the format are allowed.</li>
     * </ol>
     *
     * This method returns true if the permission should be assignable to a user/role, false otherwise.
     */
    public boolean isAssignable() {
        for (MatchingPart part : _parts) {
            if (!part.isAssignable()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns a string escaped so it will be interpreted literally by the matcher.  Specifically it converts all
     * '|' and '*' characters to "\|" and "\*" respectively.
     */
    public static String escape(String raw) {
        checkNotNull(raw, "raw");
        String escaped = raw;
        escaped = escaped.replaceAll(UNESCAPED_WILDCARD_REGEX, WILDCARD_ESCAPE);
        escaped = escapeSeparators(escaped);
        return escaped;
    }

    /**
     * Returns a string with only the separators escaped, unlike {@link #escape(String)} which also protects
     * wildcards.  This is useful for subclasses which have their own formatting that needs to be protected from being split.
     */
    public static String escapeSeparators(String raw) {
        return raw.replaceAll(UNESCAPED_SEPARATOR_REGEX, SEPARATOR_ESCAPE);
    }

    /**
     * Returns a string with the modifications made by {@link #escapeSeparators(String)} reversed.
     */
    public static String unescapeSeparators(String escaped) {
        return escaped.replaceAll(ESCAPED_SEPARATOR_REGEX, SEPARATOR);
    }

    protected final List<MatchingPart> getParts() {
        return _parts;
    }

    protected ConstantPart createConstantPart(String value) {
        return new ConstantPart(value);
    }

    protected AnyPart getAnyPart() {
        return AnyPart.instance();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MatchingPermission that = (MatchingPermission) o;

        return _permission.equals(that._permission);
    }

    @Override
    public int hashCode() {
        return _permission.hashCode();
    }

    @Override
    public String toString() {
        return _permission;
    }
}
