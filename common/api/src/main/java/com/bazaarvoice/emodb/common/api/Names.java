package com.bazaarvoice.emodb.common.api;

import java.util.BitSet;

public abstract class Names {

    /** Prevent instantiation. */
    private Names() {}

    // Exclude whitespace, control chars, non-ascii, upper-case, most punctuation.
    private static final BitSet TABLE_NAME_ALLOWED = anyOf(
            anyCharInRange('a', 'z'),
            anyCharInRange('0', '9'),
            anyCharInString("-.:_"));

    // Exclude whitespace, control chars, non-ascii, most punctuation.
    private static final BitSet ROLE_NAME_ALLOWED = anyOf(
            anyCharInRange('a', 'z'),
            anyCharInRange('A', 'Z'),
            anyCharInRange('0', '9'),
            anyCharInString("-.:_"));

    public static BitSet anyOf(BitSet... bitSets) {
        BitSet combined = new BitSet();
        for (BitSet bitSet : bitSets) {
            combined.or(bitSet);
        }
        return combined;
    }

    public static BitSet anyCharInRange(char from, char to) {
        BitSet bitSet = new BitSet();
        bitSet.set(from, to + 1);
        return bitSet;
    }

    public static BitSet anyCharInString(String source) {
        BitSet bitSet = new BitSet();
        source.chars().forEach(bitSet::set);
        return bitSet;
    }

    /**
     * Table names must be lowercase ASCII strings. between 1 and 255 characters in length.  Whitespace, ISO control
     * characters and certain punctuation characters that aren't generally allowed in file names or in elasticsearch
     * index names are excluded (elasticsearch appears to allow: !$%&()+-.:;=@[]^_`{}~).  Table names may not begin
     * with a single underscore to allow URL space for extensions such as "/_table/...".  Table names may not look
     * like relative paths, ie. "." or "..".
     */
    public static boolean isLegalTableName(String table) {
        return table != null &&
                table.length() > 0 && table.length() <= 255 &&
                !(table.charAt(0) == '_' && !table.startsWith("__")) &&
                !(table.charAt(0) == '.' && (".".equals(table) || "..".equals(table))) &&
                table.chars().allMatch(TABLE_NAME_ALLOWED::get);
    }

    /**
     * Role names mostly follow the same conventions as table names except, since they are only used internally, are
     * slightly more permissive, such as allowing capital letters.
     */
    public static boolean isLegalRoleName(String role) {
        return role != null &&
                role.length() > 0 && role.length() <= 255 &&
                role.chars().allMatch(ROLE_NAME_ALLOWED::get);
    }

    /**
     * Role group names mostly follow the same conventions as role names.  The only difference is that the role group
     * "_" is a reserved null-substitution for the absence of a group.
     */
    public static boolean isLegalRoleGroupName(String group) {
        return isLegalRoleName(group) &&
                !"_".equals(group);
    }
}
