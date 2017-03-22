package com.bazaarvoice.emodb.web.auth;

import com.bazaarvoice.emodb.auth.role.RoleIdentifier;
import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.web.auth.resource.AnyResource;
import com.bazaarvoice.emodb.web.auth.resource.ConditionResource;
import com.bazaarvoice.emodb.web.auth.resource.CreateTableResource;
import com.bazaarvoice.emodb.web.auth.resource.NamedResource;
import com.bazaarvoice.emodb.web.auth.resource.VerifiableResource;
import com.google.common.base.Objects;

import javax.annotation.Nullable;

import static com.bazaarvoice.emodb.auth.permissions.MatchingPermission.escapeSeparators;
import static java.lang.String.format;

public class Permissions {

    // Resources
    public final static String SOR = "sor";
    public final static String FACADE = "facade";
    public final static String BLOB = "blob";
    public final static String QUEUE = "queue";
    public final static String DATABUS = "databus";
    public final static String ROLE = "role";
    public final static String API_KEY = "apikey";
    public final static String SYSTEM = "system";

    // Actions
    public final static String READ = "read";
    public final static String CREATE = "create";
    public final static String UPDATE = "update";
    public final static String DELETE = "delete";
    public final static String CREATE_TABLE = "create_table";
    public final static String CREATE_FACADE = "create_facade";
    public final static String SET_TABLE_ATTRIBUTES = "set_table_attributes";
    public final static String DROP_TABLE = "drop_table";
    public final static String DROP_FACADE = "drop_facade";
    public final static String COMPACT = "compact";
    public final static String POST = "post";
    public final static String POLL = "poll";
    public final static String ASSUME_OWNERSHIP = "assume_ownership";
    public final static String GET_STATUS = "get_status";
    public final static String SUBSCRIBE = "subscribe";
    public final static String UNSUBSCRIBE = "unsubscribe";
    public final static String INJECT = "inject";
    public final static String PURGE = "purge";
    public final static String REPLICATE_DATABUS = "replicate_databus";
    public final static String RAW_DATABUS = "raw_databus";
    public final static String GRANT = "grant";
    public final static String CREATE_EXACT = "create_exact";
    public final static String VIEW_BY_KEY = "view_by_key";

    // Common resource values
    public final static AnyResource ALL = new AnyResource();

    /**
     * No resources can begin with '_', and although "__" is a permitted prefix it should be reserved for EmoDB
     * system tables.  This pattern does not assess legal table names, only ensures that those beginning
     * with "__" are restricted to privileged users.
     */
    public final static ConditionResource NON_SYSTEM_RESOURCE = new ConditionResource(
            Conditions.not(
                    Conditions.like("__*")));

    /**
     * The following condition expands on NON_SYSTEM_RESOURCE to specifically be evaluated for tables.
     * In addition to restricting table names starting with "__" it also restricts placements ending with
     * ":sys", such as "app_global:sys".
     */
    public final static ConditionResource NON_SYSTEM_TABLE = new ConditionResource(
            Conditions.not(
                    Conditions.or(
                            Conditions.intrinsic(Intrinsic.TABLE, Conditions.like("__*")),
                            Conditions.intrinsic(Intrinsic.PLACEMENT, Conditions.like("*:sys")))));

    /**
     * NON_SYSTEM_TABLE along with excluding PII table/placements.
     */
    public final static ConditionResource NON_SYSTEM_NON_PII_TABLE = new ConditionResource(
            Conditions.not(
                    Conditions.or(
                            Conditions.intrinsic(Intrinsic.TABLE, Conditions.like("__*")),
                            Conditions.intrinsic(Intrinsic.PLACEMENT, Conditions.like("*:sys")),
                            Conditions.intrinsic(Intrinsic.TABLE, Conditions.like("pii:*")),
                            Conditions.intrinsic(Intrinsic.PLACEMENT, Conditions.like("*:pii")))));

    public static String unlimited() {
        return ALL.toString();
    }

    public static String readSorTable(VerifiableResource table) {
        return format("%s|%s|%s", SOR, READ, escapeSeparators(table.toString()));
    }

    public static String updateSorTable(VerifiableResource table) {
        return format("%s|%s|%s", SOR, UPDATE, escapeSeparators(table.toString()));
    }

    public static String createSorTable(VerifiableResource table) {
        return format("%s|%s|%s", SOR, CREATE_TABLE, escapeSeparators(table.toString()));
    }

    public static String createSorTable(CreateTableResource table) {
        return format("%s|%s|%s", SOR, CREATE_TABLE, escapeSeparators(table.toString()));
    }

    public static String setSorTableAttributes(VerifiableResource table) {
        return format("%s|%s|%s", SOR, SET_TABLE_ATTRIBUTES, escapeSeparators(table.toString()));
    }

    public static String dropSorTable(VerifiableResource table) {
        return format("%s|%s|%s", SOR, DROP_TABLE, escapeSeparators(table.toString()));
    }

    public static String compactSorTable(VerifiableResource table) {
        return format("%s|%s|%s", SOR, COMPACT, escapeSeparators(table.toString()));
    }

    public static String purgeSorTable(VerifiableResource table) {
        return format("%s|%s|%s", SOR, PURGE, escapeSeparators(table.toString()));
    }

    public static String unlimitedSorTable(VerifiableResource table) {
        return format("%s|%s|%s", SOR, ALL, escapeSeparators(table.toString()));
    }

    public static String readBlobTable(VerifiableResource table) {
        return format("%s|%s|%s", BLOB, READ, escapeSeparators(table.toString()));
    }

    public static String updateBlobTable(VerifiableResource table) {
        return format("%s|%s|%s", BLOB, UPDATE, escapeSeparators(table.toString()));
    }

    public static String createBlobTable(VerifiableResource table) {
        return format("%s|%s|%s", BLOB, CREATE_TABLE, escapeSeparators(table.toString()));
    }

    public static String createBlobTable(CreateTableResource table) {
        return format("%s|%s|%s", BLOB, CREATE_TABLE, escapeSeparators(table.toString()));
    }

    public static String setBlobTableAttributes(VerifiableResource table) {
        return format("%s|%s|%s", BLOB, SET_TABLE_ATTRIBUTES, escapeSeparators(table.toString()));
    }

    public static String dropBlobTable(VerifiableResource table) {
        return format("%s|%s|%s", BLOB, DROP_TABLE, escapeSeparators(table.toString()));
    }

    public static String compactBlobTable(VerifiableResource table) {
        return format("%s|%s|%s", BLOB, COMPACT, escapeSeparators(table.toString()));
    }

    public static String purgeBlobTable(VerifiableResource table) {
        return format("%s|%s|%s", BLOB, PURGE, escapeSeparators(table.toString()));
    }

    public static String unlimitedBlobTable(VerifiableResource table) {
        return format("%s|%s|%s", BLOB, ALL, escapeSeparators(table.toString()));
    }

    public static String readFacade(VerifiableResource facade) {
        return format("%s|%s|%s", FACADE, READ, escapeSeparators(facade.toString()));
    }

    public static String updateFacade(VerifiableResource facade) {
        return format("%s|%s|%s", FACADE, UPDATE, escapeSeparators(facade.toString()));
    }

    public static String createFacade(VerifiableResource facade) {
        return format("%s|%s|%s", FACADE, CREATE_FACADE, escapeSeparators(facade.toString()));
    }

    public static String dropFacade(VerifiableResource facade) {
        return format("%s|%s|%s", FACADE, DROP_FACADE, escapeSeparators(facade.toString()));
    }

    public static String unlimitedFacade(VerifiableResource facade) {
        return format("%s|%s|%s", FACADE, ALL, escapeSeparators(facade.toString()));
    }

    public static String postQueue(VerifiableResource queue) {
        return format("%s|%s|%s", QUEUE, POST, escapeSeparators(queue.toString()));
    }

    public static String pollQueue(VerifiableResource queue) {
        return format("%s|%s|%s", QUEUE, POLL, escapeSeparators(queue.toString()));
    }

    public static String getQueueStatus(VerifiableResource queue) {
        return format("%s|%s|%s", QUEUE, GET_STATUS, escapeSeparators(queue.toString()));
    }

    public static String unlimitedQueue(VerifiableResource queue) {
        return format("%s|%s|%s", QUEUE, ALL, escapeSeparators(queue.toString()));
    }

    public static String subscribeDatabus(VerifiableResource subscription) {
        return format("%s|%s|%s", DATABUS, SUBSCRIBE, escapeSeparators(subscription.toString()));
    }

    public static String unsubscribeDatabus(VerifiableResource subscription) {
        return format("%s|%s|%s", DATABUS, UNSUBSCRIBE, escapeSeparators(subscription.toString()));
    }

    public static String getDatabusStatus(VerifiableResource subscription) {
        return format("%s|%s|%s", DATABUS, GET_STATUS, escapeSeparators(subscription.toString()));
    }

    public static String pollDatabus(VerifiableResource subscription) {
        return format("%s|%s|%s", DATABUS, POLL, escapeSeparators(subscription.toString()));
    }

    public static String injectDatabus(VerifiableResource subscription) {
        return format("%s|%s|%s", DATABUS, INJECT, escapeSeparators(subscription.toString()));
    }

    public static String assumeDatabusSubscriptionOwnership(VerifiableResource subscription) {
        return format("%s|%s|%s", DATABUS, ASSUME_OWNERSHIP, escapeSeparators(subscription.toString()));
    }

    public static String unlimitedDatabus(VerifiableResource subscription) {
        return format("%s|%s|%s", DATABUS, ALL, escapeSeparators(subscription.toString()));
    }

    // Authentication and authorization permissions

    /**
     * By convention permissions for roles with no group use "_" as the group name for permissions.
     */
    public static VerifiableResource toRoleGroupResource(@Nullable String group) {
        return new NamedResource(Objects.firstNonNull(group, "_"));
    }

    public static String readRole(VerifiableResource group) {
        return readRole(group, ALL);
    }

    public static String readRole(VerifiableResource group, VerifiableResource role) {
        return format("%s|%s|%s|%s", ROLE, READ, escapeSeparators(group.toString()), escapeSeparators(role.toString()));
    }

    public static String readRole(RoleIdentifier roleIdentifier) {
        return readRole(toRoleGroupResource(roleIdentifier.getGroup()), new NamedResource(roleIdentifier.getId()));
    }

    public static String createRole(VerifiableResource group) {
        return createRole(group, ALL);
    }

    public static String createRole(VerifiableResource group, VerifiableResource role) {
        return format("%s|%s|%s|%s", ROLE, CREATE, escapeSeparators(group.toString()), escapeSeparators(role.toString()));
    }

    public static String createRole(RoleIdentifier roleIdentifier) {
        return createRole(toRoleGroupResource(roleIdentifier.getGroup()), new NamedResource(roleIdentifier.getId()));
    }

    public static String updateRole(VerifiableResource group) {
        return updateRole(group, ALL);
    }

    public static String updateRole(VerifiableResource group, VerifiableResource role) {
        return format("%s|%s|%s|%s", ROLE, UPDATE, escapeSeparators(group.toString()), escapeSeparators(role.toString()));
    }

    public static String updateRole(RoleIdentifier roleIdentifier) {
        return updateRole(toRoleGroupResource(roleIdentifier.getGroup()), new NamedResource(roleIdentifier.getId()));
    }

    public static String deleteRole(VerifiableResource group) {
        return deleteRole(group, ALL);
    }

    public static String deleteRole(RoleIdentifier roleIdentifier) {
        return deleteRole(toRoleGroupResource(roleIdentifier.getGroup()), new NamedResource(roleIdentifier.getId()));
    }

    public static String deleteRole(VerifiableResource group, VerifiableResource role) {
        return format("%s|%s|%s|%s", ROLE, DELETE, escapeSeparators(group.toString()), escapeSeparators(role.toString()));
    }

    public static String grantRole(VerifiableResource group) {
        return grantRole(group, ALL);
    }

    public static String grantRole(VerifiableResource group, VerifiableResource role) {
        return format("%s|%s|%s|%s", ROLE, GRANT, escapeSeparators(group.toString()), escapeSeparators(role.toString()));
    }

    public static String grantRole(RoleIdentifier roleIdentifier) {
        return grantRole(toRoleGroupResource(roleIdentifier.getGroup()), new NamedResource(roleIdentifier.getId()));
    }

    public static String unlimitedRole() {
        return ROLE;
    }

    public static String readApiKey() {
        return format("%s|%s", API_KEY, READ);
    }

    public static String createApiKey() {
        return format("%s|%s", API_KEY, CREATE);
    }

    public static String createExactApiKey() {
        return format("%s|%s", API_KEY, CREATE_EXACT);
    }

    public static String updateApiKey() {
        return format("%s|%s", API_KEY, UPDATE);
    }

    public static String deleteApiKey() {
        return format("%s|%s", API_KEY, DELETE);
    }

    public static String findApiKeyByKey() {
        return format("%s|%s", API_KEY, VIEW_BY_KEY);
    }

    public static String unlimitedApiKey() {
        return API_KEY;
    }

    // System permissions
    
    /**
     * Although the following permission concerns the databus it is placed in the "system" resource since
     * databus replication should only be performed internally by the system.
     */
    public static String replicateDatabus() {
        return format("%s|%s", SYSTEM, REPLICATE_DATABUS);
    }

    /**
     * Although the following permission concerns the databus it is placed in the "system" resource since
     * raw databus access should be controlled by system administrators.
     */
    public static String rawDatabus() {
        return format("%s|%s", SYSTEM, RAW_DATABUS);
    }
}
