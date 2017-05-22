package com.bazaarvoice.emodb.web.auth;

import com.bazaarvoice.emodb.blob.api.BlobStore;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.api.DefaultTable;
import com.bazaarvoice.emodb.sor.api.TableAvailability;
import com.bazaarvoice.emodb.sor.api.TableOptions;
import com.bazaarvoice.emodb.sor.api.TableOptionsBuilder;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import javax.annotation.Nullable;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class EmoPermissionTest {

    private DataStore _dataStore;
    private BlobStore _blobStore;
    private EmoPermissionResolver _resolver;

    @BeforeTest
    public void setUpResolver() {
        _dataStore = mock(DataStore.class);
        _blobStore = mock(BlobStore.class);
        _resolver = new EmoPermissionResolver(_dataStore, _blobStore);
    }

    @Test
    public void testSorCreatePermissionAny() {
        EmoPermission perm = _resolver.resolvePermission("sor|create_table|*");
        assertTrue(perm.implies(_resolver.resolvePermission("sor|create_table|test:table")));
    }

    @Test
    public void testSorCreatePermissionWildcard() {
        EmoPermission perm = _resolver.resolvePermission("sor|create_*|test:*");
        assertTrue(perm.implies(_resolver.resolvePermission("sor|create_table|test:table")));
        assertFalse(perm.implies(_resolver.resolvePermission("sor|create_table|restricted:table")));
        assertFalse(perm.implies(_resolver.resolvePermission("sor|drop_table|test:table")));
    }

    @Test
    public void testSorCreatePermissionConstant() {
        EmoPermission perm = _resolver.resolvePermission("sor|create_table|test:table");
        assertTrue(perm.implies(_resolver.resolvePermission("sor|create_table|test:table")));
        assertFalse(perm.implies(_resolver.resolvePermission("sor|create_table|restricted:table")));
        assertFalse(perm.implies(_resolver.resolvePermission("sor|drop_table|test:table")));
    }

    @Test
    public void testSorCreatePermissionCondition() {
        EmoPermission perm = _resolver.resolvePermission("sor|if(or(\"create_table\",\"update\"))|if(intrinsic(\"~table\":like(\"test*table\")))");
        assertTrue(perm.implies(_resolver.resolvePermission("sor|create_table|test:table")));
        assertFalse(perm.implies(_resolver.resolvePermission("sor|create_table|restricted:table")));
        assertFalse(perm.implies(_resolver.resolvePermission("sor|drop_table|test:table")));
    }

    @Test
    public void testSorCreatePermissionMatcherRestrictedName() {
        EmoPermission perm = _resolver.resolvePermission("sor|create_table|test*table");
        assertTrue(perm.implies(_resolver.resolvePermission("sor|create_table|createTable(name:'test:table',placement:ugc_us)")));
        assertFalse(perm.implies(_resolver.resolvePermission("sor|create_table|createTable(name:'restricted:table',placement:ug_us)")));
    }

    @Test
    public void testSorCreatePermissionMatcherRestrictedPlacement() {
        EmoPermission perm = _resolver.resolvePermission("sor|create_table|if(intrinsic(\"~placement\":\"ugc_us\"))");
        assertTrue(perm.implies(_resolver.resolvePermission("sor|create_table|createTable(name:'test:table',placement:ugc_us)")));
        assertFalse(perm.implies(_resolver.resolvePermission("sor|create_table|createTable(name:'test:table',placement:ugc_eu)")));
    }

    @Test
    public void testSorCreatePermissionMatcherRestrictedAttributes() {
        EmoPermission perm = _resolver.resolvePermission("sor|create_table|if({..,\"a\":\"b\",\"c\":\"d\"})");
        assertTrue(perm.implies(_resolver.resolvePermission("sor|create_table|createTable(name:'test:table',placement:ugc_us,attributes:(a:b,c:d))")));
        assertTrue(perm.implies(_resolver.resolvePermission("sor|create_table|createTable(name:'test:table',placement:ugc_us,attributes:(a:b,c:d,e:f))")));
        assertFalse(perm.implies(_resolver.resolvePermission("sor|create_table|createTable(name:'test:table',placement:ugc_us)")));
        assertFalse(perm.implies(_resolver.resolvePermission("sor|create_table|createTable(name:'test:table',placement:ugc_us,attributes:(a:b))")));
        assertFalse(perm.implies(_resolver.resolvePermission("sor|create_table|createTable(name:'test:table',placement:ugc_us,attributes:(a:b,c:z))")));
        assertFalse(perm.implies(_resolver.resolvePermission("sor|create_table|createTable(name:'test:table',placement:ugc_us,attributes:(y:z))")));
    }

    @Test
    public void testSorCreatePermissionMatcherMultipleRestrictions() {
        EmoPermission perm = _resolver.resolvePermission("sor|create_table|" +
                "if(and(" +
                        "intrinsic(\"~table\":like(\"test:*\"))," +
                        "intrinsic(\"~placement\":\"ugc_us\")," +
                        "{..,\"a\":\"b\"}" +
                "))");

        assertTrue(perm.implies(_resolver.resolvePermission("sor|create_table|createTable(name:'test:table',placement:ugc_us,attributes:(a:b))")));
        assertFalse(perm.implies(_resolver.resolvePermission("sor|create_table|createTable(name:'restricted:table',placement:ugc_us,attributes:(a:b))")));
        assertFalse(perm.implies(_resolver.resolvePermission("sor|create_table|createTable(name:'test:table',placement:ugc_eu,attributes:(a:b))")));
        assertFalse(perm.implies(_resolver.resolvePermission("sor|create_table|createTable(name:'test:table',placement:ugc_us,attributes:(a:z))")));
    }

    @Test
    public void testBlobCreatePermissionAny() {
        EmoPermission perm = _resolver.resolvePermission("blob|create_table|*");
        assertTrue(perm.implies(_resolver.resolvePermission("blob|create_table|test:table")));
    }

    @Test
    public void testBlobCreatePermissionWildcard() {
        EmoPermission perm = _resolver.resolvePermission("blob|create_*|test:*");
        assertTrue(perm.implies(_resolver.resolvePermission("blob|create_table|test:table")));
        assertFalse(perm.implies(_resolver.resolvePermission("blob|create_table|restricted:table")));
        assertFalse(perm.implies(_resolver.resolvePermission("blob|drop_table|test:table")));
    }

    @Test
    public void testBlobCreatePermissionConstant() {
        EmoPermission perm = _resolver.resolvePermission("blob|create_table|test:table");
        assertTrue(perm.implies(_resolver.resolvePermission("blob|create_table|test:table")));
        assertFalse(perm.implies(_resolver.resolvePermission("blob|create_table|restricted:table")));
        assertFalse(perm.implies(_resolver.resolvePermission("blob|drop_table|test:table")));
    }

    @Test
    public void testBlobCreatePermissionCondition() {
        EmoPermission perm = _resolver.resolvePermission("blob|if(or(\"create_table\",\"update\"))|if(intrinsic(\"~table\":like(\"test*table\")))");
        assertTrue(perm.implies(_resolver.resolvePermission("blob|create_table|test:table")));
        assertFalse(perm.implies(_resolver.resolvePermission("blob|create_table|restricted:table")));
        assertFalse(perm.implies(_resolver.resolvePermission("blob|drop_table|test:table")));
    }

    @Test
    public void testBlobCreatePermissionMatcherRestrictedName() {
        EmoPermission perm = _resolver.resolvePermission("blob|create_table|test*table");
        assertTrue(perm.implies(_resolver.resolvePermission("blob|create_table|createTable(name:'test:table',placement:ugc_us)")));
        assertFalse(perm.implies(_resolver.resolvePermission("blob|create_table|createTable(name:'restricted:table',placement:ug_us)")));
    }

    @Test
    public void testBlobCreatePermissionMatcherRestrictedPlacement() {
        EmoPermission perm = _resolver.resolvePermission("blob|create_table|if(intrinsic(\"~placement\":\"ugc_us\"))");
        assertTrue(perm.implies(_resolver.resolvePermission("blob|create_table|createTable(name:'test:table',placement:ugc_us)")));
        assertFalse(perm.implies(_resolver.resolvePermission("blob|create_table|createTable(name:'test:table',placement:ugc_eu)")));
    }

    @Test
    public void testBlobCreatePermissionMatcherRestrictedAttributes() {
        EmoPermission perm = _resolver.resolvePermission("blob|create_table|if({..,\"a\":\"b\",\"c\":\"d\"})");
        assertTrue(perm.implies(_resolver.resolvePermission("blob|create_table|createTable(name:'test:table',placement:ugc_us,attributes:(a:b,c:d))")));
        assertTrue(perm.implies(_resolver.resolvePermission("blob|create_table|createTable(name:'test:table',placement:ugc_us,attributes:(a:b,c:d,e:f))")));
        assertFalse(perm.implies(_resolver.resolvePermission("blob|create_table|createTable(name:'test:table',placement:ugc_us)")));
        assertFalse(perm.implies(_resolver.resolvePermission("blob|create_table|createTable(name:'test:table',placement:ugc_us,attributes:(a:b))")));
        assertFalse(perm.implies(_resolver.resolvePermission("blob|create_table|createTable(name:'test:table',placement:ugc_us,attributes:(a:b,c:z))")));
        assertFalse(perm.implies(_resolver.resolvePermission("blob|create_table|createTable(name:'test:table',placement:ugc_us,attributes:(y:z))")));
    }

    @Test
    public void testBlobCreatePermissionMatcherMultipleRestrictions() {
        EmoPermission perm = _resolver.resolvePermission("blob|create_table|" +
                "if(and(" +
                        "intrinsic(\"~table\":like(\"test:*\"))," +
                        "intrinsic(\"~placement\":\"ugc_us\")," +
                        "{..,\"a\":\"b\"}" +
                "))");

        assertTrue(perm.implies(_resolver.resolvePermission("blob|create_table|createTable(name:'test:table',placement:ugc_us,attributes:(a:b))")));
        assertFalse(perm.implies(_resolver.resolvePermission("blob|create_table|createTable(name:'restricted:table',placement:ugc_us,attributes:(a:b))")));
        assertFalse(perm.implies(_resolver.resolvePermission("blob|create_table|createTable(name:'test:table',placement:ugc_eu,attributes:(a:b))")));
        assertFalse(perm.implies(_resolver.resolvePermission("blob|create_table|createTable(name:'test:table',placement:ugc_us,attributes:(a:z))")));
    }

    @Test
    public void testSorUpdatePermissionAny() {
        EmoPermission perm = _resolver.resolvePermission("sor|update|*");
        assertTrue(perm.implies(_resolver.resolvePermission("sor|update|test:table")));
    }

    @Test
    public void testSorUpdatePermissionWildcard() {
        EmoPermission perm = _resolver.resolvePermission("sor|up*|test:*");
        assertTrue(perm.implies(_resolver.resolvePermission("sor|update|test:table")));
        assertFalse(perm.implies(_resolver.resolvePermission("sor|update|restricted:table")));
        assertFalse(perm.implies(_resolver.resolvePermission("sor|drop_table|test:table")));
    }

    @Test
    public void testSorUpdatePermissionCondition() {
        EmoPermission perm = _resolver.resolvePermission("sor|if(or(\"create_table\",\"update\"))|if(intrinsic(\"~table\":like(\"test*table\")))");
        assertTrue(perm.implies(_resolver.resolvePermission("sor|update|test:table")));
        assertFalse(perm.implies(_resolver.resolvePermission("sor|update|restricted:table")));
        assertFalse(perm.implies(_resolver.resolvePermission("sor|drop_table|test:table")));
    }

    @Test
    public void testSorUpdatePermissionMatcherRestrictedName() {
        EmoPermission perm = _resolver.resolvePermission("sor|update|test*table");
        assertTrue(perm.implies(_resolver.resolvePermission("sor|update|test:table")));
        assertFalse(perm.implies(_resolver.resolvePermission("sor|update|restricted:table")));
    }

    @Test
    public void testSorUpdatePermissionMatcherRestrictedPlacement() {
        addSorTable("test:us_table", "ugc_us", ImmutableMap.<String, Object>of());
        addSorTable("test:eu_table", "ugc_eu", ImmutableMap.<String, Object>of());

        EmoPermission perm = _resolver.resolvePermission("sor|update|if(intrinsic(\"~placement\":\"ugc_us\"))");
        assertTrue(perm.implies(_resolver.resolvePermission("sor|update|test:us_table")));
        assertFalse(perm.implies(_resolver.resolvePermission("sor|update|test:eu_table")));
    }

    @Test
    public void testSorUpdatePermissionMatcherRestrictedAttributes() {
        addSorTable("test:bar_table", "ugc_us", ImmutableMap.<String, Object>of("foo", "bar"));
        addSorTable("test:barplus_table", "ugc_us", ImmutableMap.<String, Object>of("foo", "bar", "roo", "rar"));
        addSorTable("test:baz_table", "ugc_us", ImmutableMap.<String, Object>of("foo", "baz"));

        EmoPermission perm = _resolver.resolvePermission("sor|update|if({..,\"foo\":\"bar\"})");
        assertTrue(perm.implies(_resolver.resolvePermission("sor|update|test:bar_table")));
        assertTrue(perm.implies(_resolver.resolvePermission("sor|update|test:barplus_table")));
        assertFalse(perm.implies(_resolver.resolvePermission("sor|update|test:baz_table")));
    }

    @Test
    public void testSorUpdatePermissionMatcherMultipleRestrictions() {
        addSorTable("test:good", "ugc_us", ImmutableMap.<String, Object>of("foo", "bar"));
        addSorTable("restricted:bad_name", "ugc_us", ImmutableMap.<String, Object>of("foo", "bar"));
        addSorTable("test:bad_placement", "ugc_eu", ImmutableMap.<String, Object>of("foo", "bar"));
        addSorTable("test:bad_attributes", "ugc_us", ImmutableMap.<String, Object>of("foo", "baz"));

        EmoPermission perm = _resolver.resolvePermission("sor|update|" +
                "if(and(" +
                        "intrinsic(\"~table\":like(\"test:*\"))," +
                        "intrinsic(\"~placement\":in(\"ugc_us\",\"ugc_global\"))," +
                        "{..,\"foo\":\"bar\"}" +
                "))");

        assertTrue(perm.implies(_resolver.resolvePermission("sor|update|test:good")));
        assertFalse(perm.implies(_resolver.resolvePermission("sor|update|restricted:bad_name")));
        assertFalse(perm.implies(_resolver.resolvePermission("sor|update|test:bad_placement")));
        assertFalse(perm.implies(_resolver.resolvePermission("sor|update|test:bad_attributes")));
    }

    @Test
    public void testFacadePermissionsWildcard() {
        EmoPermission perm = _resolver.resolvePermission("facade|create_facade|test:*");
        assertTrue(perm.implies(_resolver.resolvePermission("facade|create_facade|test:table")));
        assertFalse(perm.implies(_resolver.resolvePermission("facade|create_facade|restricted:table")));
    }

    @Test
    public void testFacadePermissionsCondition() {
        EmoPermission perm = _resolver.resolvePermission("facade|create_facade|if(like(\"test:*\"))");
        assertTrue(perm.implies(_resolver.resolvePermission("facade|create_facade|test:table")));
        assertFalse(perm.implies(_resolver.resolvePermission("facade|create_facade|restricted:table")));
    }

    @Test
    public void testSorUpdateWithNoDataStore() {
        EmoPermission perm = new EmoPermission(null, mock(BlobStore.class), "sor|update|if(intrinsic(\"~placement\":\"ugc_us\"))");
        assertFalse(perm.implies(_resolver.resolvePermission("sor|update|test:table")));
    }

    @Test
    public void testBlobUpdateWithNoBlobStore() {
        EmoPermission perm = new EmoPermission(mock(DataStore.class), null, "blob|update|if(intrinsic(\"~placement\":\"ugc_us\"))");
        assertFalse(perm.implies(_resolver.resolvePermission("blob|update|test:table")));
    }

    @Test
    public void testPureWildcard() {
        EmoPermission perm = _resolver.resolvePermission("*");
        assertTrue(perm.implies(_resolver.resolvePermission("sor|create_table|createTable(name:'test:newtable',placement:ugc_us,attributes:(foo:bar))")));
        assertTrue(perm.implies(_resolver.resolvePermission("sor|update|test:table")));
        assertTrue(perm.implies(_resolver.resolvePermission("facade|create_facade|test:table")));
    }

    @Test
    public void testEscapedSeparators() {
        addSorTable("good_table", "ugc_us", ImmutableMap.<String, Object>of("piper", "red|blue"));
        addSorTable("wrong_value", "ugc_us", ImmutableMap.<String, Object>of("piper", "no_pipe"));
        addSorTable("missing_attribute", "ugc_us", ImmutableMap.<String, Object>of());

        EmoPermission perm = _resolver.resolvePermission("sor|update|if({..,\"piper\":like(\"*\\|*\")})");
        assertEquals(perm.toString(), "sor|update|if({..,\"piper\":like(\"*\\|*\")})");
        assertTrue(perm.implies(_resolver.resolvePermission("sor|update|good_table")));
        assertFalse(perm.implies(_resolver.resolvePermission("sor|update|wrong_value")));
        assertFalse(perm.implies(_resolver.resolvePermission("sor|update|missing_attribute")));

        // Currently EmoDB does not allow pipe characters in any supported object names.  The following test
        // verifies the permission parser works in the even this restriction is relaxed or a new object is introduced
        // that does support pipe characters.

        perm = _resolver.resolvePermission("piper|pipe|*\\|*");
        assertEquals(perm.toString(), "piper|pipe|*\\|*");
        assertTrue(perm.implies(_resolver.resolvePermission("piper|pipe|red\\|blue")));
        assertFalse(perm.implies(_resolver.resolvePermission("piper|pipe|no_pipe")));
    }

    @Test
    public void testFacadePermission() {
        // Table exists remotely in ugc_eu but has a local facade in ugc_us
        addFacadeTable("test:facade", "ugc_eu", "ugc_us", ImmutableMap.<String, Object>of());

        EmoPermission usPerm = _resolver.resolvePermission("sor|*|if(intrinsic(\"~placement\":\"ugc_us\"))");
        assertTrue(usPerm.implies(_resolver.resolvePermission("sor|read|test:facade")));
        assertTrue(usPerm.implies(_resolver.resolvePermission("sor|update|test:facade")));
        assertFalse(usPerm.implies(_resolver.resolvePermission("sor|set_table_attributes|test:facade")));
        assertFalse(usPerm.implies(_resolver.resolvePermission("sor|drop_table|test:facade")));

        EmoPermission euPerm = _resolver.resolvePermission("sor|*|if(intrinsic(\"~placement\":\"ugc_eu\"))");
        assertFalse(euPerm.implies(_resolver.resolvePermission("sor|read|test:facade")));
        assertFalse(euPerm.implies(_resolver.resolvePermission("sor|update|test:facade")));
        assertTrue(euPerm.implies(_resolver.resolvePermission("sor|set_table_attributes|test:facade")));
        assertTrue(euPerm.implies(_resolver.resolvePermission("sor|drop_table|test:facade")));

        EmoPermission globalPerm = _resolver.resolvePermission("sor|*|if(intrinsic(\"~placement\":in(\"ugc_us\",\"ugc_eu\")))");
        assertTrue(globalPerm.implies(_resolver.resolvePermission("sor|read|test:facade")));
        assertTrue(globalPerm.implies(_resolver.resolvePermission("sor|update|test:facade")));
        assertTrue(globalPerm.implies(_resolver.resolvePermission("sor|set_table_attributes|test:facade")));
        assertTrue(globalPerm.implies(_resolver.resolvePermission("sor|drop_table|test:facade")));
    }

    @Test
    public void testUnavailableLocallyPermission() {
        // Table exists remotely in ugc_eu but not locally in ugc_us
        addSorTable("test:facade", "ugc_eu", null, ImmutableMap.<String, Object>of());

        EmoPermission usPerm = _resolver.resolvePermission("sor|*|if(intrinsic(\"~placement\":\"ugc_us\"))");
        assertFalse(usPerm.implies(_resolver.resolvePermission("sor|read|test:facade")));
        assertFalse(usPerm.implies(_resolver.resolvePermission("sor|update|test:facade")));
        assertFalse(usPerm.implies(_resolver.resolvePermission("sor|set_table_attributes|test:facade")));
        assertFalse(usPerm.implies(_resolver.resolvePermission("sor|drop_table|test:facade")));

        EmoPermission euPerm = _resolver.resolvePermission("sor|*|if(intrinsic(\"~placement\":\"ugc_eu\"))");
        assertTrue(euPerm.implies(_resolver.resolvePermission("sor|read|test:facade")));
        assertTrue(euPerm.implies(_resolver.resolvePermission("sor|update|test:facade")));
        assertTrue(euPerm.implies(_resolver.resolvePermission("sor|set_table_attributes|test:facade")));
        assertTrue(euPerm.implies(_resolver.resolvePermission("sor|drop_table|test:facade")));

        EmoPermission globalPerm = _resolver.resolvePermission("sor|*|if(intrinsic(\"~placement\":in(\"ugc_us\",\"ugc_eu\")))");
        assertTrue(globalPerm.implies(_resolver.resolvePermission("sor|read|test:facade")));
        assertTrue(globalPerm.implies(_resolver.resolvePermission("sor|update|test:facade")));
        assertTrue(globalPerm.implies(_resolver.resolvePermission("sor|set_table_attributes|test:facade")));
        assertTrue(globalPerm.implies(_resolver.resolvePermission("sor|drop_table|test:facade")));
    }

    @Test
    public void testConstantImpliesConditionPermission() {
        assertTrue(_resolver.resolvePermission("sor|read|test:table").implies(
                _resolver.resolvePermission("sor|if(\"read\")|if(intrinsic(\"~table\":\"test:table\"))")));
        assertFalse(_resolver.resolvePermission("sor|read|other:table").implies(
                _resolver.resolvePermission("sor|if(\"read\")|if(intrinsic(\"~table\":\"test:table\"))")));
        assertFalse(_resolver.resolvePermission("sor|read|other:table").implies(
                _resolver.resolvePermission("sor|if(\"update\")|if(intrinsic(\"~table\":\"test:table\"))")));
        assertFalse(_resolver.resolvePermission("sor|read|test:table").implies(
                _resolver.resolvePermission("sor|if(\"read\")|if(intrinsic(\"~table\":\"other:table\"))")));
        assertFalse(_resolver.resolvePermission("sor|read|test:table").implies(
                _resolver.resolvePermission("sor|if(alwaysTrue())|if(intrinsic(\"~table\":\"test:table\"))")));
        assertFalse(_resolver.resolvePermission("sor|read|test:table").implies(
                _resolver.resolvePermission("sor|if(alwaysTrue())|if(intrinsic(\"~table\":alwaysTrue()))")));
    }

    @Test
    public void testConditionImpliesConditionPermission() {
        assertTrue(_resolver.resolvePermission("sor|if(\"read\")").implies(_resolver.resolvePermission("sor|if(\"read\")")));
        assertTrue(_resolver.resolvePermission("sor|if(in(\"read\",\"update\",\"delete\"))")
                .implies(_resolver.resolvePermission("sor|if(in(\"read\",\"update\"))")));
        assertTrue(_resolver.resolvePermission("sor|if(not(like(\"*delete*\")))")
                .implies(_resolver.resolvePermission("sor|if(in(\"read\",\"update\"))")));
        assertFalse(_resolver.resolvePermission("sor|if(\"read\")").implies(_resolver.resolvePermission("sor|if(\"update\")")));
        assertFalse(_resolver.resolvePermission("sor|if(in(\"read\",\"update\"))")
                .implies(_resolver.resolvePermission("sor|if(in(\"read\",\"update\",\"delete\"))")));
        assertFalse(_resolver.resolvePermission("sor|if(not(like(\"*delete*\")))")
                .implies(_resolver.resolvePermission("sor|if(in(\"read\",\"update\",\"delete\"))")));
    }

    @Test
    public void testTableConditionImpliesTableConditionPermission() {
        assertTrue(_resolver.resolvePermission("sor|read|if(intrinsic(\"~table\":not(like(\"private:*\"))))")
                .implies(_resolver.resolvePermission("sor|read|if(and(intrinsic(\"~table\":not(like(\"private:*\"))),{..,\"type\":in(\"record\",\"other\")}))")));
        assertTrue(_resolver.resolvePermission("sor|read|if(and(intrinsic(\"~table\":not(like(\"private:*\"))),{..,\"type\":in(\"record\",\"other\")}))")
                .implies(_resolver.resolvePermission("sor|read|if(and(intrinsic(\"~table\":not(like(\"private:*\"))),{..,\"type\":in(\"record\",\"other\")}))")));
        assertTrue(_resolver.resolvePermission("sor|read|if(and(intrinsic(\"~table\":not(like(\"private:*\"))),{..,\"type\":in(\"record\",\"other\")}))")
                .implies(_resolver.resolvePermission("sor|read|if(and(intrinsic(\"~table\":\":test:table\"),{..,\"type\":\"record\"}))")));
        assertFalse(_resolver.resolvePermission("sor|read|if(and(intrinsic(\"~table\":not(like(\"private:*\"))),{..,\"type\":in(\"record\",\"other\")}))")
                .implies(_resolver.resolvePermission("sor|read|if(and(intrinsic(\"~table\":\"private:table\"),{..,\"type\":\"record\"}))")));
        assertFalse(_resolver.resolvePermission("sor|read|if(and(intrinsic(\"~table\":not(like(\"private:*\"))),{..,\"type\":in(\"record\",\"other\")}))")
                .implies(_resolver.resolvePermission("sor|read|if(and(intrinsic(\"~table\":\":test:table\"),{..,\"type\":\"neither\"}))")));
        assertFalse(_resolver.resolvePermission("sor|read|if(and(intrinsic(\"~table\":not(like(\"private:*\"))),{..,\"type\":in(\"record\",\"other\")}))")
                .implies(_resolver.resolvePermission("sor|read|if(intrinsic(\"~table\":\":test:table\"))")));
    }

    @Test (expectedExceptions = IllegalArgumentException.class)
    public void testNonConstantInitialScopeRejected() {
        _resolver.resolvePermission("s*");
    }

    @Test (expectedExceptions = IllegalArgumentException.class)
    public void testNarrowedWildcardScopeRejected() {
        _resolver.resolvePermission("*|update");
    }

    @Test (expectedExceptions = IllegalArgumentException.class)
    public void testInvalidActionCondition() {
        _resolver.resolvePermission("sor|if(intrinsic(\"~table\":\"foo\"))");
    }

    @Test (expectedExceptions = IllegalArgumentException.class)
    public void testInvalidTableCondition() {
        _resolver.resolvePermission("sor|*|if(intrinsic(\"~id\":~))");
    }

    private void addSorTable(String name, String placement, Map<String, Object> attributes) {
        TableAvailability availability = new TableAvailability(placement, false);
        addSorTable(name, placement, availability, attributes);
    }

    private void addFacadeTable(String name, String originPlacement, String facadePlacement, Map<String, Object> attributes) {
        TableAvailability availability = new TableAvailability(facadePlacement, true);
        addSorTable(name, originPlacement, availability, attributes);
    }

    private void addSorTable(String name, String placement, @Nullable TableAvailability availability, Map<String, Object> attributes) {
        TableOptions options = new TableOptionsBuilder().setPlacement(placement).build();
        when(_dataStore.getTableMetadata(name)).thenReturn(
                new DefaultTable(name, options, attributes, availability));
        when(_dataStore.getTableTemplate(name)).thenReturn(attributes);
    }
}
