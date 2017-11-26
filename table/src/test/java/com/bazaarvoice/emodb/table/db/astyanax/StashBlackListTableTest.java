package com.bazaarvoice.emodb.table.db.astyanax;

import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.api.TableOptions;
import com.bazaarvoice.emodb.sor.api.TableOptionsBuilder;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.table.db.test.InMemoryTable;
import com.google.common.collect.ImmutableMap;
import org.testng.Assert;
import org.testng.annotations.Test;

public class StashBlackListTableTest {

    @Test
    public void testIsTableBlacklisted()
            throws Exception {

        TableOptions options = new TableOptionsBuilder().setPlacement("sor-ugc").build();

        // TRUE cases
        Assert.assertEquals(AstyanaxTableDAO.isTableBlacklisted(new InMemoryTable("review:customer", options, ImmutableMap.<String, Object>of(), false),
                Conditions.alwaysTrue()), true);

        Assert.assertEquals(AstyanaxTableDAO.isTableBlacklisted(new InMemoryTable("review:customer", options, ImmutableMap.<String, Object>of(), false),
                Conditions.intrinsic(Intrinsic.TABLE, Conditions.equal("review:customer"))), true);
        Assert.assertEquals(AstyanaxTableDAO.isTableBlacklisted(new InMemoryTable("review:customer", options, ImmutableMap.<String, Object>of(), false),
                Conditions.intrinsic(Intrinsic.TABLE, Conditions.like("*:customer"))), true);
        Assert.assertEquals(AstyanaxTableDAO.isTableBlacklisted(new InMemoryTable("review:customer", options, ImmutableMap.<String, Object>of(), false),
                Conditions.or(Conditions.intrinsic(Intrinsic.TABLE, Conditions.equal("review:customer")), Conditions.intrinsic(Intrinsic.TABLE, Conditions.equal("review:customer2")))), true);

        Assert.assertEquals(AstyanaxTableDAO.isTableBlacklisted(new InMemoryTable("review:customer", options, ImmutableMap.<String, Object>of(), false),
                Conditions.intrinsic(Intrinsic.PLACEMENT, Conditions.equal("sor-ugc"))), true);
        Assert.assertEquals(AstyanaxTableDAO.isTableBlacklisted(new InMemoryTable("review:customer", options, ImmutableMap.<String, Object>of(), false),
                Conditions.intrinsic(Intrinsic.PLACEMENT, Conditions.like("*sor*"))), true);
        Assert.assertEquals(AstyanaxTableDAO.isTableBlacklisted(new InMemoryTable("review:customer", options, ImmutableMap.<String, Object>of(), false),
                Conditions.or(Conditions.intrinsic(Intrinsic.PLACEMENT, Conditions.equal("sor-ugc")), Conditions.intrinsic(Intrinsic.PLACEMENT, Conditions.equal("sor-cat")))), true);

        Assert.assertEquals(AstyanaxTableDAO.isTableBlacklisted(new InMemoryTable("review:customer", options, ImmutableMap.<String, Object>of(), false),
                Conditions.and(Conditions.intrinsic(Intrinsic.TABLE, Conditions.equal("review:customer")), Conditions.intrinsic(Intrinsic.PLACEMENT, Conditions.equal("sor-ugc")))), true);
        Assert.assertEquals(AstyanaxTableDAO.isTableBlacklisted(new InMemoryTable("review:customer", options, ImmutableMap.<String, Object>of(), false),
                Conditions.and(Conditions.or(Conditions.intrinsic(Intrinsic.TABLE, Conditions.equal("review:customer")), Conditions.intrinsic(Intrinsic.TABLE, Conditions.equal("review:customer2"))),
                        Conditions.intrinsic(Intrinsic.PLACEMENT, Conditions.equal("sor-ugc")))), true);

        Assert.assertEquals(AstyanaxTableDAO.isTableBlacklisted(new InMemoryTable("review:customer", options, ImmutableMap.<String, Object>of("type", "review"), false),
                Conditions.mapBuilder().contains("type", "review").build()), true);
        Assert.assertEquals(AstyanaxTableDAO.isTableBlacklisted(new InMemoryTable("review:customer", options, ImmutableMap.<String, Object>of("type", "review", "some-key", "some-value"), false),
                Conditions.mapBuilder().contains("type", "review").build()), true);
        Assert.assertEquals(AstyanaxTableDAO.isTableBlacklisted(new InMemoryTable("review:customer", options, ImmutableMap.<String, Object>of("type", "review"), false),
                Conditions.or(Conditions.mapBuilder().contains("type", "review").build(), Conditions.mapBuilder().contains("some-key", "some-value").build())), true);


        // FALSE cases
        Assert.assertEquals(AstyanaxTableDAO.isTableBlacklisted(new InMemoryTable("review:customer", options, ImmutableMap.<String, Object>of(), false),
                Conditions.alwaysFalse()), false);

        Assert.assertEquals(AstyanaxTableDAO.isTableBlacklisted(new InMemoryTable("review:customer", options, ImmutableMap.<String, Object>of(), false),
                Conditions.intrinsic(Intrinsic.TABLE, Conditions.equal("review:customer2"))), false);
        Assert.assertEquals(AstyanaxTableDAO.isTableBlacklisted(new InMemoryTable("review:customer", options, ImmutableMap.<String, Object>of(), false),
                Conditions.intrinsic(Intrinsic.TABLE, Conditions.like("cat*:customer"))), false);
        Assert.assertEquals(AstyanaxTableDAO.isTableBlacklisted(new InMemoryTable("review:customer", options, ImmutableMap.<String, Object>of(), false),
                Conditions.or(Conditions.intrinsic(Intrinsic.TABLE, Conditions.equal("review:customer2")),
                        Conditions.intrinsic(Intrinsic.TABLE, Conditions.equal("review:customer3")))), false);

        Assert.assertEquals(AstyanaxTableDAO.isTableBlacklisted(new InMemoryTable("review:customer", options, ImmutableMap.<String, Object>of(), false),
                Conditions.intrinsic(Intrinsic.PLACEMENT, Conditions.equal("sor-cat"))), false);
        Assert.assertEquals(AstyanaxTableDAO.isTableBlacklisted(new InMemoryTable("review:customer", options, ImmutableMap.<String, Object>of(), false),
                Conditions.intrinsic(Intrinsic.PLACEMENT, Conditions.like("*cat*"))), false);
        Assert.assertEquals(AstyanaxTableDAO.isTableBlacklisted(new InMemoryTable("review:customer", options, ImmutableMap.<String, Object>of(), false),
                Conditions.or(Conditions.intrinsic(Intrinsic.PLACEMENT, Conditions.equal("databus")),
                        Conditions.intrinsic(Intrinsic.PLACEMENT, Conditions.equal("blob")))), false);

        Assert.assertEquals(AstyanaxTableDAO.isTableBlacklisted(new InMemoryTable("review:customer", options, ImmutableMap.<String, Object>of(), false),
                Conditions.and(Conditions.intrinsic(Intrinsic.TABLE, Conditions.equal("review:customer")),
                        Conditions.intrinsic(Intrinsic.PLACEMENT, Conditions.equal("sor-cat")))), false);
        Assert.assertEquals(AstyanaxTableDAO.isTableBlacklisted(new InMemoryTable("review:customer", options, ImmutableMap.<String, Object>of(), false),
                Conditions.and(Conditions.or(Conditions.intrinsic(Intrinsic.TABLE, Conditions.equal("review:customer2")), Conditions.intrinsic(Intrinsic.TABLE, Conditions.equal("review:customer3"))),
                        Conditions.intrinsic(Intrinsic.PLACEMENT, Conditions.equal("sor-ugc")))), false);
        Assert.assertEquals(AstyanaxTableDAO.isTableBlacklisted(new InMemoryTable("review:customer", options, ImmutableMap.<String, Object>of(), false),
                Conditions.and(Conditions.or(Conditions.intrinsic(Intrinsic.TABLE, Conditions.equal("review:customer")), Conditions.intrinsic(Intrinsic.TABLE, Conditions.equal("review:customer2"))),
                        Conditions.intrinsic(Intrinsic.PLACEMENT, Conditions.equal("sor-cat")))), false);

        Assert.assertEquals(AstyanaxTableDAO.isTableBlacklisted(new InMemoryTable("review:customer", options, ImmutableMap.<String, Object>of("type", "review"), false),
                Conditions.mapBuilder().contains("type", "story").build()), false);
        Assert.assertEquals(AstyanaxTableDAO.isTableBlacklisted(new InMemoryTable("review:customer", options, ImmutableMap.<String, Object>of("type", "review", "some-key", "some-value"), false),
                Conditions.mapBuilder().contains("type", "story").build()), false);
        Assert.assertEquals(AstyanaxTableDAO.isTableBlacklisted(new InMemoryTable("review:customer", options, ImmutableMap.<String, Object>of("type", "review"), false),
                Conditions.and(Conditions.mapBuilder().contains("type", "review").build(), Conditions.mapBuilder().contains("some-key", "some-value").build())), false);

    }
}
