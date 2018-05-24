package com.bazaarvoice.emodb.databus.api;

import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.Date;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class SubscriptionCompareTest {

    @Test
    public void testSubscriptionCompare(){
        Date now = new Date();
        Subscription first = new DefaultSubscription("test-subscription",
                Conditions.intrinsic(Intrinsic.TABLE, "review:testcustomer"),
                now, Duration.ofHours(48));

        Subscription second = new DefaultSubscription("test-subscription",
                Conditions.intrinsic(Intrinsic.TABLE, "review:testcustomer"),
                now, Duration.ofHours(48));

        assertTrue(first.equals(second));
        assertTrue(first.equals(first));
    }

    @DataProvider
    public Object[][] negativeCompareDataProvider() {
        Date now = new Date();
        return new Object[][] {
                {
                        new DefaultSubscription("test-subscription", Conditions.intrinsic(Intrinsic.TABLE, "review:testcustomer"), now, Duration.ofHours(48)),
                        new DefaultSubscription("test-subscription-2", Conditions.intrinsic(Intrinsic.TABLE, "review:testcustomer"), now, Duration.ofHours(48))
                },
                {
                        new DefaultSubscription("test-subscription", Conditions.intrinsic(Intrinsic.TABLE, "review:testcustomer"), now, Duration.ofHours(48)),
                        new DefaultSubscription("test-subscription", Conditions.intrinsic(Intrinsic.TABLE, "review:testcustomer2"), now, Duration.ofHours(48))
                },
                {
                        new DefaultSubscription("test-subscription", Conditions.intrinsic(Intrinsic.TABLE, "review:testcustomer"), now, Duration.ofHours(48)),
                        new DefaultSubscription("test-subscription", Conditions.intrinsic(Intrinsic.TABLE, "review:testcustomer"), new Date(now.getTime()+1000), Duration.ofHours(48))
                },
                {
                        new DefaultSubscription("test-subscription", Conditions.intrinsic(Intrinsic.TABLE, "review:testcustomer"), now, Duration.ofHours(48)),
                        new DefaultSubscription("test-subscription", Conditions.intrinsic(Intrinsic.TABLE, "review:testcustomer"), now, Duration.ofHours(47))
                },
        };
    }

    @Test(dataProvider = "negativeCompareDataProvider")
    public void testSubscriptionCompareNegative(Subscription first, Subscription second){
        assertFalse(first.equals(second));
    }
}
