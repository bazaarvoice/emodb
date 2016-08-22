package com.bazaarvoice.emodb.databus.api;

import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dropwizard.jackson.Jackson;
import org.joda.time.Duration;
import org.joda.time.format.ISODateTimeFormat;
import org.testng.annotations.Test;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import static org.testng.Assert.assertEquals;

public class SubscriptionJsonTest {
    @Test
    public void testSubscriptionJson() throws Exception {
        Date now = new Date();
        String nowString = ISODateTimeFormat.dateTime().withZoneUTC().print(now.getTime());

        Subscription expected = new DefaultSubscription("test-subscription",
                Conditions.intrinsic(Intrinsic.TABLE, "review:testcustomer"),
                now, Duration.standardHours(48));

        SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        fmt.setTimeZone(TimeZone.getTimeZone("GMT"));

        ObjectMapper mapper = Jackson.newObjectMapper();
        mapper.setDateFormat(fmt);

        String subscriptionString = mapper.writeValueAsString(expected);
        assertEquals(subscriptionString, "{" +
                "\"name\":\"test-subscription\"," +
                "\"tableFilter\":\"intrinsic(\\\"~table\\\":\\\"review:testcustomer\\\")\"," +
                "\"expiresAt\":\"" + nowString + "\"," +
                "\"eventTtl\":172800000" +
                "}");

        Subscription actual = JsonHelper.fromJson(subscriptionString, Subscription.class);
        assertEquals(actual.getName(), expected.getName());
        assertEquals(actual.getTableFilter(), expected.getTableFilter());
        assertEquals(actual.getExpiresAt(), expected.getExpiresAt());
        assertEquals(actual.getEventTtl(), expected.getEventTtl());
    }
}
