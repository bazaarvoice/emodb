package com.bazaarvoice.emodb.databus.core;

import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import org.testng.annotations.Test;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.UUID;

import static org.testng.Assert.assertEquals;

public class ReplayRequestTest {
    @Test
    public void testReplayRequestJson() {
        String json = "{" +
                "\"subscription\":\"test\"}";
        ReplaySubscriptionRequest request = JsonHelper.fromJson(json, ReplaySubscriptionRequest.class);
        assertEquals(JsonHelper.asJson(request), json, "Json representation without 'since' looks good");
        SimpleDateFormat dateFmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZZZ");
        dateFmt.setTimeZone(TimeZone.getTimeZone("UTC"));
        String jsonWithSince = "{" +
                "\"subscription\":\"test\"," +
                "\"since\":\"" + dateFmt.format(new Date()) + "\"}";
        request = JsonHelper.fromJson(jsonWithSince, ReplaySubscriptionRequest.class);
        assertEquals(JsonHelper.asJson(request), jsonWithSince, "Json representation with 'since' looks good");

        Date date = TimeUUIDs.getDate(UUID.fromString("fb477df0-25ab-11e5-8e96-06ba31973ead"));
        Date date2 = TimeUUIDs.getDate(UUID.fromString("7e7c2590-2543-11e5-9cfe-0a01f5a8819d"));
        Date date3 = TimeUUIDs.getDate(UUID.fromString("80c09750-2543-11e5-9cfe-0a01f5a8819d"));
        Date date4 = TimeUUIDs.getDate(UUID.fromString("ffb5f670-25bd-11e5-b850-68a86d52e332"));
        System.out.println(date);
        System.out.println(date2);
    }
}
