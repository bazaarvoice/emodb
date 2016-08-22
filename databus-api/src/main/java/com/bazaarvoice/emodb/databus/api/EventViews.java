package com.bazaarvoice.emodb.databus.api;

/**
 * Class hierarchy for JsonViews into {@link Event} objects.
 */
public class EventViews {

    /**
     * View for including the event key and content only in the Event.
     */
    public static class ContentOnly {}

    /**
     * View for including the event key, content, and tags in the Event.
     */
    public static class WithTags extends ContentOnly {}

}
