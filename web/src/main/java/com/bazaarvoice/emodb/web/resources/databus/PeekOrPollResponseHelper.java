package com.bazaarvoice.emodb.web.resources.databus;

import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.bazaarvoice.emodb.databus.api.Event;
import com.bazaarvoice.emodb.databus.api.EventViews;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.StreamingOutput;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Helper class used by peek and poll operations.  This class ensures that the events returned are serialized
 * including only those attributes expected by the caller.  In particular, it only includes event tags if the
 * caller explicitly requested to include tags.
 */
public class PeekOrPollResponseHelper {

    private final JsonHelper.JsonWriterWithViewHelper _json;

    public PeekOrPollResponseHelper(final Class<? extends EventViews.ContentOnly> view) {
        checkNotNull(view, "view");
        _json = JsonHelper.withView(view);
    }

    /**
     * Returns a JSON helper that can be used to serialize events using the proper view.
     */
    public JsonHelper.JsonWriterWithViewHelper getJson() {
        return _json;
    }

    /**
     * Returns an object that can be serialized as a response entity to output the event list using the proper view.
     */
    public StreamingOutput asEntity(final List<Event> events) {
        return new StreamingOutput() {
            @Override
            public void write(OutputStream out) throws IOException, WebApplicationException {
                _json.writeJson(out, events);
            }
        };
    }
}
