package com.bazaarvoice.emodb.web.resources.blob;

import com.bazaarvoice.emodb.blob.api.RangeSpecification;
import com.bazaarvoice.emodb.blob.api.RangeSpecifications;
import io.dropwizard.jersey.params.AbstractParam;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses an HTTP "Range" header.  Note that, per the HTTP spec, invalid or unsupported range headers must be ignored.
 * So parsing a range header should never thrown an exception.
 */
public class RangeParam extends AbstractParam<RangeSpecification> {

    private static final Pattern RANGE_BYTES = Pattern.compile("^bytes=(\\d+)?-(\\d+)?$");

    public RangeParam(String input) {
        super(input);
    }

    @Override
    protected RangeSpecification parse(String input) throws Exception {
        Matcher matcher = RANGE_BYTES.matcher(input);
        if (!matcher.matches()) {
            return null;  // Invalid or unsupported range header, ignore it.
        }
        Long start, end;
        try {
            start = matcher.group(1) != null ? Long.valueOf(matcher.group(1)) : null;
            end = matcher.group(2) != null ? Long.valueOf(matcher.group(2)) : null;
        } catch (NumberFormatException e)  {
            return null;  // Invalid range header, ignore it.
        }
        if (start != null && end != null && start <= end) {
            return RangeSpecifications.slice(start, end - start + 1);  // HTTP range end is inclusive
        } else if (start != null && end == null) {
            return RangeSpecifications.slice(start);
        } else if (start == null && end != null) {
            return RangeSpecifications.suffix(end);
        }
        return null;  // Invalid range header, ignore it.
    }
}
