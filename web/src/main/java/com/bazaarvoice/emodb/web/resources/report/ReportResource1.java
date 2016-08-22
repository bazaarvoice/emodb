package com.bazaarvoice.emodb.web.resources.report;

import com.bazaarvoice.emodb.sor.api.report.TableReportEntry;
import com.bazaarvoice.emodb.sor.api.report.TableReportMetadata;
import com.bazaarvoice.emodb.web.report.AllTablesReportQuery;
import com.bazaarvoice.emodb.web.report.ReportLoader;
import com.bazaarvoice.emodb.web.report.ReportNotFoundException;
import com.google.common.collect.ImmutableMap;
import com.sun.jersey.api.client.ClientResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;

@Path ("/report/1")
@Produces (MediaType.APPLICATION_JSON)
public class ReportResource1 {
    private static final Logger _log = LoggerFactory.getLogger(ReportResource1.class);

    private final ReportLoader _reportLoader;

    public ReportResource1(ReportLoader reportLoader) {
        _reportLoader = reportLoader;
    }

    @Path ("tables/{reportId}/metadata")
    @GET
    public TableReportMetadata getTableReportMetadata(@PathParam ("reportId") String reportId) {
        try {
            return _reportLoader.getTableReportMetadata(reportId);
        } catch (Exception e) {
            throw handleException(reportId, e);
        }
    }

    @Path ("tables/{reportId}/entries")
    @GET
    public Iterable<TableReportEntry> getTableReportEntries(@PathParam  ("reportId") String reportId,
                                              @QueryParam ("table") List<String> tables,
                                              @QueryParam ("placement") List<String> placements,
                                              @QueryParam ("includeDropped") @DefaultValue ("true") boolean includeDropped,
                                              @QueryParam ("includeFacades") @DefaultValue ("true") boolean includeFacades,
                                              @QueryParam ("from") String fromTable,
                                              @QueryParam ("limit") @DefaultValue ("10") int limit) {

        AllTablesReportQuery query = new AllTablesReportQuery();
        if (tables != null && !tables.isEmpty()) {
            query.setTableNames(tables);
        }
        if (placements != null && !placements.isEmpty()) {
            query.setPlacements(placements);
        }
        if (fromTable != null) {
            query.setFromTable(fromTable);
        }
        query.setIncludeDropped(includeDropped);
        query.setIncludeFacades(includeFacades);
        query.setLimit(limit);

        try {
            return _reportLoader.getTableReportEntries(reportId, query);
        } catch (ReportNotFoundException e) {
            throw handleException(reportId, e);
        }
    }

    private RuntimeException handleException(String reportId, Exception e) throws WebApplicationException {
        if (e instanceof ReportNotFoundException) {
            throw new WebApplicationException(
                    Response.status(ClientResponse.Status.NOT_FOUND)
                            .type(MediaType.APPLICATION_JSON_TYPE)
                            .entity(ImmutableMap.of("notFound", reportId))
                            .build());
        }

        _log.warn("Report request failed: [reportId={}]", reportId, e);

        throw new WebApplicationException(
                Response.status(ClientResponse.Status.INTERNAL_SERVER_ERROR)
                        .type(MediaType.APPLICATION_JSON_TYPE)
                        .entity(ImmutableMap.of("exception", e.getClass().getName(), "message", e.getMessage()))
                        .build());
    }
}
