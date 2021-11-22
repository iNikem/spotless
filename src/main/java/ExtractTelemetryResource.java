package io.omnition.traceanalysis.resources.telemetry;

import static io.omnition.traceanalysis.util.APIException.requireParameter;
import static io.omnition.traceanalysis.util.Nullsafe.firstNonNull;
import static java.util.Collections.emptySet;

import com.codahale.metrics.annotation.ExceptionMetered;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.omnition.traceanalysis.api.tracestore.PrestoTraceReader.PrestoSource;
import io.omnition.traceanalysis.db.presto.PrestoJdbiFactory;
import io.omnition.traceanalysis.resources.FriendlyDateTime;
import io.omnition.traceanalysis.resources.ResponseStream;
import io.omnition.traceanalysis.util.APIException;
import io.omnition.traceanalysis.util.CloudUtils;
import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;
import org.glassfish.jersey.server.ChunkedOutput;
import org.jdbi.v3.core.statement.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sf.account.subscriptioninfo.AccountSubscriptionInfoService;
import sf.id.ID;
import sf.id.IdUtil;

@Timed
@ExceptionMetered
@Path("/api/telemetry/extract")
public class ExtractTelemetryResource {

  private static final Logger logger = LoggerFactory.getLogger(ExtractTelemetryResource.class);

  private final AccountSubscriptionInfoService subscriptionInfoService;
  private final TelemetryConfig telemetryConfig;
  private final PrestoJdbiFactory jdbiFactory;
  private final ExecutorService executorService;

  public ExtractTelemetryResource(
      AccountSubscriptionInfoService subscriptionInfoService,
      TelemetryConfig telemetryConfig,
      PrestoJdbiFactory jdbiFactory,
      ExecutorService executorService) {
    this.subscriptionInfoService = subscriptionInfoService;
    this.telemetryConfig = telemetryConfig;
    this.jdbiFactory = jdbiFactory;
    this.executorService = executorService;
  }

  @POST
  @Produces(MediaType.TEXT_PLAIN)
  public ChunkedOutput<String> extract(Request request) {
    Sender sender = new Sender(telemetryConfig, request.token);
    ChunkedOutput<String> output = new ChunkedOutput<>(String.class);

    executorService.submit(
        () -> {
          ResponseStream responseStream = new ResponseStream(output);
          try {
            if (request.orgId != null) {
              extractTelemetry(request.orgId, request, responseStream, sender);
            } else {
              extractTelemetryForAllOrg(request, responseStream, sender);
            }
            responseStream.status("Extraction completed");
          } catch (Exception e) {
            responseStream.status("Extraction failed: %s", e.getMessage());
            logger.error("Extraction failed", e);
          } finally {
            responseStream.close();
          }
        });

    return output;
  }

  private void extractTelemetryForAllOrg(
      Request request, ResponseStream responseStream, Sender sender) {
    Set<ID> orgs = subscriptionInfoService.getOrgsWithEntitlements(emptySet(), emptySet());

    int count = 0;
    for (ID orgId : orgs) {
      responseStream.status("Processing org %s", IdUtil.id2Str(orgId));
      extractTelemetry(orgId, request, responseStream, sender);
      count++;
      responseStream.status("Finished processing %,d of %,d orgs", count, orgs.size());
    }
  }

  private void extractTelemetry(
      ID orgId, Request request, ResponseStream responseStream, Sender sender) {
    jdbiFactory.useHandle(
        orgId,
        PrestoSource.REPORTING.toString(),
        CloudUtils.TRACES_TABLE_NAME,
        () -> responseStream.status("No table found: check for typos in org id"),
        handle -> {
          ZonedDateTime date = request.start.getDateTime();
          long startMillis = System.currentTimeMillis();
          long total = 0;

          while (date.isBefore(request.end.getDateTime())) {
            ZonedDateTime queryDate = date;
            date = date.plusMinutes(1);

            ResultConsumer resultConsumer = new ResultConsumer(request, responseStream, sender);
            String queryTemplate =
                "SELECT"
                    + " analyze_trace(trace_binary, 'telemetry_tags', '') as spans"
                    + " FROM \"%s\".%s "
                    + " WHERE  y=:year AND mo=:month AND d=:day AND h=:hour AND m=:minute";

            Query query =
                handle
                    .createQuery(
                        String.format(queryTemplate, orgId.id, CloudUtils.TRACES_TABLE_NAME))
                    .bind("year", queryDate.get(ChronoField.YEAR))
                    .bind("month", queryDate.get(ChronoField.MONTH_OF_YEAR))
                    .bind("day", queryDate.get(ChronoField.DAY_OF_MONTH))
                    .bind("hour", queryDate.get(ChronoField.HOUR_OF_DAY))
                    .bind("minute", queryDate.get(ChronoField.MINUTE_OF_HOUR));

            query.map((resultSet, context) -> resultSet.getString("spans")).forEach(resultConsumer);

            resultConsumer.flush();
            total += resultConsumer.count;
            responseStream.status(
                "Finished processing %,d rows (%,d total, %,d rows/second) in %s",
                resultConsumer.count,
                total,
                1000L * total / (System.currentTimeMillis() - startMillis),
                queryDate);
          }
        });
  }

  private static class ResultConsumer implements Consumer<String> {
    private static final ObjectMapper mapper =
        new ObjectMapper().configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
    private final Request request;
    private final ResponseStream responseStream;
    private final Sender sender;
    private int count = 0;
    private final Set<String> rows = new HashSet<>();

    public ResultConsumer(Request request, ResponseStream responseStream, Sender sender) {
      this.request = request;
      this.responseStream = responseStream;
      this.sender = sender;
    }

    @Override
    public void accept(String row) {
      for (String json : row.split("\n")) {
        // Different strings may actually represent the same json object (e.g. just the order of
        // keys in the string is different). Convert the strings to Map<String,String> and
        // then serializes them back to strings but this time with a stable key order to remove any
        // duplicates.
        try {
          json = mapper.writeValueAsString(mapper.readValue(json, Map.class));
          rows.add(json);
        } catch (IOException e) {
          logger.warn("Failed to read json value returned from Presto", e);
        }
      }
      count++;
    }

    public void flush() {
      if (request.dryRun) {
        return;
      }

      long start = System.currentTimeMillis();
      sender.send(rows, responseStream);
      responseStream.status(
          "%,d rows flushed in %,d ms", rows.size(), System.currentTimeMillis() - start);
    }
  }

  public static class Request {
    // TODO replace with proper secret
    public final @JsonProperty("token") String token;
    public final @JsonProperty("dryRun") boolean dryRun;
    public final @JsonProperty("start") FriendlyDateTime start;
    public final @JsonProperty("end") FriendlyDateTime end;
    public final @JsonProperty("orgId") ID orgId;

    @JsonCreator
    public Request(
        @JsonProperty("token") String token,
        @JsonProperty("dryRun") Boolean dryRun,
        @JsonProperty("start") FriendlyDateTime start,
        @JsonProperty("end") FriendlyDateTime end,
        @JsonProperty("orgId") ID orgId) {
      this.token = requireParameter(token, "token");
      this.dryRun = firstNonNull(dryRun, false);
      this.start = requireParameter(start, "start");
      this.end = requireParameter(end, "end");
      this.orgId = orgId;

      if (!start.getDateTime().isBefore(end.getDateTime())) {
        throw APIException.userVisible(Status.BAD_REQUEST, "start must come before end");
      }
    }
  }
}
