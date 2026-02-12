package io.patchfox.orchestrate_service.scheduled;

import java.util.List;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.zip.DataFormatException;
import java.io.IOException;
import java.net.URI;
import java.time.ZonedDateTime;

import org.apache.el.stream.Optional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.patchfox.db_entities.entities.Dataset;
import io.patchfox.db_entities.entities.DatasetMetrics;
import io.patchfox.db_entities.entities.Datasource;
import io.patchfox.db_entities.entities.DatasourceEvent;
import io.patchfox.orchestrate_service.components.EnvironmentComponent;
import io.patchfox.orchestrate_service.kafka.KafkaBeans;
import io.patchfox.orchestrate_service.repositories.DatasetMetricsRepository;
import io.patchfox.orchestrate_service.repositories.DatasetRepository;
import io.patchfox.orchestrate_service.repositories.DatasourceEventRepository;
import io.patchfox.orchestrate_service.repositories.DatasourceRepository;
import io.patchfox.package_utils.data.pkg.PackageWrapper;
import io.patchfox.package_utils.json.ApiRequest;
import io.patchfox.package_utils.json.ApiRequest.ApiRequestBuilder;
import io.patchfox.package_utils.util.ApiDataHelpers;
import jakarta.transaction.Transactional;
import lombok.AllArgsConstructor;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;


@Slf4j
//@EnableAsync
@Component
public class Peristalsis {

    @Autowired
    EnvironmentComponent env;

    @Autowired
    KafkaBeans kafka;

    @Autowired
    DatasetRepository datasetRepository;

    @Autowired
    DatasourceRepository datasourceRepository;

    @Autowired
    DatasourceEventRepository datasourceEventRepository;

    @Autowired
    DatasetMetricsRepository datasetMetricsRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;
    

    /**
     * 
     * @throws StreamReadException
     * @throws DatabindException
     * @throws IOException
     * @throws DataFormatException
     * @throws InterruptedException
     */
    //@Async
    @Scheduled(fixedRate = 2, timeUnit = TimeUnit.MINUTES)
    //@Synchronized // <-- this might not be necessary given how @Scheduled works
    public void propagate() throws StreamReadException, DatabindException, IOException, DataFormatException, InterruptedException {
        if ( !env.isPeristalsisActivated()) {
            log.debug("peristalsis is not activated - skipping propagate()");
            return;
        }

        checkStartEnrichment();  // Phase 1: Start OSS enrichment (grype)
        checkDoneOssEnrichment(); // Phase 2: Check if OSS done, start package-index enrichment
        checkDoneEnrichment();    // Phase 3: Check if all enrichment done
        checkDoneAnalyze();
        //checkDoneForecast();
        //checkDoneRecommend();
    }


    /**
     * @throws DataFormatException
     * @throws IOException
     * @throws DatabindException
     * @throws StreamReadException
     *
     */
    @Transactional
    public void checkStartEnrichment() throws StreamReadException, DatabindException, IOException, DataFormatException {

        log.info("running checkStartEnrichment...");

        // don't do this here
        //
        // // in case it's not already there we're going to want an index on commit_datetime
        // datasourceEventRepository.createDatasourceEventCommitDatetimeIndex();

        // find all datasets ready for processing - fetch IDs only to avoid memory bloat
        List<Long> datasetIds = jdbcTemplate.queryForList(
            "SELECT id FROM dataset WHERE status = ?",
            Long.class,
            Dataset.Status.READY_FOR_PROCESSING.name()
        );

        for (var ds : datasetRepository.findAll()) {
            log.info("dataset: {}  status: {}", ds.getName(), ds.getStatus());
        }

        // nothing to do if there's nothing ready for processing
        if (datasetIds.isEmpty()) {
            log.info("nothing to process at this time");
            log.info("checkStartEnrichment done");
            return;
        }

        //
        // TODO
        // this is gonna be a problem later for cases where two datasets have the same datasource and both datasets
        // need to be processed
        //

        var jobId = UUID.randomUUID();
        log.info("using jobId: {}", jobId);
        
        // Batch update datasets
        var now = java.sql.Timestamp.from(ZonedDateTime.now().toInstant());
        jdbcTemplate.batchUpdate(
            "UPDATE dataset SET status = 'PROCESSING', latest_job_id = ?, updated_at = ? WHERE id = ?",
            datasetIds,
            datasetIds.size(),
            (ps, datasetId) -> {
                ps.setObject(1, jobId);
                ps.setTimestamp(2, now);
                ps.setLong(3, datasetId);
            }
        );
        
        // Process datasets one at a time to avoid memory bloat
        for (Long datasetId : datasetIds) {
            Dataset dataset = datasetRepository.findById(datasetId).orElseThrow();
            log.info("processing oss enrichment for dataset: {}", dataset.getName());
            
            // Fetch datasources via JDBC instead of Hibernate
            var datasources = fetchDatasourcesForDataset(datasetId);

            for (var datasource : datasources) {
                if (datasource.getStatus().equals(Datasource.Status.PROCESSING_ERROR)) {
                    log.info("skipping datasource: {} due to status of PROCESSING_ERROR", datasource);
                    continue;
                }

                // Update datasource status in DB
                jdbcTemplate.update(
                    "UPDATE datasource SET status = 'PROCESSING', latest_job_id = ? WHERE id = ?",
                    jobId,
                    datasource.getId()
                );

                log.info("running enrichment for datasource: {}", datasource.getPurl());
                var datasourceId = datasource.getId();

                var oneOrNone = jdbcTemplate.query(
                    """
                    SELECT id FROM datasource_event 
                    WHERE datasource_id = ? 
                    AND status = 'READY_FOR_PROCESSING' 
                    AND oss_enriched = false 
                    ORDER BY commit_date_time ASC 
                    LIMIT 1
                    """,
                    (rs, rowNum) -> rs.getLong("id"),
                    datasourceId
                );

                List<Long> eventIds = new ArrayList<>();

                if (oneOrNone.isEmpty()) {
                    var latestEvents = jdbcTemplate.query(
                        "SELECT id FROM datasource_event WHERE datasource_id = ? ORDER BY commit_date_time DESC LIMIT 1",
                        (rs, rowNum) -> rs.getLong("id"),
                        datasourceId
                    );
                    if (!latestEvents.isEmpty()) {
                        eventIds.add(latestEvents.get(0));
                    }
                } else {
                    var earliestEventId = oneOrNone.get(0);
                    var earliestCommitDateTime = jdbcTemplate.queryForObject(
                        "SELECT commit_date_time FROM datasource_event WHERE id = ?",
                        (rs, rowNum) -> rs.getTimestamp("commit_date_time"),
                        earliestEventId
                    );
                    
                    eventIds = jdbcTemplate.query(
                        """
                        SELECT id FROM datasource_event 
                        WHERE datasource_id = ? 
                        AND commit_date_time >= ? 
                        ORDER BY commit_date_time ASC
                        """,
                        (rs, rowNum) -> rs.getLong("id"),
                        datasourceId,
                        earliestCommitDateTime
                    );
                }

                if (!eventIds.isEmpty()) {
                    // Batch update events
                    jdbcTemplate.batchUpdate(
                        """
                        UPDATE datasource_event 
                        SET job_id = ?, 
                            oss_enriched = false, 
                            package_index_enriched = false, 
                            analyzed = false, 
                            forecasted = false, 
                            recommended = false, 
                            status = 'PROCESSING' 
                        WHERE id = ?
                        """,
                        eventIds,
                        eventIds.size(),
                        (ps, eventId) -> {
                            ps.setObject(1, jobId);
                            ps.setLong(2, eventId);
                        }
                    );
                }

                for (var eventId : eventIds) {
                    var datasourceEvent = datasourceEventRepository.findById(eventId).orElseThrow();
                    log.info("running oss enrichment for datasourceEvent: {}", datasourceEvent.getPurl());

                    //var mapper = new ObjectMapper().findAndRegisterModules();
                    //var p = mapper.readValue(datasourceEvent.getPayload(), PackageWrapper.class);

                    // send event to grype service 
                    var grypeOssMessage = ApiRequest.builder()
                                                    .txid(jobId) 
                                                    .verb(ApiRequest.httpVerb.POST)
                                                    .uri(URI.create("/api/v1/grype"))
                                                    .queryStringParameters(
                                                        Map.of(
                                                            "recordResults", "true",
                                                            "altTxid", datasourceEvent.getTxid().toString(),
                                                            "datasourceEventRecordId", Long.toString(datasourceEvent.getId())
                                                        )
                                                    )
                                                    .responseTopicName(env.getKafkaResponseTopicName()) 
                                                    .build();

                    String partitionKey = datasourceEvent.getId().toString();
                    kafka.makeRequest("grype-service_REQUEST", partitionKey, grypeOssMessage);
                }
            }  
        }

        log.info("checkStartEnrichment done");
    }


    /**
     * Check if OSS enrichment (grype) is complete, then start package-index enrichment
     */
    @Transactional
    public void checkDoneOssEnrichment() {
        log.info("running checkDoneOssEnrichment...");

        // find all datasets currently being processed - use JDBC to avoid loading full entities
        String sql = "SELECT id, name, latest_job_id, status FROM dataset WHERE status = ?";
        List<Dataset> datasets = jdbcTemplate.query(sql, (rs, rowNum) -> {
            Dataset ds = new Dataset();
            ds.setId(rs.getLong("id"));
            ds.setName(rs.getString("name"));
            ds.setLatestJobId((UUID) rs.getObject("latest_job_id"));
            ds.setStatus(Dataset.Status.valueOf(rs.getString("status")));
            return ds;
        }, Dataset.Status.PROCESSING.name());

        if (datasets.isEmpty()) {
            log.info("no datasets in PROCESSING state at this time.");
            log.info("checkDoneOssEnrichment done");
            return;
        }

        for (var dataset : datasets) {
            log.info("checking OSS enrichment status for dataset: {}", dataset.getName());
            var jobId = dataset.getLatestJobId();

            var totalEventCount = datasourceEventRepository.countByJobId(jobId);
            var processingErrorCount =
                datasourceEventRepository.countByJobIdAndStatus(jobId, DatasourceEvent.Status.PROCESSING_ERROR);

            var ossEnrichedCount =
                datasourceEventRepository.countByJobIdAndStatusAndOssEnrichedTrue(
                    jobId,
                    DatasourceEvent.Status.READY_FOR_NEXT_PROCESSING
                );

            log.info("OSS enrichment progress for dataset {}: total={}, oss_enriched={}, errors={}",
                dataset.getName(), totalEventCount, ossEnrichedCount, processingErrorCount);

            // Check if all events are either OSS enriched or in error state
            if (totalEventCount == 0 || !(totalEventCount == (processingErrorCount + ossEnrichedCount))) {
                log.info("OSS enrichment not yet complete for dataset: {}", dataset.getName());
                continue;
            }

            log.info("OSS enrichment complete for dataset: {}", dataset.getName());
            log.info("Starting package-index enrichment for dataset: {}", dataset.getName());

            // Get all events that are OSS enriched and ready for package-index enrichment
            // Use JDBC to avoid loading massive payload field into memory
            String eventSql = """
                SELECT id, purl 
                FROM datasource_event 
                WHERE job_id = ? 
                AND status = ? 
                AND oss_enriched = true 
                AND package_index_enriched = false
            """;
            
            var eventsReadyForPackageIndex = jdbcTemplate.query(eventSql, (rs, rowNum) -> {
                var event = new DatasourceEvent();
                event.setId(rs.getLong("id"));
                event.setPurl(rs.getString("purl"));
                return event;
            }, jobId, DatasourceEvent.Status.READY_FOR_NEXT_PROCESSING.name());

            log.info("Found {} events ready for package-index enrichment", eventsReadyForPackageIndex.size());

            for (var datasourceEvent : eventsReadyForPackageIndex) {
                log.info("sending datasourceEvent {} for package-index enrichment", datasourceEvent.getPurl());

                var packageIndexMessage = ApiRequest.builder()
                                                    .txid(jobId)
                                                    .verb(ApiRequest.httpVerb.POST)
                                                    .uri(URI.create("/api/v1/enrichPackages"))
                                                    .queryStringParameters(
                                                        Map.of(
                                                            "datasourceEventRecordId", Long.toString(datasourceEvent.getId())
                                                        )
                                                    )
                                                    .responseTopicName(env.getKafkaResponseTopicName())
                                                    .build();

                String partitionKey = datasourceEvent.getId().toString();
                kafka.makeRequest("package-index-service_REQUEST", partitionKey, packageIndexMessage);
            }
        }

        log.info("checkDoneOssEnrichment done");
    }


    /**
     *
     */
    @Transactional
    public void checkDoneEnrichment() {
        log.info("running checkDoneEnrichment...");

        // find all datasets ready for processing - use JDBC to avoid loading full entities
        String sql = "SELECT id, name, latest_job_id, status FROM dataset WHERE status = ?";
        List<Dataset> datasets = jdbcTemplate.query(sql, (rs, rowNum) -> {
            Dataset ds = new Dataset();
            ds.setId(rs.getLong("id"));
            ds.setName(rs.getString("name"));
            ds.setLatestJobId((UUID) rs.getObject("latest_job_id"));
            ds.setStatus(Dataset.Status.valueOf(rs.getString("status")));
            return ds;
        }, Dataset.Status.PROCESSING.name());

        // Log all datasets - use JDBC to avoid Hibernate relationship loading
        jdbcTemplate.query("SELECT name, status FROM dataset", (rs, rowNum) -> {
            log.info("dataset: {}  status: {}", rs.getString("name"), rs.getString("status"));
            return null;
        });

        // nothing to do if there's nothing ready for processing 
        if (datasets.isEmpty()) { 
            log.info("no datasets in state {} at this time.", Dataset.Status.PROCESSING);
            log.info("checkDoneEnrichment done");
            return; 
        }

        // go through every datasourceEvent in every constituent datasource to see if oss enrichment was 
        // completed for all events currently being processed 
        for (var dataset : datasets) {
            log.info("checking dataset: {}", dataset.getName());
            var jobId = dataset.getLatestJobId();
        
            var totalEventCount = datasourceEventRepository.countByJobId(jobId);
            var processingErrorCount = 
                datasourceEventRepository.countByJobIdAndStatus(jobId, DatasourceEvent.Status.PROCESSING_ERROR);
            
            var readyForAnalyzeCount = 
                datasourceEventRepository.countByJobIdAndStatusAndOssEnrichedTrueAndPackageIndexEnrichedTrueAndAnalyzedFalse(
                    jobId, 
                    DatasourceEvent.Status.READY_FOR_NEXT_PROCESSING
                );

            // Skip if not all events are ready
            if (!(totalEventCount == (processingErrorCount + readyForAnalyzeCount)) ) {
                continue;
            }

            /*
             * 
             * I know this looks fucky. here's why we are doing this. sometimes build files are updated in the same 
             * commit and thus have the same commit_datetime value. that screws with how analyze-service tabulates 
             * data. what we're doing is updating the records we're sending to analyze service by miliseconds to ensure
             * every event has a unique commit_datetime value. 
             * 
             * once we do that - we double check that the list is still sorted ASC by commit_datetime 
             * 
             */
            var datasourceEventIdsReadyForAnalyze = datasourceEventRepository.getDatasourceEventIdsReadyForAnalyze(jobId);
            var eventIdsString = listToSqlArrayString(datasourceEventIdsReadyForAnalyze);
            // Call the deduplication procedure on commitdatetime. it matters to analyze that this happens 
            datasourceEventRepository.deduplicateCommitDatetimes(eventIdsString);

            // Get the reordered list. doing this in two steps because hibernate can be a salty butthole re: returning
            // list values of large quantities 
            datasourceEventIdsReadyForAnalyze = datasourceEventRepository.getReorderedEventIds(datasourceEventIdsReadyForAnalyze);
            /*
             * 
             * 
             * 
             */


            log.info("enrichment complete for dataset: {}", dataset.getName());
            log.info("sending dataset: {} for analysis", dataset.getName());
            log.debug("sending index payload: {}", datasourceEventIdsReadyForAnalyze);

            // Use JDBC to avoid transaction propagation issues with @Modifying queries
            jdbcTemplate.update(
                """
                UPDATE datasource_event
                SET status = 'PROCESSING'
                WHERE oss_enriched = true
                AND package_index_enriched = true
                AND analyzed = false
                AND status = 'READY_FOR_NEXT_PROCESSING'
                AND job_id = ?
                """,
                jobId
            );

            // Use JDBC to avoid transaction propagation issues with @Modifying queries
            jdbcTemplate.update(
                """
                UPDATE datasource
                SET status = 'PROCESSING'
                WHERE status = 'READY_FOR_NEXT_PROCESSING'
                AND id IN (
                    SELECT datasource_id
                    FROM datasource_dataset
                    WHERE dataset_id = ?
                )
                """,
                dataset.getId()
            );

            var analyzeRequest = ApiRequest.builder()
                                            // for all internal pipeline requests that are job-related we 
                                            // use the jobID as the request txid so we can log-trace all the 
                                            // job things across all invoked services. 
                                            .txid(dataset.getLatestJobId()) 
                                            .responseTopicName(env.getKafkaResponseTopicName())
                                            .verb(ApiRequest.httpVerb.POST)
                                            .uri(URI.create("/api/v1/tabulate"))
                                            .queryStringParameters(
                                                Map.of(
                                                    "datasetName", dataset.getName(),
                                                    "pageIndex", "0",
                                                    "pageSize", Integer.toString(env.getAnalyzePageSize())
                                                )
                                            )
                                            .data(
                                                Map.of(
                                                    "datasourceEventIndexesByCommitDateAsc", 
                                                    datasourceEventIdsReadyForAnalyze
                                                )
                                            )
                                            .build();
                                            
            kafka.makeRequest("analyze-service_REQUEST", analyzeRequest);
            
            //}
        }

        log.info("checkDoneEnrichment done");
    }


    /**
     *
     */
    @Transactional
    public void checkDoneAnalyze() {
        log.info("running checkDoneAnalyze...");
        // find all datasets ready for processing - fetch IDs only
        List<Long> datasetIds = jdbcTemplate.queryForList(
            "SELECT id FROM dataset WHERE status = ?",
            Long.class,
            Dataset.Status.PROCESSING.name()
        );

        // Log all datasets - use JDBC to avoid Hibernate relationship loading
        jdbcTemplate.query("SELECT name, status FROM dataset", (rs, rowNum) -> {
            log.info("dataset: {}  status: {}", rs.getString("name"), rs.getString("status"));
            return null;
        });

        // nothing to do if there's nothing ready for processing 
        if (datasetIds.isEmpty()) { 
            log.info("no datasets in state {} at this time.", Dataset.Status.PROCESSING);
            log.info("checkDoneAnalyze done");
            return; 
        }

        // go through every datasourceEvent in every constituent datasource to see if oss enrichment was 
        // completed for all events currently being processing 
        for (Long datasetId : datasetIds) {
            Dataset dataset = datasetRepository.findById(datasetId).orElseThrow();
            log.info("checking dataset: {}", dataset.getName());
            
            // Fetch datasources via JDBC instead of Hibernate
            var datasources = fetchDatasourcesForDataset(datasetId);

            var hasBeenEnrichedCount = 0;
            var readyForProcessingCount = 0;
            var datasourcesInErrorState = 0;
            //List<Long> datasourceEventIdsReadyForForecast = new ArrayList<Long>();

            var jobId = dataset.getLatestJobId();
            var datasourcesWithNoEventsInJob = 0;

            for (var datasource : datasources) {
                log.debug("checking analyzed for datasource: {}", datasource.getPurl());

                // this happens when a new datasource pushes data to the dataset while the dataset is processing 
                if (datasource.getStatus().equals(Datasource.Status.READY_FOR_PROCESSING)) {
                    readyForProcessingCount += 1;
                    continue;
                }

                if (datasource.getStatus().equals(Datasource.Status.PROCESSING_ERROR)) {
                    datasourcesInErrorState += 1;
                    continue;
                }

                var datasourcePurl = datasource.getPurl();

                var countByDatasourceEventsInProcessing = 
                    datasourceEventRepository.countByDatasourcePurlAndJobIdAndStatus(
                        datasourcePurl,
                        jobId,
                        DatasourceEvent.Status.PROCESSING  
                    );

                var countByDatasourceEventsInProcessingReadyForForecast = 
                    datasourceEventRepository.countByDatasourcePurlAndJobIdAndStatusAndOssEnrichedTrueAndPackageIndexEnrichedTrueAndAnalyzedTrueAndForecastedFalse(
                        datasourcePurl,
                        jobId,
                        DatasourceEvent.Status.READY_FOR_NEXT_PROCESSING
                    );

                if (countByDatasourceEventsInProcessing == 0 && countByDatasourceEventsInProcessingReadyForForecast == 0) {
                    datasourcesWithNoEventsInJob += 1;
                    continue;
                }

                if (
                    countByDatasourceEventsInProcessing == 0
                    && countByDatasourceEventsInProcessingReadyForForecast > 0
                ) {
                    hasBeenEnrichedCount += 1;

                    //datasourceEventIdsReadyForForecast.addAll(datasourceEventIdsReadyForForecast);
                } 

            }

            // // these were sorted on a per datasource basis and we need them globally sorted
            // // TODO this is a shite way to do this... 
            // datasourceEventIdsReadyForForecast = 
            //     datasourceEventRepository.getDatasourceEventIdsOrderedByCommitDatetimeAsc(datasourceEventIdsReadyForForecast);

            // log.info("sorted datasourceEventIdsReadyForForecast size: {}", datasourceEventIdsReadyForForecast.size());

            // if all datasources currently being processed have been oss enriched send 
            if (hasBeenEnrichedCount == (datasources.size() - readyForProcessingCount - datasourcesInErrorState - datasourcesWithNoEventsInJob)) {
                log.info("analyze step complete for dataset: {}", dataset.getName());

                // Collect all datasource IDs that need to be updated
                List<Long> datasourceIdsToUpdate = new ArrayList<>();
                
                for (var datasource : datasources) {
                    if (
                        datasource.getStatus().equals(Datasource.Status.READY_FOR_PROCESSING)
                        || datasource.getStatus().equals(Datasource.Status.PROCESSING_ERROR)
                    ) { 
                        continue; 
                    }
                    datasourceIdsToUpdate.add(datasource.getId());
                }
                
                if (!datasourceIdsToUpdate.isEmpty()) {
                    try (var conn = jdbcTemplate.getDataSource().getConnection()) {
                        var eventArray = conn.createArrayOf("bigint", datasourceIdsToUpdate.toArray());
                        var datasourceArray = conn.createArrayOf("bigint", datasourceIdsToUpdate.toArray());
                        
                        // Bulk update all events for all datasources in one shot
                        int totalEventsUpdated = jdbcTemplate.update(
                            """
                            UPDATE datasource_event 
                            SET status = 'PROCESSED' 
                            WHERE datasource_id = ANY(?) 
                            AND status = 'READY_FOR_NEXT_PROCESSING'
                            """,
                            eventArray
                        );
                        
                        // Bulk update all datasources to IDLE
                        int datasourcesUpdated = jdbcTemplate.update(
                            """
                            UPDATE datasource 
                            SET status = 'IDLE' 
                            WHERE id = ANY(?)
                            """,
                            datasourceArray
                        );
                        
                        log.info("Bulk update complete: {} datasources marked IDLE, {} events marked PROCESSED", 
                            datasourcesUpdated, totalEventsUpdated);
                    } catch (Exception e) {
                        log.error("Error during bulk update", e);
                    }
                }

                if (dataset.getStatus() != Dataset.Status.PROCESSING_ERROR) {
                    log.info("marking dataset: {} as: {}", dataset.getName(), Dataset.Status.IDLE);
                    jdbcTemplate.update(
                        "UPDATE dataset SET status = 'IDLE', updated_at = NOW() WHERE id = ?",
                        dataset.getId()
                    );
                }

                // send event to forecast service 
                // var forecastMessage = ApiRequest.builder()
                //                                 // for all internal pipeline requests that are job-related we 
                //                                 // use the jobID as the request txid so we can log-trace all the 
                //                                 // job things across all invoked services. 
                //                                 .txid(dataset.getLatestJobId()) 
                //                                 .verb(ApiRequest.httpVerb.POST)
                //                                 .uri(URI.create("/api/v1/forecast"))
                //                                 .responseTopicName(env.getKafkaResponseTopicName()) 
                //                                 .build();


                // kafka.makeRequest("forecast-service_REQUEST", forecastMessage);
                
            } 

        }
        log.info("checkDoneAnalyze done");
    }


    /**
     *
     */
    @Transactional
    public void checkDoneForecast() {
        log.info("running checkDoneForecast...");
        // find all datasets ready for processing - use JDBC to avoid loading full entities
        String sql = "SELECT id, name, latest_job_id, status FROM dataset WHERE status = ?";
        List<Dataset> datasets = jdbcTemplate.query(sql, (rs, rowNum) -> {
            Dataset ds = new Dataset();
            ds.setId(rs.getLong("id"));
            ds.setName(rs.getString("name"));
            ds.setLatestJobId((UUID) rs.getObject("latest_job_id"));
            ds.setStatus(Dataset.Status.valueOf(rs.getString("status")));
            return ds;
        }, Dataset.Status.PROCESSING.name());

        // nothing to do if there's nothing ready for processing 
        if (datasets.isEmpty()) { 
            log.info("no datasets in state {} at this time.", Dataset.Status.PROCESSING);
            log.info("checkDoneForecast done");
            return; 
        }

        // go through every datasourceEvent in every constituent datasource to see if oss enrichment was 
        // completed for all events currently being processed 
        for (var dataset : datasets) {
            var jobId = dataset.getLatestJobId();

            if (dataset.getStatus().equals(Dataset.Status.PROCESSING_ERROR)) {
                log.warn(
                    "dataset: {} with jobId: {} reports PROCESSING_ERROR", 
                    dataset.getName(), 
                    dataset.getLatestJobId()
                );
                continue;
            }

            var mostRecentDatasourceEventTxidOptional = datasourceEventRepository.getMostRecentCommitTxidByJobId(jobId);
            if (mostRecentDatasourceEventTxidOptional.isEmpty()) {
                log.warn("unexpectedly found no datasourceEvent records associated with jobId: {}", jobId);
                continue;
            }

            var mostRecentDatasourceEventTxid = mostRecentDatasourceEventTxidOptional.get();

            // Use JDBC to avoid loading massive package_indexes arrays and edits collections
            String metricsSql = "SELECT id, txid FROM dataset_metrics WHERE job_id = ?";
            var dsmrsWithJobId = jdbcTemplate.query(metricsSql, (rs, rowNum) -> {
                DatasetMetrics dm = new DatasetMetrics();
                dm.setId(rs.getLong("id"));
                dm.setTxid(UUID.fromString(rs.getString("txid")));
                return dm;
            }, jobId);
            
            var fsmrsWithDseTxid = dsmrsWithJobId.stream()
                                                 .filter(dsmr -> dsmr.getTxid().equals(mostRecentDatasourceEventTxid))
                                                 .toList();            

            // there will be [n] of dataset_metrics records that share the same jobID. 
            // what we are looking for are cases where two of them share the same txid. if that happened it means 
            // databricks has created a forecast record but there is not recommend record yet (which will also share the 
            // same txid - making the count three and not two).
            // 
            // if there's less than two it means there's nothing for us to do here - ie - nothing to send to recommend
            // if there's more than two it means we've already sent this datsetmetrics record to recommend
            if ( fsmrsWithDseTxid.size() != 2 ) { continue; }

            log.info("forecast step complete for dataset: {}", dataset.getName());
            // this is to prevent us from continuously sending work to recommend 
           
            // this call is taking a gazillion years and probably is hibernate related somehow
            //var dseSentToRecommend = datasourceEventRepository.getDatasourceEventIdsSentToRecommend(jobId); 
            
            // this call uses jdbctemplate which should be a lot faster 
            var dseSentToRecommend = getDatasourceEventIdsSentToRecommend(jobId);
            var sendRequest = dseSentToRecommend.isEmpty() ? true : false;

            // for (var datasource : dataset.getDatasources()) {
            //     if (datasource.getStatus().equals(Datasource.Status.INGESTING)) { continue; } 
            //     if (datasource.getStatus().equals(Datasource.Status.PROCESSING_ERROR)) { continue; } 
            //     if (datasource.getStatus().equals(Datasource.Status.READY_FOR_PROCESSING)) { continue; }   
            //     // if (datasource.getStatus().equals(Datasource.Status.PROCESSING)) { continue; }      

            //     // log.info("marking datasource: {} as: {}", datasource.getPurl(), Datasource.Status.PROCESSING);
            //     // datasource.setStatus(Datasource.Status.PROCESSING);
            //     // datasourceRepository.save(datasource); 
            //     sendRequest = true;
            // } 


            //
            // ** IN CASE YOU NEED TO TERMINATE PROCESSING AFTER FORECAST STEP **
            //
            // for (var datasource : dataset.getDatasources()) {
            //     if (datasource.getStatus().equals(Datasource.Status.READY_FOR_PROCESSING)) { continue; }               

            //     log.info("marking datasource: {} as: {}", datasource.getPurl(), Datasource.Status.IDLE);
            //     datasource.setStatus(Datasource.Status.IDLE);
            //     datasourceRepository.save(datasource); 

            // } 

            // if (dataset.getStatus() != Dataset.Status.PROCESSING_ERROR) {
            //     log.info("marking dataset: {} as: {}", dataset.getName(), Dataset.Status.IDLE);
            //     dataset.setStatus(Dataset.Status.IDLE);
            //     datasetRepository.save(dataset);
            // }


            // send event to recommend service 
            if (sendRequest) {

                log.info("marking all events associated with jobId: {} as forecasted", jobId);
                
                // calling this through hibernate is taking a gazillion years 
                //datasourceEventRepository.callUpdateDatasourceEventsProcessingStatus(jobId);

                callUpdateDatasourceEventsProcessingStatus(jobId);

                // // databricks does not manage the datasource event records. if there is new completed forecasting work we 
                // // need to curate the datasourceevent records accordingly 
                // var datasourceEventRecords = datasourceEventRepository.findAllByJobId(jobId);
                // for (var dser : datasourceEventRecords) {
                //     if ( !dser.getStatus().equals(DatasourceEvent.Status.PROCESSING_ERROR) ) {
                //         dser.setForecasted(true);
                //         dser.setStatus(DatasourceEvent.Status.PROCESSING);
                //     }
                // }
    
                // datasourceEventRecords = datasourceEventRepository.saveAll(datasourceEventRecords);

                var recommendMessage = ApiRequest.builder()
                                                 // for all internal pipeline requests that are job-related we 
                                                 // use the jobID as the request txid so we can log-trace all the 
                                                 // job things across all invoked services. 
                                                 .txid(dataset.getLatestJobId()) 
                                                 .verb(ApiRequest.httpVerb.POST)
                                                 .uri(URI.create("/api/v1/recommend"))
                                                 .responseTopicName(env.getKafkaResponseTopicName()) 
                                                 .build();


                kafka.makeRequest("recommend-service_REQUEST", recommendMessage);
            }


        }

        log.info("checkDoneForecast done");
    }

    @Transactional
    public void checkDoneRecommend() {
        log.info("running checkDoneRecommend...");
        List<Long> datasetIds = jdbcTemplate.queryForList(
            "SELECT id FROM dataset WHERE status = ?",
            Long.class,
            Dataset.Status.PROCESSING.name()
        );


        // nothing to do if there's nothing ready for processing 
        if (datasetIds.isEmpty()) { 
            log.info("no datasets in state {} at this time.", Dataset.Status.PROCESSING);
            log.info("checkDoneRecommend done");
            return; 
        }

        for (Long datasetId : datasetIds) {
            Dataset dataset = datasetRepository.findById(datasetId).orElseThrow();
            var jobId = dataset.getLatestJobId();

            if (dataset.getStatus().equals(Dataset.Status.PROCESSING_ERROR)) {
                log.warn(
                    "dataset: {} with jobId: {} reports PROCESSING_ERROR", 
                    dataset.getName(), 
                    dataset.getLatestJobId()
                );

                continue;
            }

            // Use JDBC to avoid loading massive package_indexes arrays and edits collections
            String sql = "SELECT id, txid FROM dataset_metrics WHERE job_id = ?";
            var dsmrsWithJobId = jdbcTemplate.query(sql, (rs, rowNum) -> {
                DatasetMetrics dm = new DatasetMetrics();
                dm.setId(rs.getLong("id"));
                dm.setTxid(UUID.fromString(rs.getString("txid")));
                return dm;
            }, jobId);
            
            var groupedByTxid = dsmrsWithJobId.stream()
                                              .collect(
                                                Collectors.groupingBy(DatasetMetrics::getTxid)
                                              );

            

            // there will be [n] of dataset_metrics records that share the same jobID. 
            // what we are looking for are cases where two of them share the same txid. if that happened it means 
            // databricks has created a forecast record but there is not recommend record yet (which will also share the 
            // same txid - making the count three and not two).
            // 
            // there should be 
            // 
            // + 1 record indicating current state
            // + 1 record from databricks indicating projected future state
            // + 10 records indicating recommendations along ten business goals
            // = 12 total records 
            // @TODO this needs to be a config variable so when the number of recommendation types changes 
            //       this service can be updated without code changees 
            var gtg = false;
            for (var v : groupedByTxid.values()) {
                if (v.size() == 12) { gtg = true; }
            }
            if ( !gtg ) { continue; }

            log.info("recommend step complete for dataset: {}", dataset.getName());

            // var datasourceEventRecords = datasourceEventRepository.findAllByJobId(jobId);
            // log.info("marking all events associated with jobId: {} as processed", jobId);
            // datasourceEventRecords.stream()
            //                       .filter(dse -> dse.getStatus().equals(DatasourceEvent.Status.PROCESSING))
            //                       .forEach(dse -> dse.setStatus(DatasourceEvent.Status.PROCESSED));

            // datasourceEventRepository.saveAll(datasourceEventRecords);

            log.info("marking all events associated with jobId: {} as processed", jobId);
            jdbcTemplate.execute("CALL update_datasource_events_processing_completed_status(?)",
                (java.sql.PreparedStatement ps) -> {
                    ps.setObject(1, jobId);
                    return ps.execute();
                });

            // Collect datasource IDs to update - fetch via JDBC
            List<Long> datasourceIdsToUpdate = new ArrayList<>();
            var datasources = fetchDatasourcesForDataset(datasetId);
            for (var datasource : datasources) {
                if (datasource.getStatus().equals(Datasource.Status.PROCESSING)) {
                    datasourceIdsToUpdate.add(datasource.getId());
                }
            }
            
            if (!datasourceIdsToUpdate.isEmpty()) {
                try (var conn = jdbcTemplate.getDataSource().getConnection()) {
                    var datasourceArray = conn.createArrayOf("bigint", datasourceIdsToUpdate.toArray());
                    
                    // Bulk update all datasources to IDLE
                    int datasourcesUpdated = jdbcTemplate.update(
                        """
                        UPDATE datasource 
                        SET status = 'IDLE' 
                        WHERE id = ANY(?)
                        """,
                        datasourceArray
                    );
                    
                    log.info("Bulk update complete: {} datasources marked IDLE", datasourcesUpdated);
                } catch (Exception e) {
                    log.error("Error during bulk datasource update", e);
                }
            }

            if (dataset.getStatus() != Dataset.Status.PROCESSING_ERROR) {
                log.info("marking dataset: {} as: {}", dataset.getName(), Dataset.Status.IDLE);
                jdbcTemplate.update(
                    "UPDATE dataset SET status = 'IDLE', updated_at = NOW() WHERE id = ?",
                    dataset.getId()
                );
            }

        }


        log.info("checkDoneRecommend done");
    }


    /**
     * 
     */
    public String listToSqlArrayString(List<Long> l) {
        return l.toString()
                .replace("[", "")
                .replace("]", "")
                .replace(" ", "");
    }



    public List<Long> getDatasourceEventIdsSentToRecommend(UUID jobId) {
        String sql = """
            SELECT id
            FROM datasource_event
            WHERE oss_enriched = true
            AND package_index_enriched = true
            AND analyzed = true
            AND forecasted = true
            AND status = 'PROCESSING'
            AND job_id = ?
            """;
        
        return jdbcTemplate.queryForList(sql, Long.class, jobId);
    }


    @Transactional
    public void callUpdateDatasourceEventsProcessingStatus(UUID jobId) {
        jdbcTemplate.execute("CALL update_datasource_events_processing_status(?)",
            (java.sql.PreparedStatement ps) -> {
                ps.setObject(1, jobId);
                return ps.execute();
            });
    }

    /**
     * Helper method to fetch datasources for a dataset using JDBC to avoid Hibernate memory bloat
     */
    private List<Datasource> fetchDatasourcesForDataset(Long datasetId) {
        String sql = """
            SELECT d.id, d.purl, d.status, d.first_event_received_at, d.last_event_received_at
            FROM datasource d
            JOIN datasource_dataset dd ON d.id = dd.datasource_id
            WHERE dd.dataset_id = ?
        """;
        
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            Datasource ds = new Datasource();
            ds.setId(rs.getLong("id"));
            ds.setPurl(rs.getString("purl"));
            ds.setStatus(Datasource.Status.valueOf(rs.getString("status")));
            // Set timestamps to avoid NPE if accessed (using OffsetDateTime -> ZonedDateTime)
            var firstReceived = rs.getObject("first_event_received_at", java.time.OffsetDateTime.class);
            var lastReceived = rs.getObject("last_event_received_at", java.time.OffsetDateTime.class);
            if (firstReceived != null) ds.setFirstEventReceivedAt(firstReceived.toZonedDateTime());
            if (lastReceived != null) ds.setLastEventReceivedAt(lastReceived.toZonedDateTime());
            return ds;
        }, datasetId);
    }


}
