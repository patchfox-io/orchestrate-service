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
    @Transactional
    public void propagate() throws StreamReadException, DatabindException, IOException, DataFormatException, InterruptedException {
        if ( !env.isPeristalsisEnabled()) {
            return;
        }
        
        checkStartEnrichment();
        checkDoneEnrichment();
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
    public void checkStartEnrichment() throws StreamReadException, DatabindException, IOException, DataFormatException {

        log.info("running checkStartEnrichment...");

        // don't do this here
        //
        // // in case it's not already there we're going to want an index on commit_datetime
        // datasourceEventRepository.createDatasourceEventCommitDatetimeIndex();

        // find all datasets ready for processing 
        List<Dataset> datasets = datasetRepository.findAllByStatus(Dataset.Status.READY_FOR_PROCESSING);

        for (var ds : datasetRepository.findAll()) {
            log.info("dataset: {}  status: {}", ds.getName(), ds.getStatus());
        }

        // nothing to do if there's nothing ready for processing 
        if (datasets.isEmpty()) { 
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
        for (var dataset : datasets) {
            dataset.setStatus(Dataset.Status.PROCESSING);
            dataset.setLatestJobId(jobId);
            dataset = datasetRepository.save(dataset);
            log.info("processing oss enrichment for dataset: {}", dataset.getName());

            var datasources = dataset.getDatasources();
            dataset.setUpdatedAt(ZonedDateTime.now());
            dataset = datasetRepository.save(dataset);

            for (var datasource : datasources) {

                if ( datasource.getStatus().equals(Datasource.Status.PROCESSING_ERROR) ) {
                    log.info("skipping datasource: {} due to status of PROCESSING_ERROR", datasource);
                    continue;
                }

                log.info("running enrichment for datasource: {}", datasource.getPurl());
                datasource.setStatus(Datasource.Status.PROCESSING);
                datasource.setLatestJobId(jobId);
                datasource = datasourceRepository.save(datasource);

                var datasourcePurl = datasource.getPurl();

                var oneOrNone = datasourceEventRepository.findFirstByDatasourcePurlAndStatusAndOssEnrichedFalseOrderByCommitDateTimeAsc(
                    datasourcePurl,
                    DatasourceEvent.Status.READY_FOR_PROCESSING
                );

                var eventsReadyForProcessing = new ArrayList<DatasourceEvent>();

                // if oneOrNone is empty it means this datasource did not itself have an update but is part of a dataset
                // that had one or more datasources that did. as such we need to include the latest datasourceEvent from
                // this datasource to ensure the analyze-serice is able to perform tabulation accurately 
                if (oneOrNone.isEmpty()) {

                    // // the input service won't update the txid on a datasource that doesn't have a new event to process
                    // // we set it here to because we are reprocessing the head event for datasource and thus the 
                    // // txid needs reflect that. 
                    // datasource.setLatestTxid(dataset.getLatestTxid());
                    datasource = datasourceRepository.save(datasource);
                    var events = datasourceEventRepository.findFirstByDatasourcePurlOrderByCommitDateTimeDesc(datasourcePurl);

                    if ( !events.isEmpty() ) { // should never be empty but jik...
                        var event = events.get(0);
                        // event.setOssEnriched(false);
                        // event.setAnalyzed(false);
                        // event.setForecasted(false);
                        // event.setRecommended(false);
                        // event.setStatus(DatasourceEvent.Status.READY_FOR_PROCESSING);
                        eventsReadyForProcessing.add(event);
                    }
                    
                } else {

                    var earliestEvent = oneOrNone.get(0);
                    var earliestEventCommitDateTime = earliestEvent.getCommitDateTime();
                    var events = datasourceEventRepository.findAllByDatasourcePurlAndCommitDateTimeOrDatasourcePurlAndCommitDateTimeAfter(
                        datasourcePurl, 
                        earliestEventCommitDateTime,
                        datasourcePurl,
                        earliestEventCommitDateTime
                    );
                    eventsReadyForProcessing.addAll(events);
                }

                for (var datasourceEvent : eventsReadyForProcessing) {
                    log.info("running oss enrichment for datasourceEvent: {}", datasourceEvent.getPurl());

                    // the events have a unique txid but share the same jobid
                    datasourceEvent.setJobId(jobId);

                    // clear the flags because we're going to reprocess them
                    datasourceEvent.setOssEnriched(false);
                    datasourceEvent.setAnalyzed(false);
                    datasourceEvent.setForecasted(false);
                    datasourceEvent.setRecommended(false);
                    datasourceEvent.setStatus(DatasourceEvent.Status.PROCESSING);
                    datasourceEvent = datasourceEventRepository.save(datasourceEvent);

                    //var mapper = new ObjectMapper().findAndRegisterModules();
                    //var p = mapper.readValue(datasourceEvent.getPayload(), PackageWrapper.class);

                    // send event to grype service 
                    var grypeOssMessage = ApiRequest.builder()
                                                    // for all internal pipeline requests that are job-related we 
                                                    // use the jobID as the request txid so we can log-trace all the 
                                                    // job things across all invoked services. 
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


                    // Use datasourceEvent ID as partition key for even distribution
                    String partitionKey = datasourceEvent.getId().toString();
                    kafka.makeRequest("grype-service_REQUEST", partitionKey, grypeOssMessage);
                    // kafka.makeRequest("grype-service_REQUEST", grypeOssMessage);

                    // send event to package-index service 
                    var packageIndexMessage = ApiRequest.builder()
                                                        // for all internal pipeline requests that are job-related we 
                                                        // use the jobID as the request txid so we can log-trace all the 
                                                        // job things across all invoked services. 
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


                    kafka.makeRequest("package-index-service_REQUEST", partitionKey, packageIndexMessage);
                    //kafka.makeRequest("package-index-service_REQUEST", packageIndexMessage);

                }
                
            }  

        }

        log.info("checkStartEnrichment done");
    }


    /**
     * 
     */
    public void checkDoneEnrichment() {
        log.info("running checkDoneEnrichment...");

        // find all datasets ready for processing 
        List<Dataset> datasets = datasetRepository.findAllByStatus(Dataset.Status.PROCESSING);

        for (var ds : datasetRepository.findAll()) {
            log.info("dataset: {}  status: {}", ds.getName(), ds.getStatus());
        }

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

            datasourceEventRepository.setProcessingFlag(jobId);

            for (var ds : dataset.getDatasources()) {
                if (ds.getStatus().equals(Datasource.Status.READY_FOR_NEXT_PROCESSING)) {
                    ds.setStatus(Datasource.Status.PROCESSING);
                }
            }
            datasourceRepository.saveAll(dataset.getDatasources());


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
    public void checkDoneAnalyze() {
        log.info("running checkDoneAnalyze...");
        // find all datasets ready for processing 
        List<Dataset> datasets = datasetRepository.findAllByStatus(Dataset.Status.PROCESSING);

        for (var ds : datasetRepository.findAll()) {
            log.info("dataset: {}  status: {}", ds.getName(), ds.getStatus());
        }

        // nothing to do if there's nothing ready for processing 
        if (datasets.isEmpty()) { 
            log.info("no datasets in state {} at this time.", Dataset.Status.PROCESSING);
            log.info("checkDoneAnalyze done");
            return; 
        }

        // go through every datasourceEvent in every constituent datasource to see if oss enrichment was 
        // completed for all events currently being processed 
        for (var dataset : datasets) {
            log.info("checking dataset: {}", dataset.getName());
            var datasources = dataset.getDatasources();

            var hasBeenEnrichedCount = 0;
            var readyForProcessingCount = 0;
            var datasourcesInErrorState = 0;
            //List<Long> datasourceEventIdsReadyForForecast = new ArrayList<Long>();

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
                    datasourceEventRepository.countByDatasourcePurlAndStatus(
                        datasourcePurl, 
                        DatasourceEvent.Status.PROCESSING  
                    );

                var countByDatasourceEventsInProcessingReadyForForecast = 
                    datasourceEventRepository.countByDatasourcePurlAndStatusAndOssEnrichedTrueAndPackageIndexEnrichedTrueAndAnalyzedTrueAndForecastedFalse(
                        datasourcePurl, 
                        DatasourceEvent.Status.READY_FOR_NEXT_PROCESSING
                    );

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
            if (hasBeenEnrichedCount == (datasources.size() - readyForProcessingCount - datasourcesInErrorState)) {
                log.info("analyze step complete for dataset: {}", dataset.getName());
                log.info("invoking forecast-service...");

                for (var datasource : datasources) {
                    if (
                        datasource.getStatus().equals(Datasource.Status.READY_FOR_PROCESSING)
                        || datasource.getStatus().equals(Datasource.Status.PROCESSING_ERROR)
                ) { 
                    continue; 
                }
                    
                    var datasourcePurl = datasource.getPurl();
                    var datasourceEventRecords = datasourceEventRepository.findAllByDatasourcePurlAndStatus(
                        datasourcePurl,
                        DatasourceEvent.Status.READY_FOR_NEXT_PROCESSING
                    );
    
                    if ( !datasourceEventRecords.isEmpty() ) {
                        log.info("marking datasource: {} as: {}", datasource.getPurl(), Datasource.Status.IDLE);
                        datasource.setStatus(Datasource.Status.IDLE);
                        datasourceRepository.save(datasource); 

                        for (var datasourceEventRecord : datasourceEventRecords) {
                            datasourceEventRecord.setStatus(DatasourceEvent.Status.PROCESSED);
                            datasourceEventRepository.save(datasourceEventRecord);
                        }
                    }

                } 

                if (dataset.getStatus() != Dataset.Status.PROCESSING_ERROR) {
                    log.info("marking dataset: {} as: {}", dataset.getName(), Dataset.Status.IDLE);
                    dataset.setStatus(Dataset.Status.IDLE);
                    datasetRepository.save(dataset);
                }

                // // send event to forecast service 
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
    public void checkDoneForecast() {
        log.info("running checkDoneForecast...");
        // find all datasets ready for processing 
        List<Dataset> datasets = datasetRepository.findAllByStatus(Dataset.Status.PROCESSING);

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

            var dsmrsWithJobId = datasetMetricsRepository.findAllByJobId(jobId);
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


    public void checkDoneRecommend() {
        log.info("running checkDoneRecommend...");
        List<Dataset> datasets = datasetRepository.findAllByStatus(Dataset.Status.PROCESSING);


        // nothing to do if there's nothing ready for processing 
        if (datasets.isEmpty()) { 
            log.info("no datasets in state {} at this time.", Dataset.Status.PROCESSING);
            log.info("checkDoneRecommend done");
            return; 
        }

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

            var dsmrsWithJobId = datasetMetricsRepository.findAllByJobId(jobId);
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
            datasourceEventRepository.callUpdateDatasourceEventsProcessingCompletedStatus(jobId);

            for (var datasource : dataset.getDatasources()) {
                if ( !datasource.getStatus().equals(Datasource.Status.PROCESSING)) { continue; }               

                log.info("marking datasource: {} as: {}", datasource.getPurl(), Datasource.Status.IDLE);
                datasource.setStatus(Datasource.Status.IDLE);
                datasourceRepository.save(datasource); 

            } 

            if (dataset.getStatus() != Dataset.Status.PROCESSING_ERROR) {
                log.info("marking dataset: {} as: {}", dataset.getName(), Dataset.Status.IDLE);
                dataset.setStatus(Dataset.Status.IDLE);
                datasetRepository.save(dataset);
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
        jdbcTemplate.update("CALL update_datasource_events_processing_status(?)", jobId);
    }


}
