package io.patchfox.orchestrate_service.kafka;

import java.lang.reflect.InvocationTargetException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.net.URI;
import java.util.List;
import java.util.UUID;
import java.util.Map;

import org.apache.catalina.connector.Response;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.web.method.HandlerMethod;

import io.patchfox.db_entities.entities.DatasourceEvent;
import io.patchfox.db_entities.entities.DatasourceEvent.Status;
import io.patchfox.db_entities.entities.Datasource;
import io.patchfox.db_entities.entities.Dataset;
import io.patchfox.orchestrate_service.components.EnvironmentComponent;
import io.patchfox.orchestrate_service.controllers.HealthCheckController;
import io.patchfox.orchestrate_service.controllers.RestInfoController;
import io.patchfox.orchestrate_service.repositories.DatasetRepository;
import io.patchfox.orchestrate_service.repositories.DatasourceEventRepository;
import io.patchfox.orchestrate_service.repositories.DatasourceRepository;
import io.patchfox.package_utils.json.ApiRequest;
import io.patchfox.package_utils.json.ApiResponse;
import io.patchfox.orchestrate_service.services.RestInfoService;
import io.patchfox.package_utils.util.Pair;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class KafkaBeans {

    @Autowired
    private KafkaTemplate<String, ApiRequest> kafkaRequestTemplate;

    @Autowired
    private KafkaTemplate<String, ApiResponse> kafkaResponseTemplate;

    @Autowired 
    RestInfoService restInfoService;

    @Autowired
    ApplicationContext context;

    @Autowired
    EnvironmentComponent env;

    @Autowired
    DatasetRepository datasetRepository;

    @Autowired
    DatasourceRepository datasourceRepository;

    @Autowired
    DatasourceEventRepository datasourceEventRepository;

    //
    // create topics for other services to send and receive messages on 
    //

    @Bean
    public NewTopic serviceRequestTopic() {
        return TopicBuilder.name(env.getKafkaRequestTopicName())
                           // *!* you need at least as many partitions as you have consumers
                           // check "spring.kafka.listener.concurrency" in file application.properties 
                           .partitions(10)
                           .replicas(1)
                           .build();
    }

    @Bean
    public NewTopic serviceResponseTopic() {
        return TopicBuilder.name(env.getKafkaResponseTopicName())
                           // *!* you need at least as many partitions as you have consumers
                           // check "spring.kafka.listener.concurrency" in file application.properties 
                           .partitions(10)
                           .replicas(1)
                           .build();
    }


    //
    // create listeners for the topics this service will send and receive on.
    // note that the reason we're not using the "env" component here is because the Kafka annotations are fun in that 
    // they don't allow for strings that aren't constants. You HAVE to use the property placeholder directly if you want
    // to make the id and topic configurable by way of the application.yml file.
    //

    @KafkaListener(
        clientIdPrefix = "#'${spring.kafka.request.client-id-prefix}'",
        groupId = "#'${spring.kafka.group-name}'",
        topics = "#{'${spring.kafka.request-topic}'}",
        properties = {"spring.json.value.default.type=io.patchfox.package_utils.json.ApiRequest"}
    )
    public void listenToRequestTopic(ApiRequest apiRequest) throws Exception {
        log.info("received apiRequest message: {}", apiRequest);
        var now = ZonedDateTime.ofInstant(Instant.now(), ZoneOffset.UTC);
        var responseTopicName = apiRequest.getResponseTopicName();
        var txid = apiRequest.getTxid();
        var verb = apiRequest.getVerb();
        var resource = apiRequest.getUri();
        var resourceSignature = verb + "_" + resource.toString();
        try {
            var requestPair = new Pair<>(verb, resource);
            var handlerMethod = restInfoService.getHandlerFor(requestPair);
            var apiResponse = invokeMethod(txid, verb.toString(), resource.toString(), handlerMethod, now);
            apiResponse.setResponderName(env.getServiceName());
            apiResponse.setResponderResourceSignature(resourceSignature);
            kafkaResponseTemplate.send(responseTopicName, apiResponse);
        } catch (NullPointerException e) {
            var notFoundResponse = ApiResponse.builder()
                                              .responderName(env.getServiceName())
                                              .code(Response.SC_NOT_FOUND)
                                              .txid(txid)
                                              .requestReceivedAt(now.toString())
                                              .build();

            kafkaResponseTemplate.send(responseTopicName, notFoundResponse);
        } catch (Exception e) {
            log.error("exception was: ", e);
            var serverErrorResponse = ApiResponse.builder()
                                              .responderName(env.getServiceName())
                                              .code(Response.SC_INTERNAL_SERVER_ERROR)
                                              .txid(txid)
                                              .requestReceivedAt(now.toString())
                                              .build();

            kafkaResponseTemplate.send(responseTopicName, serverErrorResponse);
        }
    }

    @KafkaListener(
        clientIdPrefix = "#'${spring.kafka.response.client-id-prefix}'",
        groupId = "#'${spring.kafka.group-name}'",
        topics = "#{'${spring.kafka.response-topic}'}",
        properties = {"spring.json.value.default.type=io.patchfox.package_utils.json.ApiResponse"}
    )
    public void listenToResponseTopic(ApiResponse response) throws Exception {
        // here is where we inspect the response object and figure out what, if anything, we need to do next 
        log.info("received apiResponse message: {}", response);

        var rc = HttpStatusCode.valueOf(response.getCode());
        if (rc.isError()) {
            log.warn("response indicates error!");

            // if (response.getTxid() == null) {
            //     log.warn("received response without a txid!");
            // } else {
            //     var datasourceEvents = datasourceEventRepository.findAllByJobId(response.getTxid());
            //     /*
            //      * TODO this is setting processing error flag on only one datasourceEvent
            //      */
            //     if ( !datasourceEvents.isEmpty() ) {
            //         var datasourceEvent = datasourceEvents.get(0);
            //         var datasource = datasourceEvent.getDatasource();

            //         log.info("setting PROCESSING_ERROR flag on datasourceEvent: {} ", datasourceEvent.getPurl());
            //         datasourceEvent.setStatus(DatasourceEvent.Status.PROCESSING_ERROR);
            //         datasourceEventRepository.save(datasourceEvent);

            //         log.info("setting PROCESSING_ERROR flag on datasource: {}", datasource.getPurl());
            //         datasource.setStatus(Datasource.Status.PROCESSING_ERROR);
            //         datasource.setNumberEventProcessingErrors(datasource.getNumberEventProcessingErrors() + 1);
            //         datasourceRepository.save(datasource);
            //     } else {
            //         log.info("no datasourceEvent records found for jobId: {}", response.getTxid());
            //     }

                // var datasourceRecords = datasourceRepository.findAllByLatestJobId(response.getTxid());
                // for (var datasourceRecord : datasourceRecords) {
                //     log.info("marking datasource: {} to PROCESSING_ERROR state", datasourceRecord.getPurl());
                //     datasourceRecord.setStatus(Datasource.Status.PROCESSING_ERROR);
                // } 

                // var datasetRecords = datasetRepository.findAllByLatestJobId(response.getTxid());
                // for (var datasetRecord : datasetRecords) {
                //     log.info("marking datasource: {} to PROCESSING_ERROR state", datasetRecord.getName());
                //     datasetRecord.setStatus(Dataset.Status.PROCESSING_ERROR);
                // }

            // }
        }

        //
        // handle pagination of data to analyze-service
        //
        if (
            response.getResponderName().equals("analyze-service")
            && response.getResponderResourceSignature().equals("POST_/api/v1/tabulate")
        ) {
            log.debug("response data is: {}", response.getData());
            log.debug("data keys are: {}", response.getData().keySet());
            log.debug("data entrySet is: {}", response.getData().entrySet());

            var dataMap = (Map<String, Object>)response.getData().get("data");
            var currentPageIndex = (Integer)dataMap.get("pageIndex");
            var pageSize = (Integer)dataMap.get("pageSize");
            var dataList = (List<Long>)dataMap.get("allDatasourceEventRecordIds");
            var datasetName = (String)dataMap.get("datasetName");
            var nextPageIndex = currentPageIndex + 1;
            var nextStartIndex = nextPageIndex * pageSize;

            if (nextStartIndex <= dataList.size() - 1) {
                log.info("more analyze data to be processed - sending processing request for next page of data");
                var analyzeRequest = ApiRequest.builder()
                                               .txid(response.getTxid())
                                               .responseTopicName(env.getKafkaResponseTopicName())
                                               .verb(ApiRequest.httpVerb.POST)
                                               .uri(URI.create("/api/v1/tabulate"))
                                               .queryStringParameters(
                                                    Map.of(
                                                        "datasetName", datasetName,
                                                        "pageIndex", Integer.toString(nextPageIndex),
                                                        "pageSize", Integer.toString(pageSize)
                                                    )
                                                )
                                                .data(
                                                    Map.of(
                                                        "datasourceEventIndexesByCommitDateAsc", 
                                                        dataList
                                                    )
                                                )
                                                .build();
                                               
                makeRequest("analyze-service_REQUEST", analyzeRequest);
            }

        }

    }


    //
    // helpers 
    //


    /**
     * 
     * @param topic
     * @param apiRequest
     */
    public void makeRequest(String topic, ApiRequest apiRequest) throws IllegalArgumentException {
        log.info("servicing apiRequest as Kafka message: {}", apiRequest);
        if ( !apiRequest.isValidForKafka() ) { 
            log.error("request obj failed validity check - rejecting and throwing exception");
            throw new IllegalArgumentException(); 
        }

        kafkaRequestTemplate.send(topic, apiRequest);
    }

    /**
     * 
     * @param topic
     * @param apiRequest
     */
    public void makeRequest(String topic, String partitionKey, ApiRequest apiRequest) throws IllegalArgumentException {
        log.info("servicing apiRequest as Kafka message: {}", apiRequest);
        if ( !apiRequest.isValidForKafka() ) { 
            log.error("request obj failed validity check - rejecting and throwing exception");
            throw new IllegalArgumentException(); 
        }

        kafkaRequestTemplate.send(topic, partitionKey, apiRequest);
    }


    /**
     * 
     * @param topic
     * @param apiRequests
     * @param timeoutInMs
     * @throws IllegalArgumentException
     * @throws InterruptedException
     */
    @Async
    public void makeBufferedRequest(
        String topic, 
        List<ApiRequest> apiRequests
    ) throws IllegalArgumentException, InterruptedException {
    
        var bufferSize = env.getOssEnrichmentKafkaBufferSize();
        var timeoutInMs = env.getOssEnrichmentKafkaNapDurationMs();

        var count = 0;
        for (var apiRequest : apiRequests) {
            if (count == bufferSize) {
                count = 0; 
                log.info("********** message buffer size reached - sleeping for {}ms **********", timeoutInMs);
                Thread.sleep(timeoutInMs);
            }
            count += 1;

            log.info("servicing apiRequest as Kafka message: {}", apiRequest);
            if ( !apiRequest.isValidForKafka() ) { 
                log.error("request obj with txid: {} failed validity check - MESSAGE NOT SENT", apiRequest.getTxid());
            } else {
                kafkaRequestTemplate.send(topic, apiRequest);
            }
            
        }

    }


    /**
     * helper to invoke the handler method we already know is associated with a given REST URI. The method allows us 
     * to invoke the method with the correct arguments w/o having to deal with a lot of wonky reflection that would 
     * otherwise be necessary. 
     * 
     * @param txid
     * @param verb
     * @param resource
     * @param handlerMethod
     * @param requestReceivedAt
     * @return
     * @throws InvocationTargetException 
     * @throws IllegalAccessException 
     */
    private ApiResponse invokeMethod(
            UUID txid,
            String verb,
            String resource,
            HandlerMethod handlerMethod, 
            ZonedDateTime requestReceivedAt
    ) throws IllegalAccessException, InvocationTargetException  {
        // this should never happen so long as we call the RestInfoService helper methods first to 
        // get a hold of the reflected Method obj representing the controller for the requested resource.
        // I don't like returning null so we assume we can't find what we're looking for until we find it
        // and replace this with whatever is more appropriate. 
        var rv = ApiResponse.builder()
                            .responderName(env.getServiceName())
                            .code(Response.SC_NOT_FOUND)
                            .txid(txid)
                            .requestReceivedAt(requestReceivedAt.toString())
                            .build();
                            
        var restSignature = verb + "_" + resource;

        var bean = handlerMethod.getBean();
        // I think this is a name only until the object is actually created. It's typed as an "Object" in the 
        // HandlerMethod class.
        if (bean.getClass() == String.class) { bean = context.getBean((String)bean); }
        var beanMethod = handlerMethod.getMethod();
        log.debug("bean method name is: {}", beanMethod.getName());
        log.debug("bean var class is: {}", bean.getClass());
        log.debug("bean type is: {}", handlerMethod.getBeanType());
        log.debug("beanMethod is: {}", beanMethod);

        /*
         * 
         * WHEN YOU ADD A NEW REST CONTROLLER/SERVICE THIS IS WHERE YOU ADD THE HOOK TO ENSURE THE KAFKA LISTENER
         * KNOWS HOW TO INVOKE THE CONTROLLER METHOD
         * 
         */
        switch(restSignature) {
            case HealthCheckController.GET_PING_SIGNATURE:
            case RestInfoController.GET_REST_INFO_SIGNATURE:
                var re = (ResponseEntity<ApiResponse>)beanMethod.invoke(bean, txid, requestReceivedAt);
                rv = re.getBody();
                break;
        }
        return rv;
    }

}
