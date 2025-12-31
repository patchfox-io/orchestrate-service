package io.patchfox.orchestrate_service.repositories;


import java.util.UUID;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import io.patchfox.db_entities.entities.DatasourceEvent;
import io.patchfox.db_entities.entities.DatasourceEvent.Status;
import jakarta.transaction.Transactional;


public interface DatasourceEventRepository extends JpaRepository<DatasourceEvent, Long> { 

    List<DatasourceEvent> findAllByTxid(UUID txid);

    List<DatasourceEvent> findAllByJobId(UUID jobId);

    List<DatasourceEvent> findAllByPurl(String purl);

    List<DatasourceEvent> findAllByCommitDateTimeOrCommitDateTimeAfter(ZonedDateTime commDateTime, ZonedDateTime commDateTimeAfter);

    List<DatasourceEvent> findAllByDatasourcePurlAndCommitDateTimeOrDatasourcePurlAndCommitDateTimeAfter(
        String datasourcePurl, 
        ZonedDateTime commDateTime, 
        String datasourcePurl2,
        ZonedDateTime commDateTimeAfter);

    List<DatasourceEvent> findFirstByDatasourcePurlOrderByEventDateTimeDesc(String datasourcePurl);

    List<DatasourceEvent> findFirstByDatasourcePurlAndStatusAndOssEnrichedFalseOrderByCommitDateTimeAsc(String datasourcePurl, DatasourceEvent.Status status);

    List<DatasourceEvent> findAllByDatasourcePurlAndStatus(String datasourcePurl, DatasourceEvent.Status status);

    List<DatasourceEvent> findAllByDatasourcePurlAndCommitDateTimeAfter(String datasourcePurl, ZonedDateTime commitDateTime);

    List<DatasourceEvent> findFirstByDatasourcePurlOrderByCommitDateTimeDesc(String datasourcePurl);

    long countByDatasourcePurlAndStatus(String datasourcePurl, DatasourceEvent.Status status);

    long countByStatusAndOssEnrichedTrueAndPackageIndexEnrichedTrueAndAnalyzedFalse(DatasourceEvent.Status status); 

    long countByDatasourcePurlAndStatusAndOssEnrichedTrueAndPackageIndexEnrichedTrueAndAnalyzedFalse(String datasourcePurl, DatasourceEvent.Status status);

    long countByDatasourcePurlAndStatusAndOssEnrichedTrueAndPackageIndexEnrichedTrueAndAnalyzedTrueAndForecastedFalse(String datasourcePurl, DatasourceEvent.Status status);

    long countByJobIdAndStatusAndOssEnrichedTrueAndPackageIndexEnrichedTrueAndAnalyzedTrueAndForecastedTrue(UUID jobId, DatasourceEvent.Status status);

    long countByJobId(UUID jobId);

    long countByJobIdAndStatus(UUID jobId, DatasourceEvent.Status status);

    long countByJobIdAndStatusAndOssEnrichedTrueAndPackageIndexEnrichedTrueAndAnalyzedFalse(UUID jobId, DatasourceEvent.Status status);

    long countByJobIdAndStatusAndForecastedTrue(UUID jobId, DatasourceEvent.Status status);

    @Query(
        "SELECT de.id " +
	    "FROM DatasourceEvent de " +
	    "WHERE de.id in :datasourceEventIds " + 
        "ORDER BY de.commitDateTime ASC"
    )
    List<Long> getDatasourceEventIdsOrderedByCommitDatetimeAsc(@Param("datasourceEventIds") List<Long> datasourceEventIds);

    @Query(
        "SELECT de.id " + 
        "FROM DatasourceEvent de " + 
        "INNER JOIN datasource ON de.datasource.id=datasource.id " + 
        "WHERE de.ossEnriched " + 
        "AND de.packageIndexEnriched " +
        "AND NOT de.analyzed " +
        "AND de.status='READY_FOR_NEXT_PROCESSING' " + 
        "AND datasource.purl= :datasourcePurl " +
        "ORDER BY de.commitDateTime ASC"
    )
    List<Long> getDatasourceEventIdsReadyForAnalyzeDatasourcePurl(@Param("datasourcePurl") String datasourcePurl);

    @Query(
        "SELECT de.id " + 
        "FROM DatasourceEvent de " + 
        "WHERE de.ossEnriched " + 
        "AND de.packageIndexEnriched " +
        "AND NOT de.analyzed " +
        "AND de.status='READY_FOR_NEXT_PROCESSING' " + 
        "AND de.jobId= :jobId " +
        "ORDER BY de.commitDateTime ASC"
    )
    List<Long> getDatasourceEventIdsReadyForAnalyze(UUID jobId);

    @Query(
        "SELECT de.id " + 
        "FROM DatasourceEvent de " + 
        "WHERE de.ossEnriched " + 
        "AND de.packageIndexEnriched " +
        "AND de.analyzed " +
        "AND de.forecasted " +
        "AND de.status='PROCESSING' " + 
        "AND de.jobId= :jobId "
    )
    List<Long> getDatasourceEventIdsSentToRecommend(UUID jobId);

    @Modifying
    @Transactional
    @Query(
        "UPDATE DatasourceEvent de " + 
        "SET de.status='PROCESSING' " +
        "WHERE de.ossEnriched " + 
        "AND de.packageIndexEnriched " +
        "AND NOT de.analyzed " +
        "AND de.status='READY_FOR_NEXT_PROCESSING' " + 
        "AND de.jobId= :jobId "
    )
    void setProcessingFlag(UUID jobId);

    @Query(
        "SELECT de.id " + 
        "FROM DatasourceEvent de " + 
        "INNER JOIN datasource ON de.datasource.id=datasource.id " + 
        "WHERE de.ossEnriched " + 
        "AND de.packageIndexEnriched " +
        "AND de.analyzed " +
        "AND de.status='READY_FOR_NEXT_PROCESSING' " + 
        "AND datasource.purl= :datasourcePurl " +
        "ORDER BY de.commitDateTime ASC"
    )
    List<Long> getDatasourceEventIdsReadyForForecastDatasourcePurl(@Param("datasourcePurl") String datasourcePurl);

    @Query(
        value = "SELECT txid FROM datasource_event d WHERE d.job_id = :jobId AND d.analyzed ORDER BY d.commit_date_time DESC LIMIT 1",
        nativeQuery = true
    )
    Optional<UUID> getMostRecentCommitTxidByJobId(UUID jobId);


    @Modifying
    @Transactional
    @Query(nativeQuery = true, value = "CALL update_datasource_events_processing_status(:jobId)")
    void callUpdateDatasourceEventsProcessingStatus(@Param("jobId") UUID jobId);


    @Modifying
    @Transactional
    @Query(nativeQuery = true, value = "CALL update_datasource_events_processing_completed_status(:jobId)")
    void callUpdateDatasourceEventsProcessingCompletedStatus(@Param("jobId") UUID jobId);


    @Modifying
    @Transactional
    @Query(value = "CALL create_datasource_event_commit_datetime_index()", nativeQuery = true)
    void createDatasourceEventCommitDatetimeIndex();


    @Modifying
    @Transactional
    @Query(value = "CALL deduplicate_commit_datetimes(:eventIdsString)", nativeQuery = true)
    void deduplicateCommitDatetimes(@Param("eventIdsString") String eventIdsString);

    
    @Query(value = "SELECT de.id FROM datasource_event de WHERE de.id IN (:eventIds) ORDER BY de.commit_date_time ASC", nativeQuery = true)
    List<Long> getReorderedEventIds(@Param("eventIds") List<Long> eventIds);


}
