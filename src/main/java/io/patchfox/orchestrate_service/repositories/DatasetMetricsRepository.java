package io.patchfox.orchestrate_service.repositories;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import io.patchfox.db_entities.entities.DatasetMetrics;

public interface DatasetMetricsRepository extends JpaRepository<DatasetMetrics, Long> {

    List<DatasetMetrics> findAllByJobId(UUID jobId);

    @Query(
        value = "SELECT txid FROM dataset_metrics d WHERE d.job_id = :jobId ORDER BY d.commit_date_time DESC LIMIT 1",
        nativeQuery = true
    )
    Optional<UUID> getLatestDatasetMetricsRecordTxidForJobId(UUID jobId);


    List<DatasetMetrics> findDatasetMetricsByTxid(UUID txid);

}
