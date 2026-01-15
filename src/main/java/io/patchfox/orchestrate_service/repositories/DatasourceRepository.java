package io.patchfox.orchestrate_service.repositories;

import java.util.List;
import java.util.UUID;
import java.time.ZonedDateTime;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;

import io.patchfox.db_entities.entities.Dataset;
import io.patchfox.db_entities.entities.Datasource;


public interface DatasourceRepository extends JpaRepository<Datasource, Long> {
    public List<Datasource> findAllByPurl(String purl);

    public List<Datasource> 
        findAllByStatusAndLastEventReceivedAtBefore(
            Datasource.Status status,
            ZonedDateTime before
    );

    public List<Datasource> findAllByLatestJobId(UUID txid);

    @Modifying
    @Query("""
    UPDATE Datasource d
    SET d.status = 'PROCESSING'
    WHERE d.status = 'READY_FOR_NEXT_PROCESSING'
        AND EXISTS (
        SELECT 1
        FROM d.datasets ds
        WHERE ds = :dataset
        )
    """)
    void markProcessing(Dataset dataset);

}
