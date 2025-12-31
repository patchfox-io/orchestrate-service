package io.patchfox.orchestrate_service.repositories;

import java.util.List;
import java.util.UUID;
import java.time.ZonedDateTime;

import org.springframework.data.jpa.repository.JpaRepository;

import io.patchfox.db_entities.entities.Datasource;


public interface DatasourceRepository extends JpaRepository<Datasource, Long> {
    public List<Datasource> findAllByPurl(String purl);

    public List<Datasource> 
        findAllByStatusAndLastEventReceivedAtBefore(
            Datasource.Status status,
            ZonedDateTime before
    );

    public List<Datasource> findAllByLatestJobId(UUID txid);
}
