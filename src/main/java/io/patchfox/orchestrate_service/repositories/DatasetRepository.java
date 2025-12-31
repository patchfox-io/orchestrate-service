package io.patchfox.orchestrate_service.repositories;


import java.util.List;
import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;

import io.patchfox.db_entities.entities.Dataset;
import io.patchfox.db_entities.entities.Datasource;


public interface DatasetRepository extends JpaRepository<Dataset, Long> {
    
    public List<Dataset> findAllByName(String name);

    public List<Dataset> findAllByStatus(Dataset.Status status);

    public List<Dataset> findAllByLatestJobId(UUID txid);

}
