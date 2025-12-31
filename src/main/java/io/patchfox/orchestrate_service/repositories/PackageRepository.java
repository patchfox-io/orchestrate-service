package io.patchfox.orchestrate_service.repositories;


import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import io.patchfox.db_entities.entities.Package;

public interface PackageRepository extends JpaRepository<Package, Long> {
    List<Package> findByPurl(String purl);
}
