package io.patchfox.orchestrate_service.services;

import java.time.ZonedDateTime;
import java.util.Map;
import java.util.UUID;

import org.apache.catalina.connector.Response;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import io.patchfox.orchestrate_service.components.EnvironmentComponent;
import io.patchfox.package_utils.json.ApiResponse;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class PeristalisisService {

    @Autowired
    EnvironmentComponent env;

    public ApiResponse getPeristalisisState(UUID txid, ZonedDateTime requestReceivedAt) {
        var currentState = env.isPeristalsisActivated();
        log.info("GET peristalsis state: {}", currentState);

        var rv = ApiResponse.builder()
                            .code(Response.SC_OK)
                            .txid(txid)
                            .requestReceivedAt(requestReceivedAt.toString())
                            .data(Map.of("activated", currentState))
                            .build();

        return rv;
    }

    public ApiResponse setPeristalisisState(UUID txid, ZonedDateTime requestReceivedAt, boolean activated) {
        var previousState = env.isPeristalsisActivated();
        env.setPeristalsisActivated(activated);

        log.info("POST peristalsis state change: {} -> {}", previousState, activated);

        var rv = ApiResponse.builder()
                            .code(Response.SC_OK)
                            .txid(txid)
                            .requestReceivedAt(requestReceivedAt.toString())
                            .data(Map.of(
                                "activated", activated,
                                "previousState", previousState
                            ))
                            .build();

        return rv;
    }

}
