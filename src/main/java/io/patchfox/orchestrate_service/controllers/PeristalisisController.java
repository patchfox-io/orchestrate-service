package io.patchfox.orchestrate_service.controllers;

import java.time.ZonedDateTime;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestAttribute;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import io.patchfox.package_utils.json.ApiResponse;
import io.patchfox.orchestrate_service.services.PeristalisisService;

import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@RestController
public class PeristalisisController {

    public static final String API_PATH_PREFIX = "/api/v1";
    public static final String PERISTALSIS_PATH = API_PATH_PREFIX + "/peristalsis";
    public static final String GET_PERISTALSIS_SIGNATURE = "GET_" + PERISTALSIS_PATH;
    public static final String POST_PERISTALSIS_SIGNATURE = "POST_" + PERISTALSIS_PATH;

    @Autowired
    PeristalisisService peristalisisService;

    @GetMapping(
        value = PERISTALSIS_PATH,
        produces = MediaType.APPLICATION_JSON_VALUE
    )
    ResponseEntity<ApiResponse> getPeristalisisState(
        @RequestAttribute UUID txid,
        @RequestAttribute ZonedDateTime requestReceivedAt
    ) {
        var apiResponse = peristalisisService.getPeristalisisState(txid, requestReceivedAt);
        return ResponseEntity.status(apiResponse.getCode()).body(apiResponse);
    }

    @PostMapping(
        value = PERISTALSIS_PATH,
        produces = MediaType.APPLICATION_JSON_VALUE
    )
    ResponseEntity<ApiResponse> setPeristalisisState(
        @RequestAttribute UUID txid,
        @RequestAttribute ZonedDateTime requestReceivedAt,
        @RequestParam(name = "activated", required = true) boolean activated
    ) {
        var apiResponse = peristalisisService.setPeristalisisState(txid, requestReceivedAt, activated);
        return ResponseEntity.status(apiResponse.getCode()).body(apiResponse);
    }

}
