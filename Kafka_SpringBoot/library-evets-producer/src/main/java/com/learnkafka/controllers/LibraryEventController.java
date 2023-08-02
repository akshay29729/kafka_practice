package com.learnkafka.controllers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.domain.LibraryEventType;
import com.learnkafka.producer.LibraryEventProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ExecutionException;

@RestController
@Slf4j
public class LibraryEventController {

    @Autowired
    LibraryEventProducer libraryEventProducer;

    @PostMapping("/v1/libraryevent")
    public ResponseEntity<LibraryEvent> creatingLibraryEventController(@RequestBody LibraryEvent libraryEvent) throws ExecutionException, JsonProcessingException, InterruptedException {

        libraryEvent.setLibraryEventType(LibraryEventType.NEW);
        SendResult<Integer,String> sendResult=libraryEventProducer.sendLibraryEventSynchronous(libraryEvent);
        log.info("SendResult is:{}",sendResult.toString());
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }

    // for put:  libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);

    @PutMapping("/v1/libraryevent")
    public ResponseEntity<?> updatingLibraryEventController(@RequestBody LibraryEvent libraryEvent) throws ExecutionException, JsonProcessingException, InterruptedException {

        if(libraryEvent.getLibraryEventId()==null){
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Please pass the LibraryEventId, it can't be null for updating....");
        }
        libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
        SendResult<Integer,String> sendResult=libraryEventProducer.sendLibraryEventSynchronous(libraryEvent);
        log.info("SendResult is:{}",sendResult.toString());
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }
}
