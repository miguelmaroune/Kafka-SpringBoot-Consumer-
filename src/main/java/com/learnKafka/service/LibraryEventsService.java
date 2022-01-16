package com.learnKafka.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnKafka.entity.LibraryEvent;
import com.learnKafka.jpa.LibraryEventsRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.hibernate.sql.Update;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
@Slf4j
public class LibraryEventsService {

    @Autowired
    ObjectMapper objectMapper;
    @Autowired
    private LibraryEventsRepository libraryEventsRepository;

    public void processLibraryEvent(ConsumerRecord<Integer,String> consumerRecord) throws JsonProcessingException {
       //Transform the String into LibraryEvent Object
        LibraryEvent libraryEvent = objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);
        log.info("Library Event : {} ",libraryEvent);
        switch(libraryEvent.getLibraryEventType())
        {
            case NEW:
                //save operation
                save(libraryEvent);
                break;
            case UPDATE:
                //validate the libraryEvent
                validate(libraryEvent);
                //save
                save(libraryEvent);
                break;
            default:
                log.info("Invalid Library Event Type ! ");
        }


    }

    private void validate(LibraryEvent libraryEvent) {
        if(libraryEvent.getLibraryEventId() == null)
        {
            throw new IllegalArgumentException("Library Event ID is missing !");
        }

        Optional<LibraryEvent> libraryEventOptional =  libraryEventsRepository.findById(libraryEvent.getLibraryEventId());
        if(!libraryEventOptional.isPresent())
        {
            throw new IllegalArgumentException("Not a valid library Event");
        }
        log.info("Validation is successful fo the library Event : {} ",libraryEventOptional.get());
    }

    private void save(LibraryEvent libraryEvent) {
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventsRepository.save(libraryEvent);
        log.info("Successfully Persisted the library event {} ",libraryEvent);
    }
}
