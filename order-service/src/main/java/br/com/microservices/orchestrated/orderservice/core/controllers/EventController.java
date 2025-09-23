package br.com.microservices.orchestrated.orderservice.core.controllers;

import br.com.microservices.orchestrated.orderservice.core.document.Event;
import br.com.microservices.orchestrated.orderservice.core.dtos.EventFilters;
import br.com.microservices.orchestrated.orderservice.core.services.EventService;
import lombok.AllArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@AllArgsConstructor
@RequestMapping("/api/events")
public class EventController {

    private final EventService eventService;

    @GetMapping
    public Event findByFilters(EventFilters eventFilters){

        return eventService.findByFilters(eventFilters);
    }

    @GetMapping("/all")
    public List<Event> findAll(EventFilters eventFilters){

        return eventService.findAll();
    }
}
