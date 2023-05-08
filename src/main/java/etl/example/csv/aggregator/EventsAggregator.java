package etl.example.csv.aggregator;

import etl.example.csv.model.Event;

import java.util.HashMap;
import java.util.Map;

public class EventsAggregator {

    private Map<String, Object> collection;

    public EventsAggregator() {
        this.collection = new HashMap<>();
    }

    public EventsAggregator add(Event event) {
        collection.putAll(event.toMap());
        return this;
    }

    public EventsAggregator merge(EventsAggregator eventsAggregator) {
        collection.putAll(eventsAggregator.getCollection());
//        eventsAggregator.getAggregation()
//                .forEach((key, value) -> {
//                    if(collection.containsKey(key)){
//                        collection.get(key).putAll(value);
//                    }else{
//                        collection.put(key, value);
//                    }
//                }
//        );
        return this;
    }

    public Map<String, Object> getCollection() {
        return collection;
    }


}
