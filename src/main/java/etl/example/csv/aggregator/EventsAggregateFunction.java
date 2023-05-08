package etl.example.csv.aggregator;

import etl.example.csv.model.Event;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.Map;

public class EventsAggregateFunction implements AggregateFunction<Event, EventsAggregator, Map<String, Object>> {

    @Override
    public EventsAggregator createAccumulator() {
        return new EventsAggregator();
    }

    @Override
    public EventsAggregator add(Event event, EventsAggregator eventsAggregator) {
        return eventsAggregator.add(event);
    }

    @Override
    public Map<String, Object> getResult(EventsAggregator eventsAggregator) {
        return eventsAggregator.getCollection();
    }

    @Override
    public EventsAggregator merge(EventsAggregator eventsAggregator, EventsAggregator acc1) {
        return eventsAggregator.merge(acc1);
    }
}
