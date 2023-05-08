package etl.example.csv.model;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

public class EventCollection {

    private String id;
    private Map<String, Event> collection;
    private String timestamp;

    public EventCollection() {
        this(null, null);
    }

    public EventCollection(String id, String timestamp) {
        this.id = id;
        this.collection = new HashMap<>();
        this.timestamp = timestamp;
    }

    public void add(Event event) {
        if(collection.isEmpty()){
            setId(event.getId());
            setTimestamp(event.getTimestamp());
        }
        collection.put(event.getId(), event);
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Map<String, Event> getCollection() {
        return collection;
    }

    public void setCollection(Map<String, Event> collection) {
        this.collection = collection;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }
}
