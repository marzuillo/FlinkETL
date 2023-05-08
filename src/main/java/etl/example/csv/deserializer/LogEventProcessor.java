package etl.example.csv.deserializer;

import etl.example.csv.model.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;

public class LogEventProcessor  {

    //id=GHA_1986, year=1986, country=Ghana, countryCode=GHA, indicator=Inflation, value=24.5654160765001, timestamp=1986-12-31'T'23:59:59.000

    private static final Logger LOG = LoggerFactory.getLogger(LogEventProcessor.class);
    public static Event toEvent(String line) {
        try {
            final String[] columns = line.split(",");

            String id = columns[0].split("=")[1].trim();
            Integer year = Integer.valueOf(columns[1].split("=")[1].trim());
            String country = columns[2].split("=")[1].trim();
            String countryCode = columns[3].split("=")[1].trim();
            String indicator = columns[4].split("=")[1].trim();
            Double value = Double.valueOf(columns[5].split("=")[1].trim());
            String timestamp = columns[6].split("=")[1].trim();

            final Event event = new Event(id, year, country, countryCode, indicator, value, timestamp);
            LOG.info("parsed "+event);
            return event;
        } catch (Exception e) {
            LOG.info("error with " + line, e);
        }
        return null;
    }
}
