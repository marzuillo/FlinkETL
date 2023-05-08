package etl.example.csv.model;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;

public class Event {

    private static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

    private String id;
    private Integer year;
    private String country;
    private String countryCode;
    private String indicator;
    private Double value;

    private String timestamp;

    public Event(String id, Integer year, String country, String countryCode, String indicator, Double value, String timestamp) {
        this.id = id;
        this.year = year;
        this.country = country;
        this.countryCode = countryCode;
        this.indicator = indicator;
        this.value = value;
        this.timestamp = timestamp;
    }

    public String getId() {
        return id;
    }

    public Integer getYear() {
        return year;
    }

    public String getCountry() {
        return country;
    }

    public String getCountryCode() {
        return countryCode;
    }

    public String getIndicator() {
        return indicator;
    }

    public Double getValue() {
        return value;
    }

    public String getTimestamp() {
        return timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Event event = (Event) o;

        if (id != null ? !id.equals(event.id) : event.id != null) return false;
        if (year != null ? !year.equals(event.year) : event.year != null) return false;
        if (country != null ? !country.equals(event.country) : event.country != null) return false;
        if (countryCode != null ? !countryCode.equals(event.countryCode) : event.countryCode != null) return false;
        if (indicator != null ? !indicator.equals(event.indicator) : event.indicator != null) return false;
        if (value != null ? !value.equals(event.value) : event.value != null) return false;
        return timestamp != null ? timestamp.equals(event.timestamp) : event.timestamp == null;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (year != null ? year.hashCode() : 0);
        result = 31 * result + (country != null ? country.hashCode() : 0);
        result = 31 * result + (countryCode != null ? countryCode.hashCode() : 0);
        result = 31 * result + (indicator != null ? indicator.hashCode() : 0);
        result = 31 * result + (value != null ? value.hashCode() : 0);
        result = 31 * result + (timestamp != null ? timestamp.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ",  "", "")
            .add("id=" + id + "")
            .add("year=" + year + "")
            .add("country=" + country + "")
            .add("countryCode=" + countryCode + "")
            .add("indicator=" + indicator + "")
            .add("value=" + value)
            .add("timestamp=" + timestamp)
            .toString();
    }

    public Map<String, Object> toMap() {
        Map<String, Object> ris = new HashMap<>();
        ris.put("id", id );
        ris.put("year", year );
        ris.put("country", country );
        ris.put("countryCode", countryCode );
        ris.put(indicator, value );
        try {
            ris.put("timestamp", formatter.parse(timestamp)  );
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

        return ris;
    }
}
