package etl.example.csv.loggenerator;

import etl.example.csv.model.Event;
import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableMap;

import javax.annotation.concurrent.Immutable;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.Collections.shuffle;


/**
 *
 * This LogGenerator is a way to prepare the input for the exercise.
 *
 * In the exercise we suppose to receive a log file.
 * Like in every application, we will have the information relative to the same event, spread to many rows.
 * So the flink ETL should be able to read, collect, aggregate and send the complete information for the same id.
 *
 * In order to prepare bing input,
 * I downloaded some source file from https://data.worldbank.org/
 * and reorganized the information on log.txt moving data from column to rows.
 *
 */
public class LogGenerator {

  static String[] header = {"Country Name", "Country Code", "Indicator Name", "Indicator Code", "1960", "1961", "1962", "1963", "1964", "1965", "1966", "1967", "1968", "1969", "1970", "1971", "1972", "1973", "1974", "1975", "1976", "1977", "1978", "1979", "1980", "1981", "1982", "1983", "1984", "1985", "1986", "1987", "1988", "1989", "1990", "1991", "1992", "1993", "1994", "1995", "1996", "1997", "1998", "1999", "2000", "2001", "2002", "2003", "2004", "2005", "2006", "2007", "2008", "2009", "2010", "2011", "2012", "2013", "2014", "2015", "2016", "2017", "2018", "2019", "2020", "2021"};
  static String OUTPUT_FILE_PATH = "/Users/" + System.getenv("USER") + "/repo/flink/src/main/resources/log.txt";

  static Map<String, Path> sourceFiles = ImmutableMap.of(
      "kWh_per_capita", Paths.get("/Users/" + System.getenv("USER") + "/repo/flink/src/main/resources/Electric_power_consumption_kWh_per_capita.csv"),
      "GDP_growth",Paths.get("/Users/" + System.getenv("USER") + "/repo/flink/src/main/resources/GDP_growth_annualpercentage.csv"),
      "Inflation",Paths.get("/Users/" + System.getenv("USER") + "/repo/flink/src/main/resources/Inflation_consumer_prices_annual_percentage.csv"),
      "PM2_5_air_pollution_per_cubic_meter",Paths.get("/Users/" + System.getenv("USER") + "/repo/flink/src/main/resources/PM2_5_air_pollution_annual_exposure_micrograms_per_cubic_meter.csv"),
      "Population",Paths.get("/Users/" + System.getenv("USER") + "/repo/flink/src/main/resources/Population_total.csv")
      );

  public static void main(String[] args) throws IOException {
    List<String> logs = new ArrayList<>();
    generate(logs);
    shuffle(logs);
    writeFile(logs);
  }

  private static void generate(List<String> logs) throws IOException {
    for (Map.Entry<String, Path> entry : sourceFiles.entrySet()) {
        logs.addAll(toLogs(entry.getKey(), entry.getValue()));
    }
  }

  private static List<String> toLogs(String indicator, Path path) throws IOException {
      List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);
      final List<String> logs = lines.stream()
          .map(line -> lineToEvents(indicator, line))
          .flatMap(it -> it.stream())
          .collect(Collectors.toList());
      return logs;
  }

  public static void writeFile(List<String> stringList) throws IOException {
    BufferedWriter br = new BufferedWriter(new FileWriter(OUTPUT_FILE_PATH));
    for (String str : stringList) {
      br.write(str + System.lineSeparator());
    }
    br.close();
  }

  public static List<String> lineToEvents(String indicator, String line) {
    List<String> ris = new ArrayList<>();
    try {
      final List<String> columns = toColumns(line);
      if (columns.size() > 4) {

        for (int i = 4; i < columns.size(); i++) {
          if (columns.get(i) == null)
            continue;
          String country = columns.get(0);
          if (header[0].equals(country))
            continue;


          Integer year = Integer.parseInt(header[i]);
          String countryCode = columns.get(1);
          Double value = Double.valueOf( columns.get(i).isEmpty() ? "0": columns.get(i));
          String id = countryCode + "_" + year;

          ris.add(new Event(id, year, country.replaceAll(",",""), countryCode, indicator, value, year+"-12-31T23:59:59.000").toString());
        }
      }

    } catch (Exception e) {
      System.err.println(e);
      System.err.println(line);
    }
    return ris;
  }

  private static List<String> toColumns(String line) {
    final String[] split = line.split("\\\",");
    return Arrays.stream(split)
        .map(it -> it.replaceAll("\\\"", ""))
        .collect(Collectors.toList());
  }
}
