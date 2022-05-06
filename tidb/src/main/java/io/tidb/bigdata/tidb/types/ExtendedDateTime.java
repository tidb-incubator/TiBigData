package io.tidb.bigdata.tidb.types;

import java.sql.Timestamp;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/** Extend joda DateTime to support micro second */
public class ExtendedDateTime {

  private final DateTime dateTime;
  private final int microsOfMillis;
  private static final DateTimeZone LOCAL_TIME_ZOME = DateTimeZone.getDefault();
  private static final DateTimeFormatter DATE_TIME_FORMATTER =
      DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.S").withZone(LOCAL_TIME_ZOME);

  /**
   * if timestamp = 2019-11-11 11:11:11 123456, then dateTime = 2019-11-11 11:11:11 123
   * microInMillis = 456
   *
   * @param dateTime
   * @param microsOfMillis
   */
  public ExtendedDateTime(DateTime dateTime, int microsOfMillis) {
    this.dateTime = dateTime;
    this.microsOfMillis = microsOfMillis;
  }

  public ExtendedDateTime(DateTime dateTime) {
    this.dateTime = dateTime;
    this.microsOfMillis = 0;
  }

  public DateTime getDateTime() {
    return dateTime;
  }

  public int getMicrosOfSeconds() {
    return dateTime.getMillisOfSecond() * 1000 + microsOfMillis;
  }

  public int getMicrosOfMillis() {
    return microsOfMillis;
  }

  public Timestamp toTimeStamp() {
    Timestamp timestamp = Timestamp.valueOf(dateTime.toString(DATE_TIME_FORMATTER));
    timestamp.setNanos(dateTime.getMillisOfSecond() * 1000000 + microsOfMillis * 1000);
    return timestamp;
  }
}