// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.data.types.numeric;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.utils.Bytes;


public class NumericMillisecondShard2 implements TimeSeriesDataSource, 
    Iterable<TimeSeriesValue<?>> {
  
  /** The *width* of the data (in ms) to be stored in this shard so we can 
   * calculate how many bytes are needed to store offsets from the base time. */
  private final long span;
  
  /** How many bytes to encode the offset one from 1 to 8. */
  private final byte encode_on;
  
  /** An order if shard is part of a slice config. */
  private final int order;
  
  /** The base timestamp for the shard (the first timestamp added). */
  private TimeStamp start_timestamp;
  
  /** The end timestamp for the shard (the last inclusive timestamp). */
  private TimeStamp end_timestamp;
  
  /** Index's for the write and read paths over the array. */
  private int write_offset_idx;
  private int write_value_idx;
  
  /** The time offsets and real + value flags. */
  private byte[] offsets;
  
  /** The real counts and values. */
  private byte[] values;
  
  /** The last timestamp recorded to track dupes and OOO data. */
  private long last_timestamp;
  
  /** Whether or not the shard was copied. */
  private boolean copied;
  
  /**
   * Default ctor that sizes the arrays for 1 value.
   * 
   * @param start The start of the data shard (base time).
   * @param end The end of the data shard.
   * @throws IllegalArgumentException if the ID was null or span was less than 1.
   */
  public NumericMillisecondShard2(final TimeStamp start,
                                  final TimeStamp end) {
    this(start, end, -1, 1);
  }
  
  /**
   * Default ctor that sizes the arrays for the given count. If the count is zero
   * then the arrays will be initialized empty.
   * 
   * @param start The start of the data shard (base time).
   * @param end The end of the data shard.
   * @param order An optional order within a slice config.
   * @throws IllegalArgumentException if the ID was null or span was less than
   * 1 or the count was less than zero.
   */
  public NumericMillisecondShard2(final TimeStamp start,
                                  final TimeStamp end, 
                                  final int order) {
    this(start, end, order, 1);
  }
  
  /**
   * Default ctor that sizes the arrays for the given count. If the count is zero
   * then the arrays will be initialized empty.
   * 
   * @param start The start of the data shard (base time).
   * @param end The end of the data shard.
   * @param order An optional order within a slice config.
   * @param count The expected number of values in the set.
   * @throws IllegalArgumentException if the ID was null or span was less than
   * 1 or the count was less than zero.
   */
  public NumericMillisecondShard2(final TimeStamp start,
                                  final TimeStamp end, 
                                  final int order, 
                                  final int count) {
    if (start == null) {
      throw new IllegalArgumentException("Start cannot be null");
    }
    if (end == null) {
      throw new IllegalArgumentException("End cannot be null");
    }
    if (count < 0) {
      throw new IllegalArgumentException("Count cannot be less than zero.");
    }
    this.start_timestamp = start;
    this.end_timestamp = end;
    this.order = order;
    last_timestamp = Long.MIN_VALUE;
    span = end.msEpoch() - start.msEpoch();
    encode_on = NumericType.encodeOn(span, NumericType.TOTAL_FLAG_BITS);
    offsets = new byte[count * encode_on];
    values = new byte[count * 4]; // may be too large or too small.
  }
  
  /**
   * Add a value to the shard. The value's timestamp must be greater than the
   * previously stored value.
   * @param timestamp A timestamp in Unix Epoch milliseconds.
   * @param value A signed integer value.
   * @throws IllegalStateException if the shard has been copied and is no longer
   * accepting values.
   */
  public void add(final long timestamp, final long value) {
    if (copied) {
      throw new IllegalStateException("Cannot add data after the shard has "
          + "been copied.");
    }
    if (timestamp <= last_timestamp) {
      throw new IllegalArgumentException("Timestamp " + timestamp + " must be "
          + "greater than last time: " + last_timestamp);
    }
    if (timestamp < start_timestamp.msEpoch()) {
      throw new IllegalArgumentException("Timestamp " + timestamp + " must be "
          + "greater than or equal to the start time: " + start_timestamp);
    }
    if (timestamp > end_timestamp.msEpoch()) {
      throw new IllegalArgumentException("Timestamp " + timestamp + " must be "
          + "less than or equal to the end time: " + end_timestamp);
    }
    last_timestamp = timestamp;
    final byte[] vle = NumericType.vleEncodeLong(value);
    final byte flags = (byte) ((vle.length - 1));
    final byte[] offset = Bytes.fromLong(
       (((timestamp - start_timestamp.msEpoch()) << NumericType.TOTAL_FLAG_BITS) | flags));
    add(offset, vle);
  }
  
  /**
   * Add a value to the shard. The value's timestamp must be greater than the
   * previously stored value.
   * NOTE: Don't write a double 0 or you'll waste 8 bytes. Use the long zero.
   * @param timestamp A timestamp in Unix Epoch milliseconds.
   * @param value A signed floating point value. If it can fit within a single
   * precision encoding, the value will be converted.
   * @throws IllegalStateException if the shard has been copied and is no longer
   * accepting values.
   */
  public void add(final long timestamp, final double value) {
    if (copied) {
      throw new IllegalStateException("Cannot add data after the shard has "
          + "been copied.");
    }
    if (timestamp <= last_timestamp) {
      throw new IllegalArgumentException("Timestamp " + timestamp + " must be "
          + "greater than last time: " + last_timestamp);
    }
    if (timestamp < start_timestamp.msEpoch()) {
      throw new IllegalArgumentException("Timestamp " + timestamp + " must be "
          + "greater than or equal to the start time: " + start_timestamp);
    }
    if (timestamp > end_timestamp.msEpoch()) {
      throw new IllegalArgumentException("Timestamp " + timestamp + " must be "
          + "less than or equal  to the end time: " + end_timestamp);
    }
    last_timestamp = timestamp;
    final byte[] vle = NumericType.fitsInFloat(value) ? 
        Bytes.fromInt(Float.floatToIntBits((float) value)) :
          Bytes.fromLong(Double.doubleToLongBits(value));
    final byte flags = (byte) ((vle.length - 1) 
        | NumericType.FLAG_FLOAT);
    final byte[] offset = Bytes.fromLong(
       (((timestamp - start_timestamp.msEpoch()) << NumericType.TOTAL_FLAG_BITS) | flags));
    add(offset, vle);
  }
  
  /**
   * Expands the array as necessary and writes the encoded offset and values
   * to the arrays.
   * @param offset A non-null encoded offset.
   * @param value A non-null encoded value.
   */
  private void add(final byte[] offset, final byte[] value) {
    while (write_offset_idx + encode_on >= offsets.length) {
      final byte[] offset_copy = new byte[offsets.length * 2];
      System.arraycopy(offsets, 0, offset_copy, 0, offsets.length);
      offsets = offset_copy;
    }
    
    while (write_value_idx + value.length >= values.length) {
      final byte[] values_copy = new byte[values.length * 2];
      System.arraycopy(values, 0, values_copy, 0, values.length);
      values = values_copy;
    }
    
    System.arraycopy(offset, 8 - encode_on, offsets, write_offset_idx, encode_on);
    write_offset_idx += encode_on;
    System.arraycopy(value, 0, values, write_value_idx, value.length);
    write_value_idx += value.length;
  }
  
  public Iterator<TimeSeriesValue<?>> iterator() {
    return new LocalIterator();
  }
  
  protected class LocalIterator implements Iterator<TimeSeriesValue<?>>,
                                           TimeSeriesValue<NumericType> {
    private int read_offset_idx;
    private int read_value_idx;
    private int write_idx;
    private MutableNumericType dp;
    private TimeStamp timestamp;
    
    protected LocalIterator() {
      dp = new MutableNumericType();
      timestamp = new MillisecondTimeStamp(0);
      write_idx = write_offset_idx;
    }
    
    @Override
    public boolean hasNext() {
      return read_offset_idx < write_idx;
    }

    @Override
    public TimeSeriesValue<NumericType> next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      final byte[] offset_copy = new byte[8];
      System.arraycopy(offsets, read_offset_idx, offset_copy, 8 - encode_on, encode_on);
      long offset = Bytes.getLong(offset_copy);
      final byte flags = (byte) offset;
      offset = offset >> NumericType.TOTAL_FLAG_BITS;
      final byte vlen = (byte) ((flags & NumericType.VALUE_LENGTH_MASK) + 1);
      timestamp.updateMsEpoch(start_timestamp.msEpoch() + offset);
      
      if ((flags & NumericType.FLAG_FLOAT) == 0x0) {
        dp.reset(timestamp, NumericType.extractIntegerValue(values, 
            read_value_idx, flags));
      } else {
        dp.reset(timestamp, NumericType.extractFloatingPointValue(values, 
            read_value_idx, flags));
      }
      read_offset_idx += encode_on;
      read_value_idx += vlen;
      
      return this;
    }

    @Override
    public TimeStamp timestamp() {
      return dp.timestamp();
    }

    @Override
    public NumericType value() {
      return dp;
    }

    @Override
    public TypeToken<NumericType> type() {
      return NumericType.TYPE;
    }
    
  }
  
  public TimeStamp startTime() {
    return start_timestamp;
  }
  
  public TimeStamp endTime() {
    return end_timestamp;
  }
  
  /**
   * Writes the raw values and offsets to the stream. Does NOT write the ID or 
   * order.
   * @param stream A non-null stream to write to.
   */
  public void serialize(final OutputStream stream) {
    try {
      stream.write(Bytes.fromLong(start_timestamp.msEpoch()));
      stream.write(Bytes.fromLong(end_timestamp.msEpoch()));
      stream.write(Bytes.fromInt(write_offset_idx));
      stream.write(offsets, 0, write_offset_idx);
      stream.write(Bytes.fromInt(write_value_idx));
      stream.write(values, 0, write_value_idx);
    } catch (IOException e) {
      throw new RuntimeException("WTF?", e);
    }
  }
  
  /**
   * Helper to parse the raw arrays from a cache.
   * <b>Note:</b> The order is always -1 coming from cache.
   * @param id A non-null ID to associate with the newly created shard.
   * @param stream A non-null input stream.
   * @return An instantiated shard.
   */
  public static NumericMillisecondShard2 parseFrom(final TimeSeriesId id, 
                                                  final InputStream stream) {
    try {
      byte[] array = new byte[8];
      stream.read(array);
      long start_ts = Bytes.getLong(array);
      
      array = new byte[8];
      stream.read(array);
      long end_ts = Bytes.getLong(array);
      
      final NumericMillisecondShard2 shard = new NumericMillisecondShard2( 
          new MillisecondTimeStamp(start_ts), 
          new MillisecondTimeStamp(end_ts),
          -1);
      
      array = new byte[4];
      stream.read(array);
      shard.write_offset_idx = Bytes.getInt(array);
      
      array = new byte[shard.write_offset_idx];
      stream.read(array);
      shard.offsets = array;
      
      array = new byte[4];
      stream.read(array);
      shard.write_value_idx = Bytes.getInt(array);
      
      array = new byte[shard.write_value_idx];
      stream.read(array);
      shard.values = array;
      
      return shard;
    } catch (IOException e) {
      throw new RuntimeException("WTF?", e);
    }
  }
  
  public int order() {
    return order;
  }
  
  @VisibleForTesting
  byte[] offsets() {
    return offsets;
  }
  
  @VisibleForTesting
  byte[] values() {
    return values;
  }
  
  @VisibleForTesting
  byte encodeOn() {
    return encode_on;
  }
}
