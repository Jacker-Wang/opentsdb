// This file is part of OpenTSDB.
// Copyright (C) 2013  The OpenTSDB Authors.
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
package net.opentsdb.tools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.lang.reflect.Method;

import net.opentsdb.core.TSDB;
import net.opentsdb.storage.MockBase;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.Config;

import org.apache.zookeeper.proto.DeleteRequest;
import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.Bytes;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.Scanner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
  "ch.qos.*", "org.slf4j.*",
  "com.sum.*", "org.xml.*"})
@PrepareForTest({TSDB.class, Config.class, UniqueId.class, HBaseClient.class, 
  GetRequest.class, PutRequest.class, KeyValue.class, UidManager.class,
  Scanner.class, DeleteRequest.class, AtomicIncrementRequest.class })
public class TestUID {
  private Config config;
  private TSDB tsdb = null;
  private HBaseClient client = mock(HBaseClient.class);
  private MockBase storage;
  
  // names used for testing
  private byte[] NAME_FAMILY = "name".getBytes(MockBase.ASCII());
  private byte[] ID_FAMILY = "id".getBytes(MockBase.ASCII());
  private byte[] METRICS = "metrics".getBytes(MockBase.ASCII());
  private byte[] TAGK = "tagk".getBytes(MockBase.ASCII());
  private byte[] TAGV = "tagv".getBytes(MockBase.ASCII());
  
  private final static Method fsck;
  static {
    try {
      fsck = UidManager.class.getDeclaredMethod("fsck", HBaseClient.class, 
          byte[].class, boolean.class);
      fsck.setAccessible(true);
    } catch (Exception e) {
      throw new RuntimeException("Failed in static initializer", e);
    }
  }
  
  @Before
  public void before() throws Exception {
    config = new Config(false);
    tsdb = new TSDB(config);
    
    // replace the "real" field objects with mocks
//    Field cl = tsdb.getClass().getDeclaredField("client");
//    cl.setAccessible(true);
//    cl.set(tsdb, client);
//    
//    Field met = tsdb.getClass().getDeclaredField("metrics");
//    met.setAccessible(true);
//    met.set(tsdb, metrics);
//    
//    Field tagk = tsdb.getClass().getDeclaredField("tag_names");
//    tagk.setAccessible(true);
//    tagk.set(tsdb, tag_names);
//    
//    Field tagv = tsdb.getClass().getDeclaredField("tag_values");
//    tagv.setAccessible(true);
//    tagv.set(tsdb, tag_values);
//    
//    // mock UniqueId
//    when(metrics.getId("sys.cpu.user")).thenReturn(new byte[] {0, 0, 1 });
//    when(metrics.getName(new byte[] {0, 0, 1 })).thenReturn("sys.cpu.user");
//    when(metrics.getId("sys.cpu.system"))
//      .thenThrow(new NoSuchUniqueName("sys.cpu.system", "metric"));
//    when(metrics.getId("sys.cpu.nice")).thenReturn(new byte[] {0, 0, 2 });
//    when(metrics.getName(new byte[] {0, 0, 2 })).thenReturn("sys.cpu.nice");
//    when(tag_names.getId("host")).thenReturn(new byte[] {0, 0, 1 });
//    when(tag_names.getName(new byte[] {0, 0, 1 })).thenReturn("host");
//    when(tag_names.getOrCreateId("host")).thenReturn(new byte[] {0, 0, 1 });
//    when(tag_names.getId("dc")).thenThrow(new NoSuchUniqueName("dc", "metric"));
//    when(tag_values.getId("web01")).thenReturn(new byte[] {0, 0, 1 });
//    when(tag_values.getName(new byte[] {0, 0, 1 })).thenReturn("web01");
//    when(tag_values.getOrCreateId("web01")).thenReturn(new byte[] {0, 0, 1 });
//    when(tag_values.getId("web02")).thenReturn(new byte[] {0, 0, 2 });
//    when(tag_values.getName(new byte[] {0, 0, 2 })).thenReturn("web02");
//    when(tag_values.getOrCreateId("web02")).thenReturn(new byte[] {0, 0, 2 });
//    when(tag_values.getId("web03"))
//      .thenThrow(new NoSuchUniqueName("web03", "metric"));
//    
//    when(metrics.width()).thenReturn((short)3);
//    when(tag_names.width()).thenReturn((short)3);
//    when(tag_values.width()).thenReturn((short)3);
  }
  
  /* FSCK --------------------------------------------
   * The UID FSCK is concerned with making sure the UID table is in a clean state.
   * Most important are the forward mappings to UIDs as that's what's used to
   * store time series. 
   * A clean table is below:
   * ---------------------
   * REVERSE   |  FORWARD
   * ---------------------
   * 01 -> foo   foo -> 01
   * 02 -> bar   bar -> 02
   * 
   * The reverse map will have a unique set of UIDs but could, in error, have
   * duplicate names.
   * 
   * The forward map will have a unique set of names but could, in error, have
   * duplicate IDs.
   * 
   * Order of error checking is to loop through the FORWARD map first, then
   * the REVERSE map. Then for each it will check the other map for entries
   */
  
  @Test
  public void fsckNoData() throws Exception {
    setupMockBase();
    storage.flushStorage();
    int errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), false);
    assertEquals(0, errors);
  }
  
  @Test
  public void fsckNoErrors() throws Exception {
    setupMockBase();
    int errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), false);
    assertEquals(0, errors);
  }
  
  /*
   * Max UID row values that are higher than the largest assigned UID for their 
   * type are OK and we just warn on them. This is usually caused by a user 
   * removing a name that they no longer need.
   */ 
  @Test
  public void fsckMetricsUIDHigh() throws Exception {
    // currently a warning, not an error
    setupMockBase();
    storage.addColumn(new byte[] { 0 }, ID_FAMILY, METRICS, Bytes.fromLong(42L));
    int errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), false);
    assertEquals(0, errors);
  }
  
  @Test
  public void fsckTagkUIDHigh() throws Exception {
    // currently a warning, not an error
    setupMockBase();
    storage.addColumn(new byte[] { 0 }, ID_FAMILY, TAGK, Bytes.fromLong(42L));
    int errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), false);
    assertEquals(0, errors);
  }
  
  @Test
  public void fsckTagvUIDHigh() throws Exception {
    // currently a warning, not an error
    setupMockBase();
    storage.addColumn(new byte[] { 0 }, ID_FAMILY, TAGV, Bytes.fromLong(42L));
    int errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), false);
    assertEquals(0, errors);
  }
  
  /*
   * Max UID row values that are lower than the largest assigned UID for their 
   * type can be fixed by simply setting the max ID to the largest found UID.
   */  
  @Test
  public void fsckMetricsUIDLow() throws Exception {
    // currently a warning, not an error
    setupMockBase();
    storage.addColumn(new byte[] { 0 }, ID_FAMILY, METRICS, Bytes.fromLong(1L));
    int errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), false);
    assertEquals(1, errors);
  }
  
  @Test
  public void fsckFIXMetricsUIDLow() throws Exception {
    // currently a warning, not an error
    setupMockBase();
    storage.addColumn(new byte[] { 0 }, ID_FAMILY, METRICS, Bytes.fromLong(0L));
    int errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), true);
    assertEquals(1, errors);
    errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), false);
    assertEquals(0, errors);
  }
  
  @Test
  public void fsckTagkUIDLow() throws Exception {
    setupMockBase();
    storage.addColumn(new byte[] { 0 }, ID_FAMILY, TAGK, Bytes.fromLong(1L));
    int errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), false);
    assertEquals(1, errors);
  }
  
  @Test
  public void fsckFIXTagkUIDLow() throws Exception {
    setupMockBase();
    storage.addColumn(new byte[] { 0 }, ID_FAMILY, TAGK, Bytes.fromLong(1L));
    int errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), true);
    assertEquals(1, errors);
    errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), false);
    assertEquals(0, errors);
  }
  
  @Test
  public void fsckTagvUIDLow() throws Exception {
    setupMockBase();
    storage.addColumn(new byte[] { 0 }, ID_FAMILY, TAGV, Bytes.fromLong(1L));
    int errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), false);
    assertEquals(1, errors);
  }
  
  @Test
  public void fsckFIXTagvUIDLow() throws Exception {
    setupMockBase();
    storage.addColumn(new byte[] { 0 }, ID_FAMILY, TAGV, Bytes.fromLong(1L));
    int errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), true);
    assertEquals(1, errors);
    errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), false);
    assertEquals(0, errors);
  }
  
  /*
   * Max UID row values that are != 8 bytes wide are bizzare.
   * TODO - a fix would be to find the max used ID for the type and store that
   * in the max row.
   */
  @Test
  public void fsckMetricsUIDWrongLength() throws Exception {
    setupMockBase();
    storage.addColumn(new byte[] { 0 }, ID_FAMILY, METRICS, Bytes.fromInt(3));
    int errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), false);
    assertEquals(2, errors);
  }
  
  @Test
  public void fsckTagkUIDWrongLength() throws Exception {
    setupMockBase();
    storage.addColumn(new byte[] { 0 }, ID_FAMILY, TAGK, Bytes.fromInt(3));
    int errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), false);
    assertEquals(2, errors);
  }
  
  @Test
  public void fsckTagvUIDWrongLength() throws Exception {
    setupMockBase();
    storage.addColumn(new byte[] { 0 }, ID_FAMILY, TAGV, Bytes.fromInt(3));
    int errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), false);
    assertEquals(2, errors);
  }
  
  /* #1 - Missing Reverse Mapping
   * - Forward mapping is missing reverse: bar -> 02
   * ---------------------
   * 01 -> foo   foo -> 01
   *             bar -> 02
   * 
   * FIX - Restore reverse map 02 -> bar. OK since 02 doesn't map to anything
   */
  @Test
  public void fsckMetricsMissingReverse() throws Exception {
    setupMockBase();
    storage.flushColumn(new byte[] {0, 0, 1}, NAME_FAMILY, METRICS);
    int errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), false);
    assertEquals(1, errors);
  }
  
  @Test
  public void fsckFIXMetricsMissingReverse() throws Exception {
    setupMockBase();
    storage.flushColumn(new byte[] {0, 0, 1}, NAME_FAMILY, METRICS);
    int errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), true);
    assertEquals(1, errors);
    assertArrayEquals("foo".getBytes(MockBase.ASCII()), 
        storage.getColumn(new byte [] {0, 0, 1}, NAME_FAMILY, METRICS));
    errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), false);
    assertEquals(0, errors);
  }
  
  @Test
  public void fsckTagkMissingReverse() throws Exception {
    setupMockBase();
    storage.flushColumn(new byte[] {0, 0, 1}, NAME_FAMILY, TAGK);
    int errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), false);
    assertEquals(1, errors);
  }
  
  @Test
  public void fsckFIXTagkMissingReverse() throws Exception {
    setupMockBase();
    storage.flushColumn(new byte[] {0, 0, 1}, NAME_FAMILY, TAGK);
    int errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), true);
    assertEquals(1, errors);
    assertArrayEquals("host".getBytes(MockBase.ASCII()), 
        storage.getColumn(new byte [] {0, 0, 1}, NAME_FAMILY, TAGK));
    errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), false);
    assertEquals(0, errors);
  }
  
  @Test
  public void fsckTagvMissingReverse() throws Exception {
    setupMockBase();
    storage.flushColumn(new byte[] {0, 0, 1}, NAME_FAMILY, TAGV);
    int errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), false);
    assertEquals(1, errors);
  }
  
  @Test
  public void fsckFIXTagvMissingReverse() throws Exception {
    setupMockBase();
    storage.flushColumn(new byte[] {0, 0, 1}, NAME_FAMILY, TAGV);
    int errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), true);
    assertEquals(1, errors);
    assertArrayEquals("web01".getBytes(MockBase.ASCII()), 
        storage.getColumn(new byte [] {0, 0, 1}, NAME_FAMILY, TAGV));
    errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), false);
    assertEquals(0, errors);
  }
  
  /* #2 - Inconsistent Forward where a name points to a previously assigned UID
   * THIS SHOULD NOT HAPPEN unless there's a major but or someone was messing with
   * the UID table.
   * - Forward mapping wtf -> 01 is diff than reverse 01 -> foo
   * - Inconsistent forward mapping wtf -> 01 vs wtf -> foo / foo -> 01
   * --------------------
   * 01 -> foo   foo -> 01
   * 02 -> bar   bar -> 02
   *             wtf -> 01
   *             ^^^^^^^^^
   * FIX - Since any time series with the "01" UID is now corrupted with data from
   * both foo and wtf, the best solution is to just delete the forward maps for
   * foo and wtf, then create a new name map of "fsck.foo.wtf -> 01" so that the
   * old time series are still accessible.
   */
  @Test
  public void fsckMetricsInconsistentForward() throws Exception {
    setupMockBase();
    storage.addColumn("wtf".getBytes(MockBase.ASCII()), ID_FAMILY, 
        METRICS, new byte[] {0, 0, 1});
    storage.addColumn(new byte[] { 0 }, ID_FAMILY, METRICS, Bytes.fromLong(3L));
    int errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), false);
    assertEquals(2, errors);
  }
  
  @Test
  public void fsckFIXMetricsInconsistentForward() throws Exception {
    setupMockBase();
    storage.addColumn("wtf".getBytes(MockBase.ASCII()), ID_FAMILY, 
        METRICS, new byte[] {0, 0, 1});
    storage.addColumn(new byte[] { 0 }, ID_FAMILY, METRICS, Bytes.fromLong(3L));
    int errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), true);
    assertEquals(2, errors);
    assertArrayEquals("fsck.foo.wtf".getBytes(MockBase.ASCII()), 
        storage.getColumn(new byte [] {0, 0, 1}, NAME_FAMILY, METRICS));
    assertNull(storage.getColumn("foo".getBytes(MockBase.ASCII()), ID_FAMILY, 
        METRICS));
    assertNull(storage.getColumn("wtf".getBytes(MockBase.ASCII()), ID_FAMILY, 
        METRICS));
    errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), false);
    assertEquals(0, errors);
  }

  @Test
  public void fsckTagkInconsistentForward() throws Exception {
    setupMockBase();
    storage.addColumn("some.other.value".getBytes(MockBase.ASCII()), ID_FAMILY, 
        TAGK, new byte[] {0, 0, 1});
    storage.addColumn(new byte[] { 0 }, ID_FAMILY, TAGK, Bytes.fromLong(3L));
    int errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), false);
    assertEquals(2, errors);
  }
  
  @Test
  public void fsckFIXTagkInconsistentForward() throws Exception {
    setupMockBase();
    storage.addColumn("wtf".getBytes(MockBase.ASCII()), ID_FAMILY, 
        TAGK, new byte[] {0, 0, 1});
    storage.addColumn(new byte[] { 0 }, ID_FAMILY, TAGK, Bytes.fromLong(3L));
    int errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), true);
    assertEquals(2, errors);
    assertArrayEquals("fsck.host.wtf".getBytes(MockBase.ASCII()), 
        storage.getColumn(new byte [] {0, 0, 1}, NAME_FAMILY, TAGK));
    assertNull(storage.getColumn("host".getBytes(MockBase.ASCII()), ID_FAMILY, 
        TAGK));
    assertNull(storage.getColumn("wtf".getBytes(MockBase.ASCII()), ID_FAMILY, 
        TAGK));
    errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), false);
    assertEquals(0, errors);
  }
  
  @Test
  public void fsckTagvInconsistentForward() throws Exception {
    setupMockBase();
    storage.addColumn("some.other.value".getBytes(MockBase.ASCII()), ID_FAMILY, 
        TAGV, new byte[] {0, 0, 1});
    storage.addColumn(new byte[] { 0 }, ID_FAMILY, TAGV, Bytes.fromLong(3L));
    int errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), false);
    assertEquals(2, errors);
  }
  
  @Test
  public void fsckFIXTagvInconsistentForward() throws Exception {
    setupMockBase();
    storage.addColumn("wtf".getBytes(MockBase.ASCII()), ID_FAMILY, 
        TAGV, new byte[] {0, 0, 1});
    storage.addColumn(new byte[] { 0 }, ID_FAMILY, TAGV, Bytes.fromLong(3L));
    int errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), true);
    assertEquals(2, errors);
    assertArrayEquals("fsck.web01.wtf".getBytes(MockBase.ASCII()), 
        storage.getColumn(new byte [] {0, 0, 1}, NAME_FAMILY, TAGV));
    assertNull(storage.getColumn("web01".getBytes(MockBase.ASCII()), ID_FAMILY, 
        TAGV));
    assertNull(storage.getColumn("wtf".getBytes(MockBase.ASCII()), ID_FAMILY, 
        TAGV));
    errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), false);
    assertEquals(0, errors);
  }
  
  /* #3 - Duplicate Forward (really inconsistent) where the reverse map dosen't 
   * match the name of the forward map.
   * - Forward mapping bar -> 02 is diff than reverse: 02 -> wtf
   * - Duplicate forward mapping bar -> 02 and null -> wtf
   * - Reverse mapping missing forward mapping bar -> 02
   * ---------------------
   * 01 -> foo   foo -> 01
   * 02 -> wtf   bar -> 02
   *       ^^^
   * FIX - Restore reverse map 02 -> bar. wtf may have been deleted. It will be
   * reassigned the next time it's written.
   */
  @Test
  public void fsckMetricsDuplicateForward() throws Exception {
    setupMockBase();
    storage.addColumn(new byte[] {0, 0, 2}, NAME_FAMILY, 
        METRICS, "wtf".getBytes(MockBase.ASCII()));
    int errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), false);
    assertEquals(2, errors);
  }
  
  @Test
  public void fsckFIXMetricsDuplicateForward() throws Exception {
    setupMockBase();
    storage.addColumn(new byte[] {0, 0, 2}, NAME_FAMILY, 
        METRICS, "wtf".getBytes(MockBase.ASCII()));
    int errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), true);
    assertEquals(2, errors);
    assertArrayEquals("bar".getBytes(MockBase.ASCII()), 
        storage.getColumn(new byte [] {0, 0, 2}, NAME_FAMILY, METRICS));
    errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), false);
    assertEquals(0, errors);
  }
  
  @Test
  public void fsckTagkDuplicateForward() throws Exception {
    setupMockBase();
    storage.addColumn(new byte[] {0, 0, 2}, NAME_FAMILY, 
        TAGK, "wtf".getBytes(MockBase.ASCII()));
    int errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), false);
    assertEquals(2, errors);
  }
  
  @Test
  public void fsckFIXTagkDuplicateForward() throws Exception {
    setupMockBase();
    storage.addColumn(new byte[] {0, 0, 2}, NAME_FAMILY, 
        TAGK, "wtf".getBytes(MockBase.ASCII()));
    int errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), true);
    assertEquals(2, errors);
    assertArrayEquals("dc".getBytes(MockBase.ASCII()), 
        storage.getColumn(new byte [] {0, 0, 2}, NAME_FAMILY, TAGK));
    errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), false);
    assertEquals(0, errors);
  }
  
  @Test
  public void fsckTagvDuplicateForward() throws Exception {
    setupMockBase();
    storage.addColumn(new byte[] {0, 0, 2}, NAME_FAMILY, 
        TAGV, "wtf".getBytes(MockBase.ASCII()));
    int errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), false);
    assertEquals(2, errors);
  }
  
  @Test
  public void fsckFIXTagvDuplicateForward() throws Exception {
    setupMockBase();
    storage.addColumn(new byte[] {0, 0, 2}, NAME_FAMILY, 
        TAGV, "wtf".getBytes(MockBase.ASCII()));
    int errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), true);
    assertEquals(2, errors);
    assertArrayEquals("web02".getBytes(MockBase.ASCII()), 
        storage.getColumn(new byte [] {0, 0, 2}, NAME_FAMILY, TAGV));
    errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), false);
    assertEquals(0, errors);
  }
 
  /* #4 - Missing Forward Mapping
   * - Reverse mapping missing forward mapping bar -> 02
   * ---------------------
   * 01 -> foo   foo -> 01
   * 02 -> bar   
   * 
   * FIX - Restore forward map. OK since "bar" does not map to anything
   */
  @Test
  public void fsckMetricsMissingForward() throws Exception {
    // currently a warning, not an error
    setupMockBase();
    storage.flushColumn("bar".getBytes(MockBase.ASCII()), ID_FAMILY, 
        METRICS);
    int errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), false);
    assertEquals(0, errors);
  }
  
  @Test
  public void fsckFIXMetricsMissingForward() throws Exception {
    // currently a warning, not an error
    setupMockBase();
    storage.flushColumn("bar".getBytes(MockBase.ASCII()), ID_FAMILY, 
        METRICS);
    int errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), true);
    assertEquals(0, errors);
    assertNull(storage.getColumn(new byte [] {0, 0, 2}, NAME_FAMILY, METRICS));
  }
  
  @Test
  public void fsckTagkMissingForward() throws Exception {
    // currently a warning, not an error
    setupMockBase();
    storage.flushColumn("host".getBytes(MockBase.ASCII()), ID_FAMILY, TAGK);
    int errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), false);
    assertEquals(0, errors);
  }
  
  @Test
  public void fsckFIXTagkMissingForward() throws Exception {
    // currently a warning, not an error
    setupMockBase();
    storage.flushColumn("host".getBytes(MockBase.ASCII()), ID_FAMILY, TAGK);
    int errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), true);
    assertEquals(0, errors);
    assertNull(storage.getColumn(new byte [] {0, 0, 1}, NAME_FAMILY, TAGK));
  }
  
  @Test
  public void fsckTagvMissingForward() throws Exception {
    // currently a warning, not an error
    setupMockBase();
    storage.flushColumn("web01".getBytes(MockBase.ASCII()), ID_FAMILY, TAGV);
    int errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), false);
    assertEquals(0, errors);
  }
  
  @Test
  public void fsckFIXTagvMissingForward() throws Exception {
    // currently a warning, not an error
    setupMockBase();
    storage.flushColumn("web01".getBytes(MockBase.ASCII()), ID_FAMILY, TAGV);
    int errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), true);
    assertEquals(0, errors);
    assertNull(storage.getColumn(new byte [] {0, 0, 1}, NAME_FAMILY, TAGV));
  }
  
  /* #5 - Inconsistent Reverse Mapping
   * - Inconsistent reverse mapping 03 -> foo vs 01 -> foo / foo -> 01 
   * ---------------------
   * 01 -> foo   foo -> 01
   * 02 -> bar   bar -> 02
   * 03 -> foo   
   *       ^^^
   * FIX - Delete 03 reverse map. OK since nothing maps to 02.
   */
  @Test
  public void fsckMetricsInconsistentReverse() throws Exception {
    setupMockBase();
    storage.addColumn(new byte[] {0, 0, 3}, NAME_FAMILY, 
        METRICS, "foo".getBytes(MockBase.ASCII()));
    storage.addColumn(new byte[] { 0 }, ID_FAMILY, METRICS, Bytes.fromLong(3L));
    int errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), false);
    assertEquals(1, errors);
  }
  
  @Test
  public void fsckFIXMetricsInconsistentReverse() throws Exception {
    setupMockBase();
    storage.addColumn(new byte[] {0, 0, 3}, NAME_FAMILY, 
        METRICS, "foo".getBytes(MockBase.ASCII()));
    storage.addColumn(new byte[] { 0 }, ID_FAMILY, METRICS, Bytes.fromLong(3L));
    int errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), true);
    assertEquals(1, errors);
    assertNull(storage.getColumn(new byte [] {0, 0, 3}, NAME_FAMILY, METRICS));
    errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), false);
    assertEquals(0, errors);
  }
  
  @Test
  public void fsckTagkInconsistentReverse() throws Exception {
    setupMockBase();
    storage.addColumn(new byte[] {0, 0, 3}, NAME_FAMILY, 
        TAGK, "host".getBytes(MockBase.ASCII()));
    storage.addColumn(new byte[] { 0 }, ID_FAMILY, TAGK, Bytes.fromLong(3L));
    int errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), false);
    assertEquals(1, errors);
  }
  
  @Test
  public void fsckFIXTagkInconsistentReverse() throws Exception {
    setupMockBase();
    storage.addColumn(new byte[] {0, 0, 3}, NAME_FAMILY, 
        TAGK, "host".getBytes(MockBase.ASCII()));
    storage.addColumn(new byte[] { 0 }, ID_FAMILY, TAGK, Bytes.fromLong(3L));
    int errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), true);
    assertEquals(1, errors);
    assertNull(storage.getColumn(new byte [] {0, 0, 3}, NAME_FAMILY, TAGK));
    errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), false);
    assertEquals(0, errors);
  }
  
  @Test
  public void fsckTagvInconsistentReverse() throws Exception {
    setupMockBase();
    storage.addColumn(new byte[] {0, 0, 3}, NAME_FAMILY, 
        TAGV, "web01".getBytes(MockBase.ASCII()));
    storage.addColumn(new byte[] { 0 }, ID_FAMILY, TAGV, Bytes.fromLong(3L));
    int errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), false);
    assertEquals(1, errors);
  }
  
  @Test
  public void fsckFIXTagvInconsistentReverse() throws Exception {
    setupMockBase();
    storage.addColumn(new byte[] {0, 0, 3}, NAME_FAMILY, 
        TAGV, "web01".getBytes(MockBase.ASCII()));
    storage.addColumn(new byte[] { 0 }, ID_FAMILY, TAGV, Bytes.fromLong(3L));
    int errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), true);
    assertEquals(1, errors);
    assertNull(storage.getColumn(new byte [] {0, 0, 3}, NAME_FAMILY, TAGV));
    errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), false);
    assertEquals(0, errors);
  }
  
  /* #6 - Duplicate Reverse Mapping
   * - Forward mapping is missing reverse mapping: wtf -> 04 
   * - Duplicate reverse mapping 03 -> wtf and 04 -> null
   * ---------------------
   * 01 -> foo   foo -> 01
   * 02 -> bar   bar -> 02
   * 03 -> wtf   wtf -> 04
   *       ^^^
   * FIX - Delete 03 reverse map. wtf -> 04 will be fixed by creating a reverse
   * map for 04 -> wtf
   */
  @Test
  public void fsckMetricsDuplicateReverse() throws Exception {
    setupMockBase();
    storage.addColumn(new byte[] {0, 0, 3}, NAME_FAMILY, 
        METRICS, "wtf".getBytes(MockBase.ASCII()));
    storage.addColumn("wtf".getBytes(MockBase.ASCII()), ID_FAMILY, 
        METRICS, new byte[] {0, 0, 4});
    storage.addColumn(new byte[] { 0 }, ID_FAMILY, METRICS, Bytes.fromLong(4L));
    int errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), false);
    assertEquals(2, errors);
  }
  
  @Test
  public void fsckFIXMetricsDuplicateReverse() throws Exception {
    setupMockBase();
    storage.addColumn(new byte[] {0, 0, 3}, NAME_FAMILY, 
        METRICS, "wtf".getBytes(MockBase.ASCII()));
    storage.addColumn("wtf".getBytes(MockBase.ASCII()), ID_FAMILY, 
        METRICS, new byte[] {0, 0, 4});
    storage.addColumn(new byte[] { 0 }, ID_FAMILY, METRICS, Bytes.fromLong(4L));
    int errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), true);
    assertEquals(2, errors);
    assertNull(storage.getColumn(new byte [] {0, 0, 3}, NAME_FAMILY, METRICS));
    assertArrayEquals("wtf".getBytes(MockBase.ASCII()), 
        storage.getColumn(new byte [] {0, 0, 4}, NAME_FAMILY, METRICS));
    errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), false);
    assertEquals(0, errors);
  }
  
  @Test
  public void fsckTagkDuplicateReverse() throws Exception {
    setupMockBase();
    storage.addColumn(new byte[] {0, 0, 3}, NAME_FAMILY, 
        TAGK, "wtf".getBytes(MockBase.ASCII()));
    storage.addColumn("wtf".getBytes(MockBase.ASCII()), ID_FAMILY, 
        TAGK, new byte[] {0, 0, 4});
    storage.addColumn(new byte[] { 0 }, ID_FAMILY, TAGK, Bytes.fromLong(4L));
    int errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), false);
    assertEquals(2, errors);
  }
  
  @Test
  public void fsckFIXTagkDuplicateReverse() throws Exception {
    setupMockBase();
    storage.addColumn(new byte[] {0, 0, 3}, NAME_FAMILY, 
        TAGK, "wtf".getBytes(MockBase.ASCII()));
    storage.addColumn("wtf".getBytes(MockBase.ASCII()), ID_FAMILY, 
        TAGK, new byte[] {0, 0, 4});
    storage.addColumn(new byte[] { 0 }, ID_FAMILY, TAGK, Bytes.fromLong(4L));
    int errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), true);
    assertEquals(2, errors);
    assertNull(storage.getColumn(new byte [] {0, 0, 3}, NAME_FAMILY, TAGK));
    assertArrayEquals("wtf".getBytes(MockBase.ASCII()), 
        storage.getColumn(new byte [] {0, 0, 4}, NAME_FAMILY, TAGK));
    errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), false);
    assertEquals(0, errors);
  }
  
  @Test
  public void fsckTagvDuplicateReverse() throws Exception {
    setupMockBase();
    storage.addColumn(new byte[] {0, 0, 3}, NAME_FAMILY, 
        TAGV, "wtf".getBytes(MockBase.ASCII()));
    storage.addColumn("wtf".getBytes(MockBase.ASCII()), ID_FAMILY, 
        TAGV, new byte[] {0, 0, 4});
    storage.addColumn(new byte[] { 0 }, ID_FAMILY, TAGV, Bytes.fromLong(4L));
    int errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), false);
    assertEquals(2, errors);
  }
  
  @Test
  public void fsckFIXTagvDuplicateReverse() throws Exception {
    setupMockBase();
    storage.addColumn(new byte[] {0, 0, 3}, NAME_FAMILY, 
        TAGV, "wtf".getBytes(MockBase.ASCII()));
    storage.addColumn("wtf".getBytes(MockBase.ASCII()), ID_FAMILY, 
        TAGV, new byte[] {0, 0, 4});
    storage.addColumn(new byte[] { 0 }, ID_FAMILY, TAGV, Bytes.fromLong(4L));
    int errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), true);
    assertEquals(2, errors);
    assertNull(storage.getColumn(new byte [] {0, 0, 3}, NAME_FAMILY, TAGV));
    assertArrayEquals("wtf".getBytes(MockBase.ASCII()), 
        storage.getColumn(new byte [] {0, 0, 4}, NAME_FAMILY, TAGV));
    errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), false);
    assertEquals(0, errors);
  }
  
  /* ---------------- COMPOUND ERRORS --------------- 
   * (if the base fixes above work for all UID types, we only need to test
   *  one of the types below)
   * 
   * #7 - Inconsistent Forward And Duplicate Reverse
   * - Forward mapping missing reverse mapping: wtf -> 03
   * - Forward mapping bar -> 02 is diff than reverse mapping: 02 -> wtf
   * - Inconsistent forward mapping bar -> 02 vs bar -> wtf / wtf -> 03
   * - Duplicate reverse mapping 02 -> wtf and 03 -> null
   * ---------------------
   * 01 -> foo   foo -> 01
   * 02 -> wtf   bar -> 02
   *       ^^^   wtf -> 01
   *                    ^^
   * FIX - #1 covers the 02 -> wtf / bar -> 02 mismatch. #2 will fix wtf -> 01
   * and foo -> 01
   */
  @Test
  public void fsckMetricsInconsistentFwdAndDupeRev() throws Exception {
    setupMockBase();
    storage.addColumn(new byte[] {0, 0, 2}, NAME_FAMILY, 
        METRICS, "wtf".getBytes(MockBase.ASCII()));
    storage.addColumn("wtf".getBytes(MockBase.ASCII()), ID_FAMILY, 
        METRICS, new byte[] {0, 0, 1});
    storage.addColumn(new byte[] { 0 }, ID_FAMILY, METRICS, Bytes.fromLong(3L));
    int errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), false);
    assertEquals(5, errors);
  }
  
  @Test
  public void fsckFIXMetricsInconsistentFwdAndDupeRev() throws Exception {
    setupMockBase();
    storage.addColumn(new byte[] {0, 0, 2}, NAME_FAMILY, 
        METRICS, "wtf".getBytes(MockBase.ASCII()));
    storage.addColumn("wtf".getBytes(MockBase.ASCII()), ID_FAMILY, 
        METRICS, new byte[] {0, 0, 1});
    storage.addColumn(new byte[] { 0 }, ID_FAMILY, METRICS, Bytes.fromLong(3L));
    int errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), true);
    assertEquals(4, errors);
    assertArrayEquals("fsck.foo.wtf".getBytes(MockBase.ASCII()), 
        storage.getColumn(new byte [] {0, 0, 1}, NAME_FAMILY, METRICS));
    assertNull(storage.getColumn("foo".getBytes(MockBase.ASCII()), ID_FAMILY, 
        METRICS));
    assertNull(storage.getColumn("wtf".getBytes(MockBase.ASCII()), ID_FAMILY, 
        METRICS));
    assertArrayEquals("bar".getBytes(MockBase.ASCII()), 
        storage.getColumn(new byte [] {0, 0, 2}, NAME_FAMILY, METRICS));
    errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), false);
    assertEquals(0, errors);
  }
  
  /* #8 - Inconsistent Forward And Inconsistent Reverse
   * - Forward mapping wtf -> 01 is diff than reverse mapping: 01 -> foo
   * - Inconsistent forward mapping wtf -> 01 vs wtf -> foo / foo -> 01
   * - Forward mapping bar -> 02 is diff than reverse mapping: 02 -> wtf
   * - Inconsistent forward mapping bar -> 02 vs bar -> wtf / wtf -> 01
   * - Inconsistent reverse mapping 02 -> wtf vs 01 -> wtf / foo -> 01
   * - Inconsistent reverse mapping 03 -> foo vs 01 -> foo / foo -> 01
   * ---------------------
   * 01 -> foo   foo -> 01
   * 02 -> wtf   bar -> 02
   *       ^^^
   * 03 -> foo   wtf -> 01
   *       ^^^          ^^
   * 
   * FIX - Same as #2 && #3
   */
  @Test
  public void fsckMetricsInconsistentFwdAndInconsistentRev() throws Exception {
    setupMockBase();
    storage.addColumn(new byte[] {0, 0, 2}, NAME_FAMILY, 
        METRICS, "wtf".getBytes(MockBase.ASCII()));
    storage.addColumn("wtf".getBytes(MockBase.ASCII()), ID_FAMILY, 
        METRICS, new byte[] {0, 0, 1});
    storage.addColumn(new byte[] {0, 0, 3}, NAME_FAMILY, 
        METRICS, "foo".getBytes(MockBase.ASCII()));
    storage.addColumn(new byte[] { 0 }, ID_FAMILY, METRICS, Bytes.fromLong(3L));
    int errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), false);
    assertEquals(6, errors);
  }
  
  @Test
  public void fsckFIXMetricsInconsistentFwdAndInconsistentRev() throws Exception {
    setupMockBase();
    storage.addColumn(new byte[] {0, 0, 2}, NAME_FAMILY, 
        METRICS, "wtf".getBytes(MockBase.ASCII()));
    storage.addColumn("wtf".getBytes(MockBase.ASCII()), ID_FAMILY, 
        METRICS, new byte[] {0, 0, 1});
    storage.addColumn(new byte[] {0, 0, 3}, NAME_FAMILY, 
        METRICS, "foo".getBytes(MockBase.ASCII()));
    storage.addColumn(new byte[] { 0 }, ID_FAMILY, METRICS, Bytes.fromLong(3L));
    int errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), true);
    assertEquals(4, errors); // diff than above since we remove some forwards early
    assertArrayEquals("fsck.foo.wtf".getBytes(MockBase.ASCII()), 
        storage.getColumn(new byte [] {0, 0, 1}, NAME_FAMILY, METRICS));
    assertNull(storage.getColumn("foo".getBytes(MockBase.ASCII()), ID_FAMILY, 
        METRICS));
    assertNull(storage.getColumn("wtf".getBytes(MockBase.ASCII()), ID_FAMILY, 
        METRICS));
    assertArrayEquals("bar".getBytes(MockBase.ASCII()), 
        storage.getColumn(new byte [] {0, 0, 2}, NAME_FAMILY, METRICS));    
    errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), false);
    assertEquals(0, errors);
  }
  
  /* #9 - Inconsistent Forward, No Dupes
   * - Forward mapping bar -> 02 is different than reverse mapping: 02 -> wtf
   * - Inconsistent forward mapping bar -> 02 vs bar -> wtf / wtf -> 03
   * - Inconsistent reverse mapping 02 -> wtf vs 03 -> wtf / wtf -> 03
   * ---------------------
   * 01 -> foo   foo -> 01
   * 02 -> wtf   bar -> 02
   * 03 -> wtf   wtf -> 03
   * 
   * FIX - Remove reverse 02 -> wtf. Run again and restore 02 -> bar
   */
  @Test
  public void fsckMetricsInconsistentFwdNoDupes() throws Exception {
    setupMockBase();
    storage.addColumn(new byte[] {0, 0, 2}, NAME_FAMILY, 
        METRICS, "wtf".getBytes(MockBase.ASCII()));
    storage.addColumn("wtf".getBytes(MockBase.ASCII()), ID_FAMILY, 
        METRICS, new byte[] {0, 0, 3});
    storage.addColumn(new byte[] {0, 0, 3}, NAME_FAMILY, 
        METRICS, "wtf".getBytes(MockBase.ASCII()));
    storage.addColumn(new byte[] { 0 }, ID_FAMILY, METRICS, Bytes.fromLong(3L));
    int errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), false);
    assertEquals(3, errors);
  }
  
  @Test
  public void fsckFixMetricsInconsistentFwdNoDupes() throws Exception {
    setupMockBase();
    storage.addColumn(new byte[] {0, 0, 2}, NAME_FAMILY, 
        METRICS, "wtf".getBytes(MockBase.ASCII()));
    storage.addColumn("wtf".getBytes(MockBase.ASCII()), ID_FAMILY, 
        METRICS, new byte[] {0, 0, 3});
    storage.addColumn(new byte[] {0, 0, 3}, NAME_FAMILY, 
        METRICS, "wtf".getBytes(MockBase.ASCII()));
    storage.addColumn(new byte[] { 0 }, ID_FAMILY, METRICS, Bytes.fromLong(3L));
    int errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), true);
    assertEquals(2, errors);
    assertArrayEquals("bar".getBytes(MockBase.ASCII()), 
        storage.getColumn(new byte [] {0, 0, 2}, NAME_FAMILY, METRICS));    
    errors = (Integer)fsck.invoke(null, client, 
        "tsdb".getBytes(MockBase.ASCII()), true);
    assertEquals(0, errors);
  }
  
  /**
   * Write clean data to MockBase that can be overridden by individual unit tests
   */
  private void setupMockBase() {
    storage = new MockBase(tsdb, client, true, true, true, true);

    storage.addColumn(new byte[] { 0 }, ID_FAMILY, METRICS, Bytes.fromLong(2L));
    storage.addColumn(new byte[] { 0 }, ID_FAMILY, TAGK, Bytes.fromLong(2L));
    storage.addColumn(new byte[] { 0 }, ID_FAMILY, TAGV, Bytes.fromLong(2L));
    
    // forward mappings
    storage.addColumn("foo".getBytes(MockBase.ASCII()), ID_FAMILY, 
        METRICS, new byte[] {0, 0, 1});
    storage.addColumn("host".getBytes(MockBase.ASCII()), ID_FAMILY, 
        TAGK, new byte[] {0, 0, 1});
    storage.addColumn("web01".getBytes(MockBase.ASCII()), ID_FAMILY, 
        TAGV, new byte[] {0, 0, 1});
    
    storage.addColumn("bar".getBytes(MockBase.ASCII()), ID_FAMILY, 
        METRICS, new byte[] {0, 0, 2});
    storage.addColumn("dc".getBytes(MockBase.ASCII()), ID_FAMILY, 
        TAGK, new byte[] {0, 0, 2});
    storage.addColumn("web02".getBytes(MockBase.ASCII()), ID_FAMILY, 
        TAGV, new byte[] {0, 0, 2});
    
    // reverse mappings
    storage.addColumn(new byte[] {0, 0, 1}, NAME_FAMILY, 
        METRICS, "foo".getBytes(MockBase.ASCII()));
    storage.addColumn(new byte[] {0, 0, 1}, NAME_FAMILY, 
        TAGK, "host".getBytes(MockBase.ASCII()));
    storage.addColumn(new byte[] {0, 0, 1}, NAME_FAMILY, 
        TAGV, "web01".getBytes(MockBase.ASCII()));
    
    storage.addColumn(new byte[] {0, 0, 2}, NAME_FAMILY, 
        METRICS, "bar".getBytes(MockBase.ASCII()));
    storage.addColumn(new byte[] {0, 0, 2}, NAME_FAMILY, 
        TAGK, "dc".getBytes(MockBase.ASCII()));
    storage.addColumn(new byte[] {0, 0, 2}, NAME_FAMILY, 
        TAGV, "web02".getBytes(MockBase.ASCII()));
  }
}
