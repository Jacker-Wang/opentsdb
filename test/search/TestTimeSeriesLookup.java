// This file is part of OpenTSDB.
// Copyright (C) 2014  The OpenTSDB Authors.
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
package net.opentsdb.search;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.lang.reflect.Field;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import net.opentsdb.core.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.storage.MockBase;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.Config;

import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
  "ch.qos.*", "org.slf4j.*",
  "com.sum.*", "org.xml.*"})
@PrepareForTest({TSDB.class, Config.class, UniqueId.class, HBaseClient.class, 
  KeyValue.class, Scanner.class, TimeSeriesLookup.class})
public class TestTimeSeriesLookup {
  private Config config;
  private TSDB tsdb = null;
  private HBaseClient client = mock(HBaseClient.class);
  private UniqueId metrics = mock(UniqueId.class);
  private UniqueId tag_names = mock(UniqueId.class);
  private UniqueId tag_values = mock(UniqueId.class);
  private MockBase storage = null;
  
  // tsuids
  private static List<byte[]> test_tsuids = new ArrayList<byte[]>(7);
  static {
    test_tsuids.add(new byte[] { 0, 0, 1, 0, 0, 1, 0, 0, 1 });
    test_tsuids.add(new byte[] { 0, 0, 1, 0, 0, 1, 0, 0, 2 });
    test_tsuids.add(new byte[] { 0, 0, 2, 0, 0, 1, 0, 0, 1 });
    test_tsuids.add(new byte[] { 0, 0, 3, 0, 0, 1, 0, 0, 1, 0, 0, 4, 0, 0, 5});
    test_tsuids.add(new byte[] { 0, 0, 3, 0, 0, 1, 0, 0, 2, 0, 0, 4, 0, 0, 5});
    test_tsuids.add(new byte[] { 0, 0, 3, 0, 0, 6, 0, 0, 7, 0, 0, 8, 0, 0, 1, 
        0, 0, 9, 0, 0, 3});
    test_tsuids.add(new byte[] { 0, 0, 3, 0, 0, 6, 0, 0, 7, 0, 0, 8, 0, 0, 10, 
        0, 0, 9, 0, 0, 3});
  }
  
  @Before
  public void before() throws Exception {
    PowerMockito.whenNew(HBaseClient.class)
    .withArguments(anyString(), anyString()).thenReturn(client); 
    config = new Config(false);
    tsdb = new TSDB(config);

    // replace the "real" field objects with mocks
    Field met = tsdb.getClass().getDeclaredField("metrics");
    met.setAccessible(true);
    met.set(tsdb, metrics);
    
    Field tagk = tsdb.getClass().getDeclaredField("tag_names");
    tagk.setAccessible(true);
    tagk.set(tsdb, tag_names);
    
    Field tagv = tsdb.getClass().getDeclaredField("tag_values");
    tagv.setAccessible(true);
    tagv.set(tsdb, tag_values);
    
    // mock UniqueId
    when(metrics.getId("sys.cpu.user")).thenReturn(new byte[] { 0, 0, 1 });
    when(metrics.getId("sys.cpu.system"))
      .thenThrow(new NoSuchUniqueName("sys.cpu.system", "metric"));
    when(metrics.getId("sys.cpu.nice")).thenReturn(new byte[] { 0, 0, 2 });
    when(metrics.getId("sys.cpu.idle")).thenReturn(new byte[] { 0, 0, 3 });
    when(metrics.getId("no.values")).thenReturn(new byte[] { 0, 0, 11 });
    
    when(tag_names.getId("host")).thenReturn(new byte[] { 0, 0, 1 });
    when(tag_names.getId("dc"))
      .thenThrow(new NoSuchUniqueName("dc", "metric"));
    when(tag_names.getId("owner")).thenReturn(new byte[] { 0, 0, 4 });
    
    when(tag_values.getId("web01")).thenReturn(new byte[] { 0, 0, 1 });
    when(tag_values.getId("web02")).thenReturn(new byte[] { 0, 0, 2 });
    when(tag_values.getId("web03"))
      .thenThrow(new NoSuchUniqueName("web03", "metric"));
    
    when(metrics.width()).thenReturn((short)3);
    when(tag_names.width()).thenReturn((short)3);
    when(tag_values.width()).thenReturn((short)3);
  }

  @Test
  public void metricOnlyMeta() throws Exception {
    generateMeta();
    final TimeSeriesLookup query = new TimeSeriesLookup(tsdb, "sys.cpu.user", 
        null);
    final List<byte[]> tsuids = query.lookup();
    assertNotNull(tsuids);
    assertEquals(2, tsuids.size());
    assertArrayEquals(test_tsuids.get(0), tsuids.get(0));
    assertArrayEquals(test_tsuids.get(1), tsuids.get(1));
  }
  
  @Test
  public void metricOnlyData() throws Exception {
    generateData();
    final TimeSeriesLookup query = new TimeSeriesLookup(tsdb, "sys.cpu.user", 
        null);
    query.setUseMeta(false);
    final List<byte[]> tsuids = query.lookup();
    assertNotNull(tsuids);
    assertEquals(2, tsuids.size());
    assertArrayEquals(test_tsuids.get(0), tsuids.get(0));
    assertArrayEquals(test_tsuids.get(1), tsuids.get(1));
  }
  
  @Test
  public void metricOnly2Meta() throws Exception {
    generateMeta();
    final TimeSeriesLookup query = new TimeSeriesLookup(tsdb, "sys.cpu.nice", 
        null);
    final List<byte[]> tsuids = query.lookup();
    assertNotNull(tsuids);
    assertEquals(1, tsuids.size());
    assertArrayEquals(test_tsuids.get(2), tsuids.get(0));
  }
  
  @Test
  public void metricOnly2Data() throws Exception {
    generateData();
    final TimeSeriesLookup query = new TimeSeriesLookup(tsdb, "sys.cpu.nice", 
        null);
    query.setUseMeta(false);
    final List<byte[]> tsuids = query.lookup();
    assertNotNull(tsuids);
    assertEquals(1, tsuids.size());
    assertArrayEquals(test_tsuids.get(2), tsuids.get(0));
  }
  
  @Test (expected = NoSuchUniqueName.class)
  public void noSuchMetricMeta() throws Exception {
    final TimeSeriesLookup query = new TimeSeriesLookup(tsdb, "sys.cpu.system", 
        null);
    query.lookup();
  }
  
  @Test
  public void metricOnlyNoValuesMeta() throws Exception {
    generateMeta();
    final TimeSeriesLookup query = new TimeSeriesLookup(tsdb, "no.values", 
        null);
    final List<byte[]> tsuids = query.lookup();
    assertNotNull(tsuids);
    assertEquals(0, tsuids.size());
  }
  
  @Test
  public void metricOnlyNoValuesData() throws Exception {
    generateData();
    final TimeSeriesLookup query = new TimeSeriesLookup(tsdb, "no.values", 
        null);
    query.setUseMeta(false);
    final List<byte[]> tsuids = query.lookup();
    assertNotNull(tsuids);
    assertEquals(0, tsuids.size());
  }
  
  @Test
  public void tagkOnlyMeta() throws Exception {
    generateMeta();
    final List<Map.Entry<String, String>> tags = 
        new ArrayList<Map.Entry<String, String>>(1);
    tags.add(new AbstractMap.SimpleEntry<String, String>("host", null));
    final TimeSeriesLookup query = new TimeSeriesLookup(tsdb, null, tags);
    final List<byte[]> tsuids = query.lookup();
    assertNotNull(tsuids);
    assertEquals(5, tsuids.size());
    for (int i = 0; i < 5; i++) {
      assertArrayEquals(test_tsuids.get(i), tsuids.get(i));
    }
  }
  
  @Test
  public void tagkOnlyData() throws Exception {
    generateData();
    final List<Map.Entry<String, String>> tags = 
        new ArrayList<Map.Entry<String, String>>(1);
    tags.add(new AbstractMap.SimpleEntry<String, String>("host", null));
    final TimeSeriesLookup query = new TimeSeriesLookup(tsdb, null, tags);
    query.setUseMeta(false);
    final List<byte[]> tsuids = query.lookup();
    assertNotNull(tsuids);
    assertEquals(5, tsuids.size());
    for (int i = 0; i < 5; i++) {
      assertArrayEquals(test_tsuids.get(i), tsuids.get(i));
    }
  }
  
  @Test
  public void tagkOnly2Meta() throws Exception {
    generateMeta();
    final List<Map.Entry<String, String>> tags = 
        new ArrayList<Map.Entry<String, String>>(1);
    tags.add(new AbstractMap.SimpleEntry<String, String>("owner", null));
    final TimeSeriesLookup query = new TimeSeriesLookup(tsdb, null, tags);
    final List<byte[]> tsuids = query.lookup();
    assertNotNull(tsuids);
    assertEquals(2, tsuids.size());
    assertArrayEquals(test_tsuids.get(3), tsuids.get(0));
    assertArrayEquals(test_tsuids.get(4), tsuids.get(1));
  }
  
  @Test
  public void tagkOnly2Data() throws Exception {
    generateData();
    final List<Map.Entry<String, String>> tags = 
        new ArrayList<Map.Entry<String, String>>(1);
    tags.add(new AbstractMap.SimpleEntry<String, String>("owner", null));
    final TimeSeriesLookup query = new TimeSeriesLookup(tsdb, null, tags);
    query.setUseMeta(false);
    final List<byte[]> tsuids = query.lookup();
    assertNotNull(tsuids);
    assertEquals(2, tsuids.size());
    assertArrayEquals(test_tsuids.get(3), tsuids.get(0));
    assertArrayEquals(test_tsuids.get(4), tsuids.get(1));
  }
  
  @Test (expected = NoSuchUniqueName.class)
  public void noSuchTagkMeta() throws Exception {
    final List<Map.Entry<String, String>> tags = 
        new ArrayList<Map.Entry<String, String>>(1);
    tags.add(new AbstractMap.SimpleEntry<String, String>("dc", null));
    final TimeSeriesLookup query = new TimeSeriesLookup(tsdb, null, tags);
    query.lookup();
  }
  
  @Test
  public void tagvOnlyMeta() throws Exception {
    generateMeta();
    final List<Map.Entry<String, String>> tags = 
        new ArrayList<Map.Entry<String, String>>(1);
    tags.add(new AbstractMap.SimpleEntry<String, String>(null, "web01"));
    final TimeSeriesLookup query = new TimeSeriesLookup(tsdb, null, tags);
    final List<byte[]> tsuids = query.lookup();
    assertNotNull(tsuids);
    assertEquals(4, tsuids.size());
    assertArrayEquals(test_tsuids.get(0), tsuids.get(0));
    assertArrayEquals(test_tsuids.get(2), tsuids.get(1));
    assertArrayEquals(test_tsuids.get(3), tsuids.get(2));
    assertArrayEquals(test_tsuids.get(5), tsuids.get(3));
  }
  
  @Test
  public void tagvOnlyData() throws Exception {
    generateData();
    final List<Map.Entry<String, String>> tags = 
        new ArrayList<Map.Entry<String, String>>(1);
    tags.add(new AbstractMap.SimpleEntry<String, String>(null, "web01"));
    final TimeSeriesLookup query = new TimeSeriesLookup(tsdb, null, tags);
    query.setUseMeta(false);
    final List<byte[]> tsuids = query.lookup();
    assertNotNull(tsuids);
    assertEquals(4, tsuids.size());
    assertArrayEquals(test_tsuids.get(0), tsuids.get(0));
    assertArrayEquals(test_tsuids.get(2), tsuids.get(1));
    assertArrayEquals(test_tsuids.get(3), tsuids.get(2));
    assertArrayEquals(test_tsuids.get(5), tsuids.get(3));
  }
  
  @Test
  public void tagvOnly2Meta() throws Exception {
    generateMeta();
    final List<Map.Entry<String, String>> tags = 
        new ArrayList<Map.Entry<String, String>>(1);
    tags.add(new AbstractMap.SimpleEntry<String, String>(null, "web02"));
    final TimeSeriesLookup query = new TimeSeriesLookup(tsdb, null, tags);
    final List<byte[]> tsuids = query.lookup();
    assertNotNull(tsuids);
    assertEquals(2, tsuids.size());
    assertArrayEquals(test_tsuids.get(1), tsuids.get(0));
    assertArrayEquals(test_tsuids.get(4), tsuids.get(1));
  }
  
  @Test
  public void tagvOnly2Data() throws Exception {
    generateData();
    final List<Map.Entry<String, String>> tags = 
        new ArrayList<Map.Entry<String, String>>(1);
    tags.add(new AbstractMap.SimpleEntry<String, String>(null, "web02"));
    final TimeSeriesLookup query = new TimeSeriesLookup(tsdb, null, tags);
    query.setUseMeta(false);
    final List<byte[]> tsuids = query.lookup();
    assertNotNull(tsuids);
    assertEquals(2, tsuids.size());
    assertArrayEquals(test_tsuids.get(1), tsuids.get(0));
    assertArrayEquals(test_tsuids.get(4), tsuids.get(1));
  }
  
  @Test (expected = NoSuchUniqueName.class)
  public void noSuchTagvMeta() throws Exception {
    final List<Map.Entry<String, String>> tags = 
        new ArrayList<Map.Entry<String, String>>(1);
    tags.add(new AbstractMap.SimpleEntry<String, String>(null, "web03"));
    final TimeSeriesLookup query = new TimeSeriesLookup(tsdb, null, tags);
    query.lookup();
  }
  
  @Test
  public void metricAndTagkMeta() throws Exception {
    generateMeta();
    final List<Map.Entry<String, String>> tags = 
        new ArrayList<Map.Entry<String, String>>(1);
    tags.add(new AbstractMap.SimpleEntry<String, String>("host", null));
    final TimeSeriesLookup query = new TimeSeriesLookup(tsdb, "sys.cpu.nice", 
        tags);
    final List<byte[]> tsuids = query.lookup();
    assertNotNull(tsuids);
    assertEquals(1, tsuids.size());
    assertArrayEquals(test_tsuids.get(2), tsuids.get(0));
  }
  
  @Test
  public void metricAndTagkData() throws Exception {
    generateData();
    final List<Map.Entry<String, String>> tags = 
        new ArrayList<Map.Entry<String, String>>(1);
    tags.add(new AbstractMap.SimpleEntry<String, String>("host", null));
    final TimeSeriesLookup query = new TimeSeriesLookup(tsdb, "sys.cpu.nice", 
        tags);
    query.setUseMeta(false);
    final List<byte[]> tsuids = query.lookup();
    assertNotNull(tsuids);
    assertEquals(1, tsuids.size());
    assertArrayEquals(test_tsuids.get(2), tsuids.get(0));
  }
  
  @Test
  public void metricAndTagvMeta() throws Exception {
    generateMeta();
    final List<Map.Entry<String, String>> tags = 
        new ArrayList<Map.Entry<String, String>>(1);
    tags.add(new AbstractMap.SimpleEntry<String, String>(null, "web02"));
    final TimeSeriesLookup query = new TimeSeriesLookup(tsdb, "sys.cpu.idle", 
        tags);
    final List<byte[]> tsuids = query.lookup();
    assertNotNull(tsuids);
    assertEquals(1, tsuids.size());
    assertArrayEquals(test_tsuids.get(4), tsuids.get(0));
  }
  
  @Test
  public void metricAndTagvData() throws Exception {
    generateData();
    final List<Map.Entry<String, String>> tags = 
        new ArrayList<Map.Entry<String, String>>(1);
    tags.add(new AbstractMap.SimpleEntry<String, String>(null, "web02"));
    final TimeSeriesLookup query = new TimeSeriesLookup(tsdb, "sys.cpu.idle", 
        tags);
    query.setUseMeta(false);
    final List<byte[]> tsuids = query.lookup();
    assertNotNull(tsuids);
    assertEquals(1, tsuids.size());
    assertArrayEquals(test_tsuids.get(4), tsuids.get(0));
  }
  
  @Test
  public void metricAndTagPairMeta() throws Exception {
    generateMeta();
    final List<Map.Entry<String, String>> tags = 
        new ArrayList<Map.Entry<String, String>>(1);
    tags.add(new AbstractMap.SimpleEntry<String, String>("host", "web01"));
    final TimeSeriesLookup query = new TimeSeriesLookup(tsdb, "sys.cpu.idle", 
        tags);
    final List<byte[]> tsuids = query.lookup();
    assertNotNull(tsuids);
    assertEquals(1, tsuids.size());
    assertArrayEquals(test_tsuids.get(3), tsuids.get(0));
  }
  
  @Test
  public void metricAndTagPairData() throws Exception {
    generateData();
    final List<Map.Entry<String, String>> tags = 
        new ArrayList<Map.Entry<String, String>>(1);
    tags.add(new AbstractMap.SimpleEntry<String, String>("host", "web01"));
    final TimeSeriesLookup query = new TimeSeriesLookup(tsdb, "sys.cpu.idle", 
        tags);
    query.setUseMeta(false);
    final List<byte[]> tsuids = query.lookup();
    assertNotNull(tsuids);
    assertEquals(1, tsuids.size());
    assertArrayEquals(test_tsuids.get(3), tsuids.get(0));
  }
  
  @Test
  public void tagPairOnlyMeta() throws Exception {
    generateMeta();
    final List<Map.Entry<String, String>> tags = 
        new ArrayList<Map.Entry<String, String>>(1);
    tags.add(new AbstractMap.SimpleEntry<String, String>("host", "web01"));
    final TimeSeriesLookup query = new TimeSeriesLookup(tsdb, null, tags);
    final List<byte[]> tsuids = query.lookup();
    assertNotNull(tsuids);
    assertEquals(3, tsuids.size());
    assertArrayEquals(test_tsuids.get(0), tsuids.get(0));
    assertArrayEquals(test_tsuids.get(2), tsuids.get(1));
    assertArrayEquals(test_tsuids.get(3), tsuids.get(2));
  }
  
  @Test
  public void tagPairOnlyData() throws Exception {
    generateData();
    final List<Map.Entry<String, String>> tags = 
        new ArrayList<Map.Entry<String, String>>(1);
    tags.add(new AbstractMap.SimpleEntry<String, String>("host", "web01"));
    final TimeSeriesLookup query = new TimeSeriesLookup(tsdb, null, tags);
    query.setUseMeta(false);
    final List<byte[]> tsuids = query.lookup();
    assertNotNull(tsuids);
    assertEquals(3, tsuids.size());
    assertArrayEquals(test_tsuids.get(0), tsuids.get(0));
    assertArrayEquals(test_tsuids.get(2), tsuids.get(1));
    assertArrayEquals(test_tsuids.get(3), tsuids.get(2));
  }
  
  // TODO test the dump to stdout
  
  /**
   * Stores some data in the mock tsdb-meta table for unit testing
   */
  private void generateMeta() {
    storage = new MockBase(tsdb, client, true, true, true, true);
    storage.setFamily("t".getBytes(MockBase.ASCII()));
    
    final byte[] val = new byte[] { 0, 0, 0, 0, 0, 0, 0, 1 };
    for (final byte[] tsuid : test_tsuids) {
      storage.addColumn(tsuid, TSMeta.COUNTER_QUALIFIER(), val);
    }
  }
  
  /**
   * Stores some data in the mock tsdb data table for unit testing
   */
  private void generateData() {
    storage = new MockBase(tsdb, client, true, true, true, true);
    storage.setFamily("t".getBytes(MockBase.ASCII()));
    
    final byte[] qual = new byte[] { 0, 0 };
    final byte[] val = new byte[] { 1 };
    for (final byte[] tsuid : test_tsuids) {
      byte[] row_key = new byte[tsuid.length + Const.TIMESTAMP_BYTES];
      System.arraycopy(tsuid, 0, row_key, 0, TSDB.metrics_width());
      System.arraycopy(tsuid, TSDB.metrics_width(), row_key, 
          TSDB.metrics_width() + Const.TIMESTAMP_BYTES, 
          tsuid.length - TSDB.metrics_width());
      storage.addColumn(row_key, qual, val);
    }
  }
}
