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
package net.opentsdb.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;

import org.junit.Test;

public final class TestTSSubQuery {

  @Test
  public void constructor() {
    assertNotNull(new TSSubQuery());
  }
  
  // Getters and Setters ----------------
  @Test
  public void gsAggregator() {
    TSSubQuery sub = new TSSubQuery();
    sub.setAggregator("avg");
    assertEquals("avg", sub.getAggregator());
  }
  
  @Test
  public void gsMetric() {
    TSSubQuery sub = new TSSubQuery();
    sub.setMetric("sys.cpu.0");
    assertEquals("sys.cpu.0", sub.getMetric());
  }
  
  @Test
  public void gsgetTsuids() {
    TSSubQuery sub = new TSSubQuery();
    sub.setTsuids(new ArrayList<String>());
    assertNotNull(sub.getTsuids());
  }
  
  @Test
  public void gsgetTags() {
    TSSubQuery sub = new TSSubQuery();
    sub.setTags(new HashMap<String, String>());
    assertNotNull(sub.getTags());
  }
  
  @Test
  public void gsDownsample() {
    TSSubQuery sub = new TSSubQuery();
    sub.setDownsample("1h-avg");
    assertEquals("1h-avg", sub.getDownsample());
  }
  
  @Test
  public void gsRate() {
    TSSubQuery sub = new TSSubQuery();
    sub.setRate(true);
    assertTrue(sub.getRate());
  }
  
  @Test
  public void validate() {
    TSSubQuery sub = getMetricForValidate();
    sub.validateAndSetQuery();
    assertEquals("sys.cpu.0", sub.getMetric());
    assertEquals("*", sub.getTags().get("host"));
    assertEquals("lga", sub.getTags().get("dc"));
    assertEquals(Aggregators.SUM, sub.aggregator());
    assertEquals(Aggregators.AVG, sub.downsampler());
    assertEquals(300, sub.downsampleInterval());
  }
  
  @Test
  public void validateTS() {
    TSSubQuery sub = getMetricForValidate();
    sub.setMetric(null);
    ArrayList<String> tsuids = new ArrayList<String> (1);
    tsuids.add("ABCD");
    sub.setTsuids(tsuids);
    sub.validateAndSetQuery();
    assertNotNull(sub.getTsuids());
    assertEquals("*", sub.getTags().get("host"));
    assertEquals("lga", sub.getTags().get("dc"));
    assertEquals(Aggregators.SUM, sub.aggregator());
    assertEquals(Aggregators.AVG, sub.downsampler());
    assertEquals(300, sub.downsampleInterval());
  }
  
  @Test
  public void validateNoDS() {
    TSSubQuery sub = getMetricForValidate();
    sub.setDownsample(null);
    sub.validateAndSetQuery();
    assertEquals("sys.cpu.0", sub.getMetric());
    assertEquals("*", sub.getTags().get("host"));
    assertEquals("lga", sub.getTags().get("dc"));
    assertEquals(Aggregators.SUM, sub.aggregator());
    assertNull(sub.downsampler());
    assertEquals(0, sub.downsampleInterval());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void validateNullAgg() {
    TSSubQuery sub = getMetricForValidate();
    sub.setAggregator(null);
    sub.validateAndSetQuery();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void validateEmptyAgg() {
    TSSubQuery sub = getMetricForValidate();
    sub.setAggregator("");
    sub.validateAndSetQuery();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void validateBadAgg() {
    TSSubQuery sub = getMetricForValidate();
    sub.setAggregator("Notanagg");
    sub.validateAndSetQuery();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void validateNoMetricOrTsuids() {
    TSSubQuery sub = getMetricForValidate();
    sub.setMetric(null);
    sub.setTsuids(null);
    sub.validateAndSetQuery();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void validateNoMetricOrEmptyTsuids() {
    TSSubQuery sub = getMetricForValidate();
    sub.setMetric(null);
    sub.setTsuids(new ArrayList<String>());
    sub.validateAndSetQuery();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void validateBadDS() {
    TSSubQuery sub = getMetricForValidate();
    sub.setDownsample("bad");
    sub.validateAndSetQuery();
  }
  
  /**
   * Sets up an object with good, common values for testing the validation
   * function with an "m" type query (no tsuids). Each test can "set" the 
   * method it wants to fool with and call .validateAndSetQuery()
   * <b>Warning:</b> This method is also shared by {@link TestTSQuery} so be
   * careful if you change any values
   * @return A sub query object
   */
  public static TSSubQuery getMetricForValidate() {
    final TSSubQuery sub = new TSSubQuery();
    sub.setAggregator("sum");
    sub.setDownsample("5m-avg");
    sub.setMetric("sys.cpu.0");
    sub.setRate(false);
    final HashMap<String, String> tags = new HashMap<String, String>();
    tags.put("host", "*");
    tags.put("dc", "lga");
    sub.setTags(tags);
    return sub;
  }
}
