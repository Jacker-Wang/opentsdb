package net.opentsdb.query.processor.groupby;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Period;
import java.time.ZoneId;
import java.util.Iterator;
import java.util.Map;

import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.MockTimeSeries;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TimeStamp.RelationalOperator;
import net.opentsdb.data.types.numeric.Aggregators;
import net.opentsdb.data.types.numeric.MutableNumericSummaryValue;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.NumericMillisecondShard;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.TimeSeriesQuery;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.interpolation.types.numeric.DefaultInterpolationConfig;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorFactory;
import net.opentsdb.query.interpolation.types.numeric.NumericSummaryInterpolatorConfig;
import net.opentsdb.query.interpolation.types.numeric.ScalarNumericInterpolatorConfig;
import net.opentsdb.query.pojo.Downsampler;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.pojo.Metric;
import net.opentsdb.query.pojo.Timespan;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.rollup.RollupInterval;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Maps;

public class TestGroupByNumericSummaryIterator {
  private GroupByConfig config;
  private QueryNode node;
  private QueryContext query_context;
  private QueryPipelineContext pipeline_context;
  private MockTimeSeries<NumericSummaryType> ts1;
  private MockTimeSeries<NumericSummaryType> ts2;
  private MockTimeSeries<NumericSummaryType> ts3;
  private Map<String, TimeSeries> source_map;
  private NumericSummaryInterpolatorConfig interpolator_config;
  private RollupConfig rollup_config;
  
  private static final long BASE_TIME = 1356998400000L;
  
  @Before
  public void before() throws Exception {
    rollup_config = RollupConfig.builder()
        .addAggregationId("sum", 0)
        .addAggregationId("count", 2)
        .addAggregationId("avg", 5)
        .addInterval(RollupInterval.builder()
            .setInterval("sum")
            .setTable("tsdb")
            .setPreAggregationTable("tsdb")
            .setInterval("1h")
            .setRowSpan("1d"))
        .build();
    interpolator_config = NumericSummaryInterpolatorConfig.newBuilder()
        .setDefaultFillPolicy(FillPolicy.NONE)
        .setDefaultRealFillPolicy(FillWithRealPolicy.NONE)
        .addExpectedSummary(0)
        .addExpectedSummary(2)
        .setRollupConfig(rollup_config)
        .build();
    config = GroupByConfig.newBuilder()
        .setAggregator("sum")
        .setId("Testing")
        .addTagKey("dc")
        .setQueryInterpolationConfig(DefaultInterpolationConfig.newBuilder()
            .add(NumericSummaryType.TYPE, interpolator_config, 
                new NumericInterpolatorFactory.Default())
            .build())
        .build();
  }
  
  @Test
  public void nextAllPresent() throws Exception {
    long[] sums = new long[] { 10, 11, 12, 13, 21, 22, 23, 24, 31, 32, 33, 34 };
    long[] counts = new long[] { 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4 }; 
    setupData(sums, counts, false);
    setupMock();
    
    GroupByNumericSummaryIterator iterator = new GroupByNumericSummaryIterator(node, source_map);
    long ts = BASE_TIME;
    int i = 0;
    while (iterator.hasNext()) {
      TimeSeriesValue<NumericSummaryType> tsv = (TimeSeriesValue<NumericSummaryType>) iterator.next();
      print(tsv);
      assertEquals(ts, tsv.timestamp().msEpoch());
      assertEquals(sum(sums, i, false), tsv.value().value(0).longValue());
      assertEquals(sum(counts, i, false), tsv.value().value(2).longValue());
      ts += 3600 * 1000L;
      i++;
    }
    assertEquals(4, i);
  }
  
  @Test
  public void nextStaggeredMissing() throws Exception {
    long[] sums = new long[] { -1, 11, 12, -1, 21, 22, -1, 24, 31, 32, 33, 34 };
    long[] counts = new long[] { 1, 2, 3, 4, -1, 2, 3, -1, -1, 2, -1, 4 }; 
    setupData(sums, counts, false);
    setupMock();
    
    GroupByNumericSummaryIterator iterator = new GroupByNumericSummaryIterator(node, source_map);
    long ts = BASE_TIME;
    int i = 0;
    while (iterator.hasNext()) {
      TimeSeriesValue<NumericSummaryType> tsv = (TimeSeriesValue<NumericSummaryType>) iterator.next();
      print(tsv);
      assertEquals(ts, tsv.timestamp().msEpoch());
      long sum = sum(sums, i, false);
      if (sum < 0) {
        assertNull(tsv.value().value(0));
      } else {
        assertEquals(sum, tsv.value().value(0).longValue());
      }
      sum = sum(counts, i, false);
      if (sum < 0) {
        assertNull(tsv.value().value(2));
      } else {
        assertEquals(sum, tsv.value().value(2).longValue());
      }
      ts += 3600 * 1000L;
      i++;
    }
    assertEquals(4, i);
  }
  
  @Test
  public void nextStaggeredNaNs() throws Exception {
    long[] sums = new long[] { -1, 11, 12, -1, 21, 22, -1, 24, 31, 32, 33, 34 };
    long[] counts = new long[] { 1, 2, 3, 4, -1, 2, 3, -1, -1, 2, -1, 4 }; 
    setupData(sums, counts, true);
    setupMock();
    
    GroupByNumericSummaryIterator iterator = new GroupByNumericSummaryIterator(node, source_map);
    long ts = BASE_TIME;
    int i = 0;
    while (iterator.hasNext()) {
      TimeSeriesValue<NumericSummaryType> tsv = (TimeSeriesValue<NumericSummaryType>) iterator.next();
      print(tsv);
      assertEquals(ts, tsv.timestamp().msEpoch());
      long sum = sum(sums, i, false);
      if (sum < 0) {
        assertNull(tsv.value().value(0));
      } else {
        assertEquals(sum, tsv.value().value(0).longValue());
      }
      sum = sum(counts, i, false);
      if (sum < 0) {
        assertNull(tsv.value().value(2));
      } else {
        assertEquals(sum, tsv.value().value(2).longValue());
      }
      ts += 3600 * 1000L;
      i++;
    }
    assertEquals(4, i);
  }
  
  @Test
  public void nextStaggeredNaNsInfectiousNans() throws Exception {
    config = GroupByConfig.newBuilder()
        .setAggregator("sum")
        .setId("Testing")
        .addTagKey("dc")
        .setInfectiousNan(true)
        .setQueryInterpolationConfig(DefaultInterpolationConfig.newBuilder()
            .add(NumericSummaryType.TYPE, interpolator_config, 
                new NumericInterpolatorFactory.Default())
            .build())
        .build();
    
    long[] sums = new long[] { -1, 11, 12, -1, 21, 22, -1, 24, 31, 32, 33, 34 };
    long[] counts = new long[] { 1, 2, 3, 4, -1, 2, 3, -1, -1, 2, -1, 4 }; 
    setupData(sums, counts, true);
    setupMock();
    
    GroupByNumericSummaryIterator iterator = new GroupByNumericSummaryIterator(node, source_map);
    long ts = BASE_TIME;
    int i = 0;
    while (iterator.hasNext()) {
      TimeSeriesValue<NumericSummaryType> tsv = (TimeSeriesValue<NumericSummaryType>) iterator.next();
      print(tsv);
      assertEquals(ts, tsv.timestamp().msEpoch());
      long sum = sum(sums, i, true);
      if (sum < 0) {
        assertTrue(Double.isNaN(tsv.value().value(0).doubleValue()));
      } else {
        assertEquals(sum, tsv.value().value(0).longValue());
      }
      sum = sum(counts, i, true);
      if (sum < 0) {
        assertTrue(Double.isNaN(tsv.value().value(2).doubleValue()));
      } else {
        assertEquals(sum, tsv.value().value(2).longValue());
      }
      ts += 3600 * 1000L;
      i++;
    }
    assertEquals(4, i);
  }
  
  @Test
  public void nextNoSummariesStart() throws Exception {
    long[] sums = new long[] { -1, 11, 12, 13, -1, 22, 23, 24, -1, 32, 33, 34 };
    long[] counts = new long[] { -1, 2, 3, 4, -1, 2, 3, 4, -1, 2, 3, 4 }; 
    setupData(sums, counts, false);
    setupMock();
    
    GroupByNumericSummaryIterator iterator = new GroupByNumericSummaryIterator(node, source_map);
    long ts = BASE_TIME;
    int i = 0;
    while (iterator.hasNext()) {
      TimeSeriesValue<NumericSummaryType> tsv = (TimeSeriesValue<NumericSummaryType>) iterator.next();
      print(tsv);
      assertEquals(ts, tsv.timestamp().msEpoch());
      long sum = sum(sums, i, false);
      if (sum < 0) {
        assertNull(tsv.value().value(0));
      } else {
        assertEquals(sum, tsv.value().value(0).longValue());
      }
      sum = sum(counts, i, false);
      if (sum < 0) {
        assertNull(tsv.value().value(2));
      } else {
        assertEquals(sum, tsv.value().value(2).longValue());
      }
      ts += 3600 * 1000L;
      i++;
    }
    assertEquals(4, i);
  }
  
  @Test
  public void nextNoSummariesEnd() throws Exception {
    long[] sums = new long[] { 10, 11, 12, -1, 22, 22, 23, -1, 31, 32, 33, -1 };
    long[] counts = new long[] { 1, 2, 3, -1, 1, 2, 3, -1, 1, 2, 3, -1 }; 
    setupData(sums, counts, false);
    setupMock();
    
    GroupByNumericSummaryIterator iterator = new GroupByNumericSummaryIterator(node, source_map);
    long ts = BASE_TIME;
    int i = 0;
    while (iterator.hasNext()) {
      TimeSeriesValue<NumericSummaryType> tsv = (TimeSeriesValue<NumericSummaryType>) iterator.next();
      print(tsv);
      assertEquals(ts, tsv.timestamp().msEpoch());
      long sum = sum(sums, i, false);
      if (sum < 0) {
        assertNull(tsv.value().value(0));
      } else {
        assertEquals(sum, tsv.value().value(0).longValue());
      }
      sum = sum(counts, i, false);
      if (sum < 0) {
        assertNull(tsv.value().value(2));
      } else {
        assertEquals(sum, tsv.value().value(2).longValue());
      }
      ts += 3600 * 1000L;
      i++;
    }
    assertEquals(4, i);
  }
  
  @Test
  public void nextNoSummariesMiddle() throws Exception {
    long[] sums = new long[] { 10, -1, 12, 13, 22, -1, 23, 24, 31, -1, 33, 34 };
    long[] counts = new long[] { 1, -1, 3, 4, 1, -1, 3, 4, 1, -1, 3, 4 }; 
    setupData(sums, counts, false);
    setupMock();
    
    GroupByNumericSummaryIterator iterator = new GroupByNumericSummaryIterator(node, source_map);
    long ts = BASE_TIME;
    int i = 0;
    while (iterator.hasNext()) {
      TimeSeriesValue<NumericSummaryType> tsv = (TimeSeriesValue<NumericSummaryType>) iterator.next();
      print(tsv);
      assertEquals(ts, tsv.timestamp().msEpoch());
      long sum = sum(sums, i, false);
      if (sum < 0) {
        assertNull(tsv.value().value(0));
      } else {
        assertEquals(sum, tsv.value().value(0).longValue());
      }
      sum = sum(counts, i, false);
      if (sum < 0) {
        assertNull(tsv.value().value(2));
      } else {
        assertEquals(sum, tsv.value().value(2).longValue());
      }
      ts += 3600 * 1000L;
      i++;
    }
    assertEquals(4, i);
  }
  
  @Test
  public void nextNoSummariesStartFillInfectiousNan() throws Exception {
    interpolator_config = NumericSummaryInterpolatorConfig.newBuilder()
        .setDefaultFillPolicy(FillPolicy.NOT_A_NUMBER)
        .setDefaultRealFillPolicy(FillWithRealPolicy.NONE)
        .addExpectedSummary(0)
        .addExpectedSummary(2)
        .build();
    config = GroupByConfig.newBuilder()
        .setAggregator("sum")
        .setId("Testing")
        .addTagKey("dc")
        .setInfectiousNan(true)
        .setQueryInterpolationConfig(DefaultInterpolationConfig.newBuilder()
            .add(NumericSummaryType.TYPE, interpolator_config, 
                new NumericInterpolatorFactory.Default())
            .build())
        .build();
    
    long[] sums = new long[] { -1, 11, 12, 13, -1, 22, 23, 24, -31, 32, 33, 34 };
    long[] counts = new long[] { -1, 2, 3, 4, 1, 2, 3, 4, -1, 2, 3, 4 }; 
    setupData(sums, counts, false);
    setupMock();
    
    GroupByNumericSummaryIterator iterator = new GroupByNumericSummaryIterator(node, source_map);
    long ts = BASE_TIME;
    int i = 0;
    while (iterator.hasNext()) {
      TimeSeriesValue<NumericSummaryType> tsv = (TimeSeriesValue<NumericSummaryType>) iterator.next();
      print(tsv);
      assertEquals(ts, tsv.timestamp().msEpoch());
      long sum = sum(sums, i, true);
      if (sum < 0) {
        assertTrue(Double.isNaN(tsv.value().value(0).doubleValue()));
      } else {
        assertEquals(sum, tsv.value().value(0).longValue());
      }
      sum = sum(counts, i, true);
      if (sum < 0) {
        assertTrue(Double.isNaN(tsv.value().value(2).doubleValue()));
      } else {
        assertEquals(sum, tsv.value().value(2).longValue());
      }
      ts += 3600 * 1000L;
      i++;
    }
    assertEquals(4, i);
  }
  
  @Test
  public void nextNoSummariesEndFillInfectiousNan() throws Exception {
    interpolator_config = NumericSummaryInterpolatorConfig.newBuilder()
        .setDefaultFillPolicy(FillPolicy.NOT_A_NUMBER)
        .setDefaultRealFillPolicy(FillWithRealPolicy.NONE)
        .addExpectedSummary(0)
        .addExpectedSummary(2)
        .build();
    config = GroupByConfig.newBuilder()
        .setAggregator("sum")
        .setId("Testing")
        .addTagKey("dc")
        .setInfectiousNan(true)
        .setQueryInterpolationConfig(DefaultInterpolationConfig.newBuilder()
            .add(NumericSummaryType.TYPE, interpolator_config, 
                new NumericInterpolatorFactory.Default())
            .build())
        .build();
    
    long[] sums = new long[] { 10, 11, 12, -1, 22, 22, 23, -1, 31, 32, 33, -1 };
    long[] counts = new long[] { 1, 2, 3, -1, 1, 2, 3, -1, 1, 2, 3, -1 }; 
    setupData(sums, counts, false);
    setupMock();
    
    GroupByNumericSummaryIterator iterator = new GroupByNumericSummaryIterator(node, source_map);
    long ts = BASE_TIME;
    int i = 0;
    while (iterator.hasNext()) {
      TimeSeriesValue<NumericSummaryType> tsv = (TimeSeriesValue<NumericSummaryType>) iterator.next();
      print(tsv);
      assertEquals(ts, tsv.timestamp().msEpoch());
      long sum = sum(sums, i, true);
      if (sum < 0) {
        assertTrue(Double.isNaN(tsv.value().value(0).doubleValue()));
      } else {
        assertEquals(sum, tsv.value().value(0).longValue());
      }
      sum = sum(counts, i, true);
      if (sum < 0) {
        assertTrue(Double.isNaN(tsv.value().value(2).doubleValue()));
      } else {
        assertEquals(sum, tsv.value().value(2).longValue());
      }
      ts += 3600 * 1000L;
      i++;
    }
    assertEquals(4, i);
  }
  
  @Test
  public void nextNoSummariesMiddleFillInfectiousNan() throws Exception {
    interpolator_config = NumericSummaryInterpolatorConfig.newBuilder()
        .setDefaultFillPolicy(FillPolicy.NOT_A_NUMBER)
        .setDefaultRealFillPolicy(FillWithRealPolicy.NONE)
        .addExpectedSummary(0)
        .addExpectedSummary(2)
        .build();
    config = GroupByConfig.newBuilder()
        .setAggregator("sum")
        .setId("Testing")
        .addTagKey("dc")
        .setInfectiousNan(true)
        .setQueryInterpolationConfig(DefaultInterpolationConfig.newBuilder()
            .add(NumericSummaryType.TYPE, interpolator_config, 
                new NumericInterpolatorFactory.Default())
            .build())
        .build();
    
    long[] sums = new long[] { 10, -1, 12, 13, 22, -1, 23, 24, 31, -1, 33, 34 };
    long[] counts = new long[] { 1, -1, 3, 4, 1, -1, 3, 4, 1, -1, 3, 4 }; 
    setupData(sums, counts, false);
    setupMock();
    
    GroupByNumericSummaryIterator iterator = new GroupByNumericSummaryIterator(node, source_map);
    long ts = BASE_TIME;
    int i = 0;
    while (iterator.hasNext()) {
      TimeSeriesValue<NumericSummaryType> tsv = (TimeSeriesValue<NumericSummaryType>) iterator.next();
      print(tsv);
      assertEquals(ts, tsv.timestamp().msEpoch());
      long sum = sum(sums, i, true);
      if (sum < 0) {
        assertTrue(Double.isNaN(tsv.value().value(0).doubleValue()));
      } else {
        assertEquals(sum, tsv.value().value(0).longValue());
      }
      sum = sum(counts, i, true);
      if (sum < 0) {
        assertTrue(Double.isNaN(tsv.value().value(2).doubleValue()));
      } else {
        assertEquals(sum, tsv.value().value(2).longValue());
      }
      ts += 3600 * 1000L;
      i++;
    }
    assertEquals(4, i);
  }
  
  @Test
  public void nextOneSeriesEmpty() throws Exception {
    long[] sums = new long[] { 10, 11, 12, 13, -1, -1, -1, -1, 31, 32, 33, 34 };
    long[] counts = new long[] { 1, 2, 3, 4, -1, -1, -1, -1, 1, 2, 3, 4 };
    setupData(sums, counts, false);
    ts2.data.clear();
    setupMock();
    
    GroupByNumericSummaryIterator iterator = new GroupByNumericSummaryIterator(node, source_map);
    long ts = BASE_TIME;
    int i = 0;
    while (iterator.hasNext()) {
      TimeSeriesValue<NumericSummaryType> tsv = (TimeSeriesValue<NumericSummaryType>) iterator.next();
      print(tsv);
      assertEquals(ts, tsv.timestamp().msEpoch());
      long sum = sum(sums, i, false);
      assertEquals(sum, tsv.value().value(0).longValue());
      sum = sum(counts, i, false);
      assertEquals(sum, tsv.value().value(2).longValue());
      ts += 3600 * 1000L;
      i++;
    }
    assertEquals(4, i);
  }
  
  @Test
  public void nextAvgSumAndCountAllPresent() throws Exception {
    interpolator_config = NumericSummaryInterpolatorConfig.newBuilder()
        .setDefaultFillPolicy(FillPolicy.NONE)
        .setDefaultRealFillPolicy(FillWithRealPolicy.NONE)
        .addExpectedSummary(0)
        .addExpectedSummary(2)
        .setComponentAggregator(Aggregators.SUM)
        .setRollupConfig(rollup_config)
        .build();
    config = GroupByConfig.newBuilder()
        .setAggregator("avg")
        .setId("Testing")
        .addTagKey("dc")
        .setQueryInterpolationConfig(DefaultInterpolationConfig.newBuilder()
            .add(NumericSummaryType.TYPE, interpolator_config, 
                new NumericInterpolatorFactory.Default())
            .build())
        .build();
    
    long[] sums = new long[] { 10, 11, 12, 13, 21, 22, 23, 24, 31, 32, 33, 34 };
    long[] counts = new long[] { 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4 }; 
    setupData(sums, counts, false);
    setupMock();
    
    GroupByNumericSummaryIterator iterator = new GroupByNumericSummaryIterator(node, source_map);
    long ts = BASE_TIME;
    int i = 0;
    while (iterator.hasNext()) {
      TimeSeriesValue<NumericSummaryType> tsv = (TimeSeriesValue<NumericSummaryType>) iterator.next();
      print(tsv);
      assertEquals(ts, tsv.timestamp().msEpoch());
      assertEquals(avg(sums, counts, i, false), tsv.value().value(5).doubleValue(), 0.0001);
      assertNull(tsv.value().value(0));
      assertNull(tsv.value().value(2));
      ts += 3600 * 1000L;
      i++;
    }
    assertEquals(4, i);
  }
  
  @Test
  public void nextAvgSumAndCountEmpty() throws Exception {
    interpolator_config = NumericSummaryInterpolatorConfig.newBuilder()
        .setDefaultFillPolicy(FillPolicy.NONE)
        .setDefaultRealFillPolicy(FillWithRealPolicy.NONE)
        .addExpectedSummary(0)
        .addExpectedSummary(2)
        .setComponentAggregator(Aggregators.SUM)
        .setRollupConfig(rollup_config)
        .build();
    config = GroupByConfig.newBuilder()
        .setAggregator("avg")
        .setId("Testing")
        .addTagKey("dc")
        .setQueryInterpolationConfig(DefaultInterpolationConfig.newBuilder()
            .add(NumericSummaryType.TYPE, interpolator_config, 
                new NumericInterpolatorFactory.Default())
            .build())
        .build();
    
    long[] sums = new long[] { 10, 11, 12, 13, 21, 22, 23, 24, 31, 32, 33, 34 };
    long[] counts = new long[] { 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4 }; 
    setupData(sums, counts, false);
    setupMock();
    ts1.data.clear();
    ts2.data.clear();
    ts3.data.clear();
    
    GroupByNumericSummaryIterator iterator = new GroupByNumericSummaryIterator(node, source_map);
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void nextAvgSumAndCountStaggeredMissing() throws Exception {
    interpolator_config = NumericSummaryInterpolatorConfig.newBuilder()
        .setDefaultFillPolicy(FillPolicy.NONE)
        .setDefaultRealFillPolicy(FillWithRealPolicy.NONE)
        .addExpectedSummary(0)
        .addExpectedSummary(2)
        .setComponentAggregator(Aggregators.SUM)
        .setRollupConfig(rollup_config)
        .build();
    config = GroupByConfig.newBuilder()
        .setAggregator("avg")
        .setId("Testing")
        .addTagKey("dc")
        .setQueryInterpolationConfig(DefaultInterpolationConfig.newBuilder()
            .add(NumericSummaryType.TYPE, interpolator_config, 
                new NumericInterpolatorFactory.Default())
            .build())
        .build();
    
    long[] sums = new long[] { -1, 11, 12, -1, 21, 22, -1, 24, 31, 32, 33, 34 };
    long[] counts = new long[] { 1, 2, 3, 4, -1, 2, 3, -1, -1, 2, -1, 4 }; 
    setupData(sums, counts, false);
    setupMock();
    
    GroupByNumericSummaryIterator iterator = new GroupByNumericSummaryIterator(node, source_map);
    long ts = BASE_TIME;
    int i = 0;
    while (iterator.hasNext()) {
      TimeSeriesValue<NumericSummaryType> tsv = (TimeSeriesValue<NumericSummaryType>) iterator.next();
      print(tsv);
      assertEquals(ts, tsv.timestamp().msEpoch());
      assertEquals(avg(sums, counts, i, false), tsv.value().value(5).doubleValue(), 0.0001);
      assertNull(tsv.value().value(0));
      assertNull(tsv.value().value(2));
      ts += 3600 * 1000L;
      i++;
    }
    assertEquals(4, i);
  }
  
  @Test
  public void nextAvgSumAndCountStaggeredMissingStaggeredNaNs() throws Exception {
    interpolator_config = NumericSummaryInterpolatorConfig.newBuilder()
        .setDefaultFillPolicy(FillPolicy.NONE)
        .setDefaultRealFillPolicy(FillWithRealPolicy.NONE)
        .addExpectedSummary(0)
        .addExpectedSummary(2)
        .setComponentAggregator(Aggregators.SUM)
        .setRollupConfig(rollup_config)
        .build();
    config = GroupByConfig.newBuilder()
        .setAggregator("avg")
        .setId("Testing")
        .addTagKey("dc")
        .setQueryInterpolationConfig(DefaultInterpolationConfig.newBuilder()
            .add(NumericSummaryType.TYPE, interpolator_config, 
                new NumericInterpolatorFactory.Default())
            .build())
        .build();
    
    long[] sums = new long[] { -1, 11, 12, -1, 21, 22, -1, 24, 31, 32, 33, 34 };
    long[] counts = new long[] { 1, 2, 3, 4, -1, 2, 3, -1, -1, 2, -1, 4 }; 
    setupData(sums, counts, true);
    setupMock();
    
    GroupByNumericSummaryIterator iterator = new GroupByNumericSummaryIterator(node, source_map);
    long ts = BASE_TIME;
    int i = 0;
    while (iterator.hasNext()) {
      TimeSeriesValue<NumericSummaryType> tsv = (TimeSeriesValue<NumericSummaryType>) iterator.next();
      print(tsv);
      assertEquals(ts, tsv.timestamp().msEpoch());
      assertEquals(avg(sums, counts, i, false), tsv.value().value(5).doubleValue(), 0.0001);
      assertNull(tsv.value().value(0));
      assertNull(tsv.value().value(2));
      ts += 3600 * 1000L;
      i++;
    }
    assertEquals(4, i);
  }
  
  @Test
  public void nextAvgSumAndCountStaggeredMissingStaggeredNaNsInfectious() throws Exception {
    interpolator_config = NumericSummaryInterpolatorConfig.newBuilder()
        .setDefaultFillPolicy(FillPolicy.NONE)
        .setDefaultRealFillPolicy(FillWithRealPolicy.NONE)
        .addExpectedSummary(0)
        .addExpectedSummary(2)
        .setComponentAggregator(Aggregators.SUM)
        .setRollupConfig(rollup_config)
        .build();
    config = GroupByConfig.newBuilder()
        .setAggregator("avg")
        .setId("Testing")
        .addTagKey("dc")
        .setInfectiousNan(true)
        .setQueryInterpolationConfig(DefaultInterpolationConfig.newBuilder()
            .add(NumericSummaryType.TYPE, interpolator_config, 
                new NumericInterpolatorFactory.Default())
            .build())
        .build();
    
    long[] sums = new long[] { -1, 11, 12, -1, 21, 22, -1, 24, 31, 32, 33, 34 };
    long[] counts = new long[] { 1, 2, 3, 4, -1, 2, 3, -1, -1, 2, -1, 4 }; 
    setupData(sums, counts, true);
    setupMock();
    
    GroupByNumericSummaryIterator iterator = new GroupByNumericSummaryIterator(node, source_map);
    long ts = BASE_TIME;
    int i = 0;
    while (iterator.hasNext()) {
      TimeSeriesValue<NumericSummaryType> tsv = (TimeSeriesValue<NumericSummaryType>) iterator.next();
      print(tsv);
      assertEquals(ts, tsv.timestamp().msEpoch());
      double avg = avg(sums, counts, i, true);
      if (avg < 0) {
        assertTrue(Double.isNaN(tsv.value().value(5).doubleValue()));
      } else {
        assertEquals(avg, tsv.value().value(5).doubleValue(), 0.0001);
      }
      assertNull(tsv.value().value(0));
      assertNull(tsv.value().value(2));
      ts += 3600 * 1000L;
      i++;
    }
    assertEquals(4, i);
  }
  
  @Test
  public void nextAvgSumAndCountNoSummariesStart() throws Exception {
    interpolator_config = NumericSummaryInterpolatorConfig.newBuilder()
        .setDefaultFillPolicy(FillPolicy.NONE)
        .setDefaultRealFillPolicy(FillWithRealPolicy.NONE)
        .addExpectedSummary(0)
        .addExpectedSummary(2)
        .setComponentAggregator(Aggregators.SUM)
        .setRollupConfig(rollup_config)
        .build();
    config = GroupByConfig.newBuilder()
        .setAggregator("avg")
        .setId("Testing")
        .addTagKey("dc")
        .setQueryInterpolationConfig(DefaultInterpolationConfig.newBuilder()
            .add(NumericSummaryType.TYPE, interpolator_config, 
                new NumericInterpolatorFactory.Default())
            .build())
        .build();
    
    long[] sums = new long[] { -1, 11, 12, 13, -1, 22, 23, 24, -1, 32, 33, 34 };
    long[] counts = new long[] { -1, 2, 3, 4, -1, 2, 3, 4, -1, 2, 3, 4 }; 
    setupData(sums, counts, false);
    setupMock();
    
    GroupByNumericSummaryIterator iterator = new GroupByNumericSummaryIterator(node, source_map);
    long ts = BASE_TIME;
    int i = 0;
    while (iterator.hasNext()) {
      TimeSeriesValue<NumericSummaryType> tsv = (TimeSeriesValue<NumericSummaryType>) iterator.next();
      print(tsv);
      assertEquals(ts, tsv.timestamp().msEpoch());
      double avg = avg(sums, counts, i, true);
      if (avg < 0) {
        assertNull(tsv.value().value(5));
      } else {
        assertEquals(avg, tsv.value().value(5).doubleValue(), 0.0001);
      }
      assertNull(tsv.value().value(0));
      assertNull(tsv.value().value(2));
      ts += 3600 * 1000L;
      i++;
    }
    assertEquals(4, i);
  }
  
  @Test
  public void nextAvgSumAndCountNoSummariesEnd() throws Exception {
    interpolator_config = NumericSummaryInterpolatorConfig.newBuilder()
        .setDefaultFillPolicy(FillPolicy.NONE)
        .setDefaultRealFillPolicy(FillWithRealPolicy.NONE)
        .addExpectedSummary(0)
        .addExpectedSummary(2)
        .setComponentAggregator(Aggregators.SUM)
        .setRollupConfig(rollup_config)
        .build();
    config = GroupByConfig.newBuilder()
        .setAggregator("avg")
        .setId("Testing")
        .addTagKey("dc")
        .setQueryInterpolationConfig(DefaultInterpolationConfig.newBuilder()
            .add(NumericSummaryType.TYPE, interpolator_config, 
                new NumericInterpolatorFactory.Default())
            .build())
        .build();
    
    long[] sums = new long[] { 10, 11, 12, -1, 22, 22, 23, -1, 31, 32, 33, -1 };
    long[] counts = new long[] { 1, 2, 3, -1, 1, 2, 3, -1, 1, 2, 3, -1 }; 
    setupData(sums, counts, false);
    setupMock();
    
    GroupByNumericSummaryIterator iterator = new GroupByNumericSummaryIterator(node, source_map);
    long ts = BASE_TIME;
    int i = 0;
    while (iterator.hasNext()) {
      TimeSeriesValue<NumericSummaryType> tsv = (TimeSeriesValue<NumericSummaryType>) iterator.next();
      print(tsv);
      assertEquals(ts, tsv.timestamp().msEpoch());
      double avg = avg(sums, counts, i, true);
      if (avg < 0) {
        assertNull(tsv.value().value(5));
      } else {
        assertEquals(avg, tsv.value().value(5).doubleValue(), 0.0001);
      }
      assertNull(tsv.value().value(0));
      assertNull(tsv.value().value(2));
      ts += 3600 * 1000L;
      i++;
    }
    assertEquals(4, i);
  }
  
  @Test
  public void nextAvgSumAndCountNoSummariesMiddle() throws Exception {
    interpolator_config = NumericSummaryInterpolatorConfig.newBuilder()
        .setDefaultFillPolicy(FillPolicy.NONE)
        .setDefaultRealFillPolicy(FillWithRealPolicy.NONE)
        .addExpectedSummary(0)
        .addExpectedSummary(2)
        .setComponentAggregator(Aggregators.SUM)
        .setRollupConfig(rollup_config)
        .build();
    config = GroupByConfig.newBuilder()
        .setAggregator("avg")
        .setId("Testing")
        .addTagKey("dc")
        .setQueryInterpolationConfig(DefaultInterpolationConfig.newBuilder()
            .add(NumericSummaryType.TYPE, interpolator_config, 
                new NumericInterpolatorFactory.Default())
            .build())
        .build();
    
    long[] sums = new long[] { 10, -1, 12, 13, 22, -1, 23, 24, 31, -1, 33, 34 };
    long[] counts = new long[] { 1, -1, 3, 4, 1, -1, 3, 4, 1, -1, 3, 4 }; 
    setupData(sums, counts, false);
    setupMock();
    
    GroupByNumericSummaryIterator iterator = new GroupByNumericSummaryIterator(node, source_map);
    long ts = BASE_TIME;
    int i = 0;
    while (iterator.hasNext()) {
      TimeSeriesValue<NumericSummaryType> tsv = (TimeSeriesValue<NumericSummaryType>) iterator.next();
      print(tsv);
      assertEquals(ts, tsv.timestamp().msEpoch());
      double avg = avg(sums, counts, i, true);
      if (avg < 0) {
        assertNull(tsv.value().value(5));
      } else {
        assertEquals(avg, tsv.value().value(5).doubleValue(), 0.0001);
      }
      assertNull(tsv.value().value(0));
      assertNull(tsv.value().value(2));
      ts += 3600 * 1000L;
      i++;
    }
    assertEquals(4, i);
  }
  
  // TODO - ints the doubles
  
  private long sum(long[] dps, int i, boolean infectious) {
    long sum = -1;
    for (int x = 0; x < 3; x++) {
      if (dps[i + (x * 4)] < 0) {
        if (infectious) {
          return -1;
        }
      } else {
        if (sum < 0) {
          sum = 0;
        }
        sum += dps[i + (x * 4)];
      }
    }
    return sum;
  }
  
  private double avg(long[] sums, long[] counts, int i, boolean infectious) {
    long sum = -1;
    long count = -1;
    for (int x = 0; x < 3; x++) {
      if (sums[i + (x * 4)] < 0) {
        if (infectious) {
          return -1;
        }
      } else {
        if (sum < 0) {
          sum = 0;
        }
        sum += sums[i + (x * 4)];
      }
      
      if (counts[i + (x * 4)] < 0) {
        if (infectious) {
          return -1;
        }
      } else {
        if (count < 0) {
          count = 0;
        }
        count += counts[i + (x * 4)];
      }
    }
    
    return (double) sum / (double) count;
  }
  
  private void setupData(long[] sums, long[] counts, boolean nans) {
    ts1 = new MockTimeSeries<NumericSummaryType>(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build(), 
        NumericSummaryType.TYPE);
    int sum_idx = 0;
    int counts_idx = 0;
    
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME));
    if (sums[sum_idx] >= 0) {
      v.resetValue(0, sums[sum_idx]);
    } else if (nans) {
      v.resetValue(0, Double.NaN);
    }
    if (counts[counts_idx] >= 0) {
      v.resetValue(2, counts[counts_idx]);
    } else if (nans) {
      v.resetValue(2, Double.NaN);
    }
    sum_idx++;
    counts_idx++;
    ts1.add(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 1L * 1000L)));
    if (sums[sum_idx] >= 0) {
      v.resetValue(0, sums[sum_idx]);
    } else if (nans) {
      v.resetValue(0, Double.NaN);
    }
    if (counts[counts_idx] >= 0) {
      v.resetValue(2, counts[counts_idx]);
    } else if (nans) {
      v.resetValue(2, Double.NaN);
    }
    sum_idx++;
    counts_idx++;
    ts1.add(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 2L * 1000L)));
    if (sums[sum_idx] >= 0) {
      v.resetValue(0, sums[sum_idx]);
    } else if (nans) {
      v.resetValue(0, Double.NaN);
    }
    if (counts[counts_idx] >= 0) {
      v.resetValue(2, counts[counts_idx]);
    } else if (nans) {
      v.resetValue(2, Double.NaN);
    }
    sum_idx++;
    counts_idx++;
    ts1.add(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 3L * 1000L)));
    if (sums[sum_idx] >= 0) {
      v.resetValue(0, sums[sum_idx]);
    } else if (nans) {
      v.resetValue(0, Double.NaN);
    }
    if (counts[counts_idx] >= 0) {
      v.resetValue(2, counts[counts_idx]);
    } else if (nans) {
      v.resetValue(2, Double.NaN);
    }
    sum_idx++;
    counts_idx++;
    ts1.add(v);
    
    ts2 = new MockTimeSeries<NumericSummaryType>(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build(), 
        NumericSummaryType.TYPE);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME));
    if (sums[sum_idx] >= 0) {
      v.resetValue(0, sums[sum_idx]);
    } else if (nans) {
      v.resetValue(0, Double.NaN);
    }
    if (counts[counts_idx] >= 0) {
      v.resetValue(2, counts[counts_idx]);
    } else if (nans) {
      v.resetValue(2, Double.NaN);
    }
    sum_idx++;
    counts_idx++;
    ts2.add(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 1L * 1000L)));
    if (sums[sum_idx] >= 0) {
      v.resetValue(0, sums[sum_idx]);
    } else if (nans) {
      v.resetValue(0, Double.NaN);
    }
    if (counts[counts_idx] >= 0) {
      v.resetValue(2, counts[counts_idx]);
    } else if (nans) {
      v.resetValue(2, Double.NaN);
    }
    sum_idx++;
    counts_idx++;
    ts2.add(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 2L * 1000L)));
    if (sums[sum_idx] >= 0) {
      v.resetValue(0, sums[sum_idx]);
    } else if (nans) {
      v.resetValue(0, Double.NaN);
    }
    if (counts[counts_idx] >= 0) {
      v.resetValue(2, counts[counts_idx]);
    } else if (nans) {
      v.resetValue(2, Double.NaN);
    }
    sum_idx++;
    counts_idx++;
    ts2.add(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 3L * 1000L)));
    if (sums[sum_idx] >= 0) {
      v.resetValue(0, sums[sum_idx]);
    } else if (nans) {
      v.resetValue(0, Double.NaN);
    }
    if (counts[counts_idx] >= 0) {
      v.resetValue(2, counts[counts_idx]);
    } else if (nans) {
      v.resetValue(2, Double.NaN);
    }
    sum_idx++;
    counts_idx++;
    ts2.add(v);
    
    ts3 = new MockTimeSeries<NumericSummaryType>(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build(), 
        NumericSummaryType.TYPE);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME));
    if (sums[sum_idx] >= 0) {
      v.resetValue(0, sums[sum_idx]);
    } else if (nans) {
      v.resetValue(0, Double.NaN);
    }
    if (counts[counts_idx] >= 0) {
      v.resetValue(2, counts[counts_idx]);
    } else if (nans) {
      v.resetValue(2, Double.NaN);
    }
    sum_idx++;
    counts_idx++;
    ts3.add(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 1L * 1000L)));
    if (sums[sum_idx] >= 0) {
      v.resetValue(0, sums[sum_idx]);
    } else if (nans) {
      v.resetValue(0, Double.NaN);
    }
    if (counts[counts_idx] >= 0) {
      v.resetValue(2, counts[counts_idx]);
    } else if (nans) {
      v.resetValue(2, Double.NaN);
    }
    sum_idx++;
    counts_idx++;
    ts3.add(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 2L * 1000L)));
    if (sums[sum_idx] >= 0) {
      v.resetValue(0, sums[sum_idx]);
    } else if (nans) {
      v.resetValue(0, Double.NaN);
    }
    if (counts[counts_idx] >= 0) {
      v.resetValue(2, counts[counts_idx]);
    } else if (nans) {
      v.resetValue(2, Double.NaN);
    }
    sum_idx++;
    counts_idx++;
    ts3.add(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 3L * 1000L)));
    if (sums[sum_idx] >= 0) {
      v.resetValue(0, sums[sum_idx]);
    } else if (nans) {
      v.resetValue(0, Double.NaN);
    }
    if (counts[counts_idx] >= 0) {
      v.resetValue(2, counts[counts_idx]);
    } else if (nans) {
      v.resetValue(2, Double.NaN);
    }
    sum_idx++;
    counts_idx++;
    ts3.add(v);
    
    source_map = Maps.newHashMapWithExpectedSize(3);
    source_map.put("a", ts1);
    source_map.put("b", ts2);
    source_map.put("c", ts3);
  }
  
  private void setupMock() {
    node = mock(QueryNode.class);
    when(node.config()).thenReturn(config);
    query_context = mock(QueryContext.class);
    pipeline_context = mock(QueryPipelineContext.class);
    when(pipeline_context.queryContext()).thenReturn(query_context);
    when(node.pipelineContext()).thenReturn(pipeline_context);
  }
  
  void print(final TimeSeriesValue<NumericSummaryType> tsv) {
    System.out.println("**** [UT] " + tsv.timestamp());
    if (tsv.value() == null) {
      System.out.println("**** [UT] Null value *****");
    } else {
      for (int summary : tsv.value().summariesAvailable()) {
        NumericType t = tsv.value().value(summary);
        if (t == null) {
          System.out.println("***** [UT] value for " + summary + " was null");
        } else {
          System.out.println("***** [UT] [" + summary + "] " + t.toDouble());
        }
      }
    }
    System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
  }
}
