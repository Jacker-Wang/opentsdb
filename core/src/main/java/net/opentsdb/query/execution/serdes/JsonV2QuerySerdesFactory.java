package net.opentsdb.query.execution.serdes;

import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSDBPlugin;
import net.opentsdb.query.serdes.SerdesFactory;
import net.opentsdb.query.serdes.TimeSeriesSerdes;

public class JsonV2QuerySerdesFactory implements SerdesFactory, TSDBPlugin {

  @Override
  public String id() {
    return "JsonV2QuerySerdes";
  }

  @Override
  public TimeSeriesSerdes newInstance() {
    return new JsonV2QuerySerdes();
  }

  @Override
  public Deferred<Object> initialize(TSDB tsdb) {
    return Deferred.fromResult(null);
  }

  @Override
  public Deferred<Object> shutdown() {
    return Deferred.fromResult(null);
  }

  @Override
  public String version() {
    return "3.0.0";
  }

}
