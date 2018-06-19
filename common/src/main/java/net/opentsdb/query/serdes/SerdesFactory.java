package net.opentsdb.query.serdes;

public interface SerdesFactory {

  public String id();
  
  public TimeSeriesSerdes newInstance();
  
}
