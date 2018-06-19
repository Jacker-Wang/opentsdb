package net.opentsdb.query;

public interface QuerySinkFactory {

  public String id();
  
  public QuerySink newSink(final QueryContext context, 
                           final QuerySinkConfig config);
}
