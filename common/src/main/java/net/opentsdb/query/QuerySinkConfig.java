package net.opentsdb.query;

import com.google.common.hash.HashCode;

import net.opentsdb.query.serdes.SerdesOptions;

public interface QuerySinkConfig {

  /**
   * @return The ID of the node in this config.
   */
  public String getId();
  
  public String type();
  
  /** @return A hash code for this configuration. */
  public HashCode buildHashCode();
  
  public String getSerdesId();
  
  public SerdesOptions serdesOptions();
}
