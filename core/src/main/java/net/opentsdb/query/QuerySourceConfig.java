//This file is part of OpenTSDB.
//Copyright (C) 2017-2018  The OpenTSDB Authors.
//
//This program is free software: you can redistribute it and/or modify it
//under the terms of the GNU Lesser General Public License as published by
//the Free Software Foundation, either version 2.1 of the License, or (at your
//option) any later version.  This program is distributed in the hope that it
//will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
//of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
//General Public License for more details.  You should have received a copy
//of the GNU Lesser General Public License along with this program.  If not,
//see <http://www.gnu.org/licenses/>.
package net.opentsdb.query;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Strings;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;

import net.opentsdb.configuration.Configuration;
import net.opentsdb.core.Const;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.utils.DateTime;

/**
 * A simple base config class for {@link TimeSeriesDataSource} nodes.
 * 
 * @since 3.0
 */
@JsonInclude(Include.NON_NULL)
@JsonDeserialize(builder = QuerySourceConfig.Builder.class)
public class QuerySourceConfig implements QueryNodeConfig {
  
  /** The query from the caller. */
  protected final TimeSeriesQuery query;
  
  /** A unique name for this config. */
  private final String id;
  
  /** The configuration class in case we need to pull info out. */
  private final Configuration configuration;
  
  private final String start;
  private final TimeStamp start_ts;
  private final String end;
  private final TimeStamp end_ts;
  /** User's timezone used for converting absolute human readable dates */
  private String timezone;
  private final List<String> types;
  private final String metric;
  private final String filter_id;
  
  /**
   * Private ctor for the builder.
   * @param builder The non-null builder.
   */
  protected QuerySourceConfig(final Builder builder) {
//    if (builder.query == null) {
//      throw new IllegalArgumentException("Query cannot be null.");
//    }
    if (Strings.isNullOrEmpty(builder.id)) {
      throw new IllegalArgumentException("ID cannot be null or empty.");
    }
//    if (builder.configuration == null) {
//      throw new IllegalArgumentException("Configuration cannot be null.");
//    }
    query = builder.query;
    id = builder.id;
    configuration = builder.configuration;
    start = builder.start;
    end = builder.end;
    timezone = builder.timezone;
    types = builder.types;
    metric = builder.metric;
    filter_id = builder.filter_id;
    start_ts = new MillisecondTimeStamp(
        DateTime.parseDateTimeString(start, timezone));
    end_ts = new MillisecondTimeStamp(
        DateTime.parseDateTimeString(end, timezone));
  }
  
  /** @return The query the node is executing. */
  public TimeSeriesQuery query() {
    return query;
  }
  
  @Override
  public String getId() {
    return id;
  }
  
  /** @return The master configuration class. */
  public Configuration configuration() {
    return configuration;
  }

  /** @return user given start date/time, could be relative or absolute */
  public String getStart() {
    return start;
  }

  /** @return user given end date/time, could be relative, absolute or empty */
  public String getEnd() {
    return end;
  }

  /** @return user's timezone used for converting absolute human readable dates */
  public String getTimezone() {
    return timezone;
  }
  
  /** @return Returns the parsed start time. 
   * @see DateTime#parseDateTimeString(String, String) */
  public TimeStamp startTime() {
    return start_ts;
  }
  
  /** @return Returns the parsed end time. 
   * @see DateTime#parseDateTimeString(String, String) */
  public TimeStamp endTime() {
    return end_ts;
  }
  
  public List<String> getTypes() {
    return types;
  }
  
  public String getMetric() {
    return metric;
  }
  
  public String getFilterId() {
    return filter_id;
  }
  
  @Override
  public int compareTo(final QueryNodeConfig o) {
    if (!(o instanceof QuerySourceConfig)) {
      return -1;
    }
    
    return ComparisonChain.start()
        .compare(id, ((QuerySourceConfig) o).id, Ordering.natural().nullsFirst())
        .compare(query, ((QuerySourceConfig) o).query, Ordering.natural().nullsFirst())
        .result();
  }

  @Override
  public int hashCode() {
    return buildHashCode().asInt();
  }
  
  @Override
  public HashCode buildHashCode() {
    final List<HashCode> hashes = Lists.newArrayListWithCapacity(2);
    hashes.add(Const.HASH_FUNCTION().newHasher().putString(id, 
        Const.UTF8_CHARSET).hash());
    hashes.add(query.buildHashCode());
    return Hashing.combineOrdered(hashes);
  }
  
  /** @return A new builder. */
  public static Builder newBuilder() {
    return new Builder();
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Builder {
    private TimeSeriesQuery query;
    @JsonProperty
    private String id;
    private Configuration configuration;
    @JsonProperty
    private String start;
    @JsonProperty
    private String end;
    @JsonProperty
    private String timezone;
    @JsonProperty
    private List<String> types;
    @JsonProperty
    private String metric;
    @JsonProperty
    private String filter_id;
    
    /** @param query The non-null query to execute. */
    public Builder setQuery(final TimeSeriesQuery query) {
      this.query = query;
      return this;
    }
    
    /** @param id The non-null and non-empty ID for this config. */
    public Builder setId(final String id) {
      this.id = id;
      return this;
    }
    
    /** @param configuration The non-null master config. */
    public Builder setConfiguration(final Configuration configuration) {
      this.configuration = configuration;
      return this;
    }
    
    public Builder setStart(final String start) {
      this.start = start;
      return this;
    }
    
    public Builder setEnd(final String end) {
      this.end = end;
      return this;
    }
    
    public Builder setTimezone(final String timezone) {
      this.timezone = timezone;
      return this;
    }
    
    public Builder setTypes(final List<String> types) {
      this.types = types;
      return this;
    }
    
    public Builder addType(final String type) {
      if (types == null) {
        types = Lists.newArrayList();
      }
      types.add(type);
      return this;
    }
    
    public Builder setMetric(final String metric) {
      this.metric = metric;
      return this;
    }
    
    public Builder setFilterId(final String filter_id) {
      this.filter_id = filter_id;
      return this;
    }
    
    public QuerySourceConfig build() {
      return new QuerySourceConfig(this);
    }
  }

}
