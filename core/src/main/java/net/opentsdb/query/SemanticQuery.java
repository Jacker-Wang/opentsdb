package net.opentsdb.query;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.hash.HashCode;

import net.opentsdb.core.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.query.execution.graph.ExecutionGraph;
import net.opentsdb.query.execution.graph.ExecutionGraphNode;
import net.opentsdb.query.pojo.Filter;
import net.opentsdb.query.serdes.SerdesOptions;
import net.opentsdb.utils.JSON;

public class SemanticQuery implements TimeSeriesQuery {

  private ExecutionGraph execution_graph;
  private List<QuerySinkConfig> sink_configs;
  private List<QuerySink> sinks;
  private Map<String, Filter> filters;
  private QueryMode mode;
  private List<SerdesOptions> serdes_options;
  
  SemanticQuery(final Builder builder) {
    execution_graph = builder.execution_graph;
    sink_configs = builder.sink_configs;
    sinks = builder.sinks;
    if (builder.filters != null) {
      filters = Maps.newHashMap();
      for (final Filter filter : builder.filters) {
        filters.put(filter.getId(), filter);
      }
    } else {
      filters = null;
    }
    mode = builder.mode;
    serdes_options = builder.serdes_options;
    
    // set the query if needed
    for (final ExecutionGraphNode node : execution_graph.getNodes()) {
      if (node.getConfig() != null && 
          node.getConfig() instanceof QuerySourceConfig &&
          ((QuerySourceConfig) node.getConfig()).getQuery() == null) {
        ((QuerySourceConfig) node.getConfig()).setTimeSeriesQuery(this);
      }
    }
    for (final QueryNodeConfig config : execution_graph.nodeConfigs().values()) {
      if (config instanceof QuerySourceConfig &&
          ((QuerySourceConfig) config).getQuery() == null) {
        ((QuerySourceConfig) config).setTimeSeriesQuery(this);
      }
    }
  }
  
  public ExecutionGraph getExecutionGraph() {
    return execution_graph;
  }
  
  public List<QuerySinkConfig> getSinkConfigs() {
    return sink_configs;
  }
  
  public List<QuerySink> getSinks() {
    return sinks;
  }
  
  public List<Filter> getFilters() {
    return Lists.newArrayList(filters.values());
  }
  
  public QueryMode getMode() {
    return mode;
  }
  
  public List<SerdesOptions> getSerdesOptions() {
    return serdes_options;
  }
  
  public Filter getFilter(final String filter_id) {
    return filters == null ? null : filters.get(filter_id);
  }
  
  @Override
  public int compareTo(TimeSeriesQuery o) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public HashCode buildHashCode() {
    // TODO Auto-generated method stub
    return Const.HASH_FUNCTION()
        .newHasher()
        .putBoolean(true)
        .hash();
  }

  public static Builder newBuilder() {
    return new Builder();
  }
  
  public static class Builder {
    private ExecutionGraph execution_graph;
    private List<QuerySinkConfig> sink_configs;
    private List<QuerySink> sinks;
    private List<Filter> filters;
    private QueryMode mode;
    private List<SerdesOptions> serdes_options;
    
    public Builder setExecutionGraph(final ExecutionGraph execution_graph) {
      this.execution_graph = execution_graph;
      return this;
    }
    
    public Builder setSinkConfigs(final List<QuerySinkConfig> sink_configs) {
      this.sink_configs = sink_configs;
      return this;
    }
    
    public Builder addSinkConfig(final QuerySinkConfig sink) {
      if (sink_configs == null) {
        sink_configs = Lists.newArrayList();
      }
      sink_configs.add(sink);
      return this;
    }
    
    public Builder setSinks(final List<QuerySink> sinks) {
      this.sinks = sinks;
      return this;
    }
    
    public Builder addSink(final QuerySink sink) {
      if (sinks == null) {
        sinks = Lists.newArrayList();
      }
      sinks.add(sink);
      return this;
    }
    
    public Builder setFilters(final List<Filter> filters) {
      this.filters = filters;
      return this;
    }
    
    public Builder addFilter(final Filter filter) {
      if (filters == null) {
        filters = Lists.newArrayList();
      }
      filters.add(filter);
      return this;
    }
    
    public Builder setMode(final QueryMode mode) {
      this.mode = mode;
      return this;
    }
    
    public Builder setSerdesOptions(final List<SerdesOptions> serdes_options) {
      this.serdes_options = serdes_options;
      return this;
    }
    
    public SemanticQuery build() {
      return new SemanticQuery(this);
    }
  }

  public static Builder parse(final TSDB tsdb, final JsonNode root) {
    if (root == null) {
      throw new IllegalArgumentException("Root cannot be null.");
    }
    
    final Builder builder = newBuilder();
    JsonNode node = root.get("executionGraph");
    if (node == null) {
      throw new IllegalArgumentException("Need a graph!");
    }
    builder.setExecutionGraph(ExecutionGraph.parse(tsdb, node).build());
    
//    node = root.get("sinkConfigs");
//    if (node == null) {
//      throw new IllegalArgumentException("Need a sink config!");
//    }
//    try {
//      builder.setSinkConfigs(JSON.getMapper().treeToValue(node, List.class));
//    } catch (JsonProcessingException e) {
//      // TODO Auto-generated catch block
//      e.printStackTrace();
//    }
    
    node = root.get("filters");
    if (node != null) {
      try {
        builder.setFilters(JSON.getMapper().treeToValue(node, List.class));
      } catch (JsonProcessingException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    
    node = root.get("mode");
    if (node != null) {
      try {
        builder.setMode(JSON.getMapper().treeToValue(node, QueryMode.class));
      } catch (JsonProcessingException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    } else {
      builder.setMode(QueryMode.SINGLE);
    }
    
    return builder;
  }
}
