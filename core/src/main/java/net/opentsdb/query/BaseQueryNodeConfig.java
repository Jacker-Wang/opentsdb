// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package net.opentsdb.query;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;

import net.opentsdb.configuration.Configuration;

public abstract class BaseQueryNodeConfig implements QueryNodeConfig {

  /** A unique name for this config. */
  protected final String id;
  
  protected final Map<String, String> overrides;
  
  protected BaseQueryNodeConfig(final Builder builder) {
    id = builder.id;
    overrides = builder.overrides;
  }
  
  @Override
  public abstract boolean equals(final Object o);
  
  @Override
  public abstract int hashCode();
  
  @Override
  public String getId() {
    return id;
  }
  
  @Override
  public Map<String, String> getOverrides() {
    return overrides;
  }
  
  @Override
  public String getString(final Configuration config, final String key) {
    if (config == null) {
      throw new IllegalArgumentException("Config cannot be null");
    }
    if (Strings.isNullOrEmpty(key)) {
      throw new IllegalArgumentException("Key cannot be null or empty.");
    }
    String value = overrides == null ? null : overrides.get(key);
    if (Strings.isNullOrEmpty(value)) {
      if (config.hasProperty(key)) {
        return config.getString(key);
      }
    }
    return value;
  }
  
  @Override
  public int getInt(final Configuration config, final String key) {
    if (config == null) {
      throw new IllegalArgumentException("Config cannot be null");
    }
    if (Strings.isNullOrEmpty(key)) {
      throw new IllegalArgumentException("Key cannot be null or empty.");
    }
    String value = overrides == null ? null : overrides.get(key);
    if (Strings.isNullOrEmpty(value)) {
      if (config.hasProperty(key)) {
        return config.getInt(key);
      }
      throw new IllegalArgumentException("No value for key '" + key + "'");
    }
    return Integer.parseInt(value);
  }
  
  @Override
  public long getLong(final Configuration config, final String key) {
    if (config == null) {
      throw new IllegalArgumentException("Config cannot be null");
    }
    if (Strings.isNullOrEmpty(key)) {
      throw new IllegalArgumentException("Key cannot be null or empty.");
    }
    String value = overrides == null ? null : overrides.get(key);
    if (Strings.isNullOrEmpty(value)) {
      if (config.hasProperty(key)) {
        return config.getInt(key);
      }
      throw new IllegalArgumentException("No value for key '" + key + "'");
    }
    return Long.parseLong(value);
  }
  
  @Override
  public boolean getBoolean(final Configuration config, final String key) {
    if (config == null) {
      throw new IllegalArgumentException("Config cannot be null");
    }
    if (Strings.isNullOrEmpty(key)) {
      throw new IllegalArgumentException("Key cannot be null or empty.");
    }
    String value = overrides == null ? null : overrides.get(key);
    if (Strings.isNullOrEmpty(value)) {
      if (config.hasProperty(key)) {
        return config.getBoolean(key);
      }
      throw new IllegalArgumentException("No value for key '" + key + "'");
    }
    value = value.trim().toLowerCase();
    return value.equals("true") || value.equals("1") || value.equals("yes");
  }
  
  @Override
  public double getDouble(final Configuration config, final String key) {
    if (config == null) {
      throw new IllegalArgumentException("Config cannot be null");
    }
    if (Strings.isNullOrEmpty(key)) {
      throw new IllegalArgumentException("Key cannot be null or empty.");
    }
    String value = overrides == null ? null : overrides.get(key);
    if (Strings.isNullOrEmpty(value)) {
      if (config.hasProperty(key)) {
        return config.getInt(key);
      }
      throw new IllegalArgumentException("No value for key '" + key + "'");
    }
    return Double.parseDouble(value);
  }
  
  @Override
  public boolean hasKey(final String key) {
    if (Strings.isNullOrEmpty(key)) {
      throw new IllegalArgumentException("Key cannot be null or empty.");
    }
    return overrides == null ? false : overrides.containsKey(key);
  }
  
  /** Base builder for QueryNodeConfig. */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static abstract class Builder {
    
    @JsonProperty
    protected String id;
    
    @JsonProperty
    protected Map<String, String> overrides;
    
    /**
     * @param id An ID for this builder.
     * @return The builder.
     */
    public Builder setId(final String id) {
      this.id = id;
      return this;
    }
    
    public Builder setOverrides(final Map<String, String> overrides) {
      this.overrides = overrides;
      return this;
    }
    
    public Builder addOverride(final String key, final String value) {
      if (overrides == null) {
        overrides = Maps.newHashMap();
      }
      overrides.put(key, value);
      return this;
    }
    
    /** @return A config object or an exception if the config failed. */
    public abstract QueryNodeConfig build();
  }
}
