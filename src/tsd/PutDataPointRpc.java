// This file is part of OpenTSDB.
// Copyright (C) 2010  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.tsd;

import java.util.HashMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.concurrent.atomic.AtomicLong;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.core.TSDB;
import net.opentsdb.core.Tags;
import net.opentsdb.core.TSDB.TSDRole;
import net.opentsdb.formatters.Ascii;
import net.opentsdb.formatters.CollectdJSON;
import net.opentsdb.formatters.TSDFormatter;
import net.opentsdb.formatters.TsdbJSON;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.uid.NoSuchUniqueName;

/** Implements the "put" telnet-style command. */
final class PutDataPointRpc implements TelnetRpc, HttpRpc {
  private static final Logger LOG = LoggerFactory
      .getLogger(PutDataPointRpc.class);
  private static final AtomicLong requests = new AtomicLong();
  private static final AtomicLong hbase_errors = new AtomicLong();
  private static final AtomicLong invalid_values = new AtomicLong();
  private static final AtomicLong illegal_arguments = new AtomicLong();
  private static final AtomicLong unknown_metrics = new AtomicLong();

  /**
   * Handles the HTTP PUT command
   * <p>
   * Metrics should be sent in a JSON format via POST. The metrics format is:
   * {"metric":"metric_name","timestamp":unix_epoch_time,"value":value",
   * "tags":{"tag1":"tag_value1","tagN":"tag_valueN"}} You can combine multiple
   * metrics in a single JSON array such as [{metric1},{metric2}]
   * <p>
   * This method will respond with a JSON string that lists the number of
   * successfully parsed metrics and the number of failed metrics, along with a
   * list of which metrics failed and why. If the JSON was improperly formatted
   * or there was another error, a JSON-RPC style error will be returned.
   * @param tsdb Master TSDB class object
   * @param query The query from Netty
   */
  public void execute(final TSDB tsdb, final HttpQuery query) {
    if (tsdb.role != TSDRole.Ingest && tsdb.role != TSDRole.Forwarder){
      query.sendError(HttpResponseStatus.NOT_IMPLEMENTED, "Not implemented for role [" + tsdb.role + "]");
      return;
    }
    
    TSDFormatter formatter = query.getFormatter();
    if (formatter == null)
      return;
    
    formatter.handleHTTPDataPut(query);
    return;
  }

  /**
   * Handles the Telnet PUT command
   */
  public Deferred<Object> execute(final TSDB tsdb, final Channel chan,
      final String[] cmd, final TSDFormatter formatter) {
    requests.incrementAndGet();
    if (formatter != null)
      return formatter.handleTelnetDataPut(cmd, chan);
    
    // default formatter
    TSDFormatter fmt = TSDFormatter.getFormatter(tsdb.config.formatterDefaultTelnet(), tsdb);
    return fmt.handleTelnetDataPut(cmd, chan);
  }

  /**
   * Collects the stats and metrics tracked by this instance.
   * @param collector The collector to use.
   */
  public static void collectStats(final StatsCollector collector) {
    collector.record("rpc.received", requests, 
        new SimpleEntry<String, String>("type", "put"));
    collector.record("rpc.errors", hbase_errors, 
        new SimpleEntry<String, String>("type", "storage_errors"));
    collector.record("rpc.errors", invalid_values, 
        new SimpleEntry<String, String>("type", "invalid_values"));
    collector.record("rpc.errors", illegal_arguments, 
        new SimpleEntry<String, String>("type", "illegal_arguments"));
    collector.record("rpc.errors", unknown_metrics, 
        new SimpleEntry<String, String>("type", "unknown_metrics"));
  }
}
