// This file is part of OpenTSDB.
// Copyright (C) 2012  The OpenTSDB Authors.
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.regex.PatternSyntaxException;

import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonTypeInfo;
import org.codehaus.jackson.type.TypeReference;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.core.Aggregator;
import net.opentsdb.core.Aggregators;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.JSON;
import net.opentsdb.core.Query;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSDB.TSDRole;
import net.opentsdb.core.Tags;
import net.opentsdb.formatters.Ascii;
import net.opentsdb.formatters.CollectdJSON;
import net.opentsdb.formatters.TSDFormatter;
import net.opentsdb.formatters.TsdbJSON;
import net.opentsdb.graph.GnuGraphFormatter;
import net.opentsdb.uid.NoSuchUniqueName;

/**
 * Used to be the GraphHandler, but we won't always be requesting graphs. Instead we'll
 * do the heavy lifting of parsing the query (the {@code /q} endpoint) here and
 * if the user requests it, we'll pass it on to the graph handler. Otherwise we'll just
 * return some JSON for the querent to play with
 */
public class QueryHandler implements HttpRpc {
  
  /** Used for deserializing the Collectd JSON data */
  private static final TypeReference<DataQuery> dqTypeRef = 
    new TypeReference<DataQuery>() {
  };
  
  private static final Logger LOG = LoggerFactory.getLogger(QueryHandler.class);

  /** 
   * Checks the cache first for valid data, then performs one or more queries against
   * HBase to fetch data, and stores data in the cache if applicable.
   * @param tsdb The TSDB to use for fetching data
   * @param query The HTTP query to work with
   * @throws IOException 
   */
  public void execute(final TSDB tsdb, final HttpQuery query) throws IOException {
    if (tsdb.role != TSDRole.API){
      query.sendError(HttpResponseStatus.NOT_IMPLEMENTED, "Not implemented for role [" + tsdb.role + "]");
      return;
    }
    
    //final long start_time = query.getQueryStringDate("start");
    final boolean nocache = query.hasQueryStringParam("nocache");
    //long end_time = query.getQueryStringDate("end");
    final int query_hash = query.getQueryStringHash();
    
    //LOG.trace(String.format("HTTP Start [%d] End [%d]", start_time, end_time));
    // first, see if we can satisfy the request from cache
    if (!nocache && query.getCacheAndReturn(query_hash)){
      // satisfied from cache!!
      return;
    }
    
    // parse query
    DataQuery dq = null;
    if (query.getMethod() == HttpMethod.POST){
      LOG.trace("Parsing POST data: " + query.getPostData());
      JSON codec = new JSON(new DataQuery());
      if (!codec.parseObject(query.getPostData(), dqTypeRef)){
        query.sendError(HttpResponseStatus.BAD_REQUEST, "Unable to parse JSON data: " + codec.getError());
        return;
      }
      dq = (DataQuery)codec.getObject();
      LOG.trace(codec.getJsonString());
      if (!dq.parseQuery(tsdb, query)){
        query.sendError(HttpResponseStatus.BAD_REQUEST, dq.error);
        return;
      }
    }else{
      dq = new DataQuery();
      if (!dq.parseQueryString(tsdb, query)){
        query.sendError(HttpResponseStatus.BAD_REQUEST, dq.error);
        return;
      }
    }    
    
    // data checks
    if (dq.start_time < 1) {
      throw BadRequestException.missingParameter("start");
    }
    final long now = System.currentTimeMillis() / 1000;
    if (dq.end_time < 1) {
      dq.end_time = now;
    }
    
    // get the cache directory
    String basepath = tsdb.getConfig().cacheDirectory();
    if (System.getProperty("os.name").contains("Windows")){
      if (!basepath.endsWith("\\"))
        basepath += "\\";
    }else{
      if (!basepath.endsWith("/"))
        basepath += "/";     
    }
    
    // append the hash of the query string so we have effective caching
    basepath += Integer.toHexString(query_hash);

    // determine how many HBase queries we'll need to run
    int total_queries = 0;
    Query[] tsdbqueries = dq.getTSDQueries();
    
    // loop through the queries and set the timestamps
    for (final Query tsdbquery : tsdbqueries) {
      try {
        tsdbquery.setStartTime(dq.start_time);
      } catch (IllegalArgumentException e) {
        throw new BadRequestException("start time: " + e.getMessage());
      }
      try {
        tsdbquery.setEndTime(dq.end_time);
      } catch (IllegalArgumentException e) {
        throw new BadRequestException("end time: " + e.getMessage());
      }
      total_queries++;
    }

    if (tsdbqueries == null || total_queries < 1){
      query.sendError(HttpResponseStatus.BAD_REQUEST, "Unable to parse the query");
      return;
    }
    
    // setup the proper formatter object based on the path
    String endpoint = query.getEndpoint();
    final TSDFormatter formatter;
    if (endpoint != null){
      formatter = TSDFormatter.getFormatter(endpoint, tsdb);
      if (endpoint.compareTo("gnugraph") == 0){
        // gnugraph needs more cruft set
        ((GnuGraphFormatter)formatter).init();
        ((GnuGraphFormatter)formatter).setBasePath(basepath);
        ((GnuGraphFormatter)formatter).setStartTime(dq.start_time);
        ((GnuGraphFormatter)formatter).setEndTime(dq.end_time);
        ((GnuGraphFormatter)formatter).setQueryString(query.querystring);
        ((GnuGraphFormatter)formatter).setQueryHash(query.hashCode());
      }
    }else
      formatter = TSDFormatter.getFormatter("tsdbjson", tsdb);
    
    if (formatter == null){
      query.sendError(HttpResponseStatus.NOT_IMPLEMENTED, 
          "Could not find a formatter for endpoint [" + endpoint + "]");
      return;
    }

    // validate the query before running it
    if (!formatter.validateQuery(dq)){
      query.sendError(HttpResponseStatus.BAD_REQUEST, dq.error);
      return;
    }
    
    final int nqueries = tsdbqueries.length;
    LOG.trace(String.format("Number of queries [%d]", nqueries));
    for (int i = 0; i < nqueries; i++) {
      try {  // execute the TSDB query!
        // XXX This is slow and will block Netty.  TODO(tsuna): Don't block.
        // TODO(tsuna): Optimization: run each query in parallel.
        final DataPoints[] series = tsdbqueries[i].run();
        
        // loop through the series and add them to the formatter
        for (final DataPoints datapoints : series) {
          formatter.putDatapoints(datapoints);
        }
      } catch (RuntimeException e) {
        LOG.info("Query failed (stack trace coming): " + tsdbqueries[i]);
        throw e;
      }
      tsdbqueries[i] = null;  // free()
    }
    tsdbqueries = null;  // free()
    
    // process the formatter
    formatter.handleHTTPGet(query);

    return;
  }
}
