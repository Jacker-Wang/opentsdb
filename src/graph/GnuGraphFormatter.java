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
package net.opentsdb.graph;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.cache.CacheEntry;
import net.opentsdb.core.Const;
import net.opentsdb.core.JSON;
import net.opentsdb.core.TSDB;
import net.opentsdb.formatters.TSDFormatter;
import net.opentsdb.stats.Histogram;
import net.opentsdb.tsd.BadRequestException;
import net.opentsdb.tsd.HttpQuery;
import net.opentsdb.tsd.JSON_HTTP;

/**
 * Refactors/Replaces the previous GraphHandler class
 */
public class GnuGraphFormatter extends TSDFormatter {
  private static final Logger LOG = LoggerFactory.getLogger(GnuGraphFormatter.class);
  
  /** Number of times we had to do all the work up to running Gnuplot. */
  private static final AtomicInteger graphs_generated
    = new AtomicInteger();

  /** Keep track of the latency of graphing requests. */
  private static final Histogram graphlatency =
    new Histogram(16000, (short) 2, 100);

  /** Keep track of the latency (in ms) introduced by running Gnuplot. */
  private static final Histogram gnuplotlatency =
    new Histogram(16000, (short) 2, 100);

  /** Executor to run Gnuplot in separate bounded thread pool. */
  private ThreadPoolExecutor gnuplot;

  /** Start time (UNIX timestamp in seconds) on 32 bits ("unsigned" int). */
  private long start_time;

  /** End time (UNIX timestamp in seconds) on 32 bits ("unsigned" int). */
  private long end_time;
  
  private Map<String, List<String>> query_string;
  
  /** Plot object that handles graph verification and script creation */
  private Plot plot = null;
  private String error = "";
  private String basepath = "";
  private int query_hash = 0;
  
  /** Name of the wrapper script we use to execute Gnuplot.  */
  private static final String WRAPPER = 
    System.getProperty("os.name").contains("Windows") ? "mygnuplot.bat" : "mygnuplot.sh";
  /** Path to the wrapper script.  */
  private static String GNUPLOT;
//  static {
//    GNUPLOT = findGnuplotHelperScript();
//  }

  /** Stores metadata about the graph generation */
  private Map<String, Object> results = new HashMap<String, Object>();
  
  /**
   * Default constructor
   * @param tsdb
   */
  public GnuGraphFormatter(final TSDB tsdb){
    super(tsdb);
    if (GNUPLOT == null)
      GNUPLOT = findGnuplotHelperScript();
  }
  
  public String getEndpoint(){
    return "gnugraph";
  }
  
  public boolean init(){
    // Gnuplot is mostly CPU bound and does only a little bit of IO at the
    // beginning to read the input data and at the end to write its output.
    // We want to avoid running too many Gnuplot instances concurrently as
    // it can steal a significant number of CPU cycles from us.  Instead, we
    // allow only one per core, and we nice it (the nicing is done in the
    // shell script we use to start Gnuplot).  Similarly, the queue we use
    // is sized so as to have a fixed backlog per core.
    final int ncores = Runtime.getRuntime().availableProcessors();
    gnuplot = new ThreadPoolExecutor(
      ncores, ncores,  // Thread pool of a fixed size.
      /* 5m = */ 300000, MILLISECONDS,        // How long to keep idle threads.
      new ArrayBlockingQueue<Runnable>(20 * ncores),  // XXX Don't hardcode?
      thread_factory);
    // ArrayBlockingQueue does not scale as much as LinkedBlockingQueue in terms
    // of throughput but we don't need high throughput here.  We use ABQ instead
    // of LBQ because it creates far fewer references.
    return true;
  }

  public void setStartTime(final long start){
    this.start_time = start;
  }
  
  public void setEndTime(final long end){
    this.end_time = end;
  }
  
  public void setQueryString(final Map<String, List<String>> qs){
    this.query_string = qs;
  }
  
  public void setBasePath(final String path){
    this.basepath = path;
  }
  
  public void setQueryHash(final int hash){
    this.query_hash = hash;
  }
  
  /**
   * Creates the plotting script and data, then runs Gnuplot in a thread pool
   * and blocks until we get a result. Gnuplot will write the PNG file to our
   * cache directory and the emitter returns JSON meta data about the graph
   * generation process. Then the GUI will parse the JSON data and request
   * the PNG image directly from disk
   */
  public final boolean handleHTTPGet(final HttpQuery query) {
    if (datapoints.size() < 1) {
      error = "No data to process";
      LOG.error(error);
      return false;
    }
    
    // setup the plot
    plot = new Plot(start_time, end_time);
    plot.setPlotParams(query_string);
        
    final int nseries = datapoints.size();
    
    // copy options
    List<String> options;
    options = query_string.get("o");
    if (options == null) {
      options = new ArrayList<String>(nseries);
      for (int i = 0; i < nseries; i++) {
        options.add("");
      }
    }
    
    @SuppressWarnings("unchecked")    
    final HashSet<String>[] aggregated_tags = new HashSet[datapoints.size()];
    int npoints = 0;
    for (int i = 0; i < nseries; i++) {
      aggregated_tags[i] = new HashSet<String>();
      aggregated_tags[i].addAll(datapoints.get(i).getAggregatedTags());
      npoints += datapoints.get(i).aggregatedSize();
      plot.add(datapoints.get(i), options.get(i));
    }
    
    try {
      RunGnuplot run = new RunGnuplot(plot, basepath);
      gnuplot.execute(run);
      
      // TODO find a better way to block until we're done
      while(!run.getFinished()){
        try {
          Thread.sleep(1L);
        } catch (InterruptedException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
        // TODO create a way to timeout
      }
      
      // see if there was an error
      if (!run.error.isEmpty()){
        error = run.getError();
        return false;
      }
      
      LOG.trace("Base path: " + basepath  + ".png");
      File f = new File(basepath + ".png");
      if (f.exists()){
        
        String image = "";
        if (basepath.lastIndexOf("\\") != -1){
          image = basepath.substring(basepath.lastIndexOf("\\")+1) + ".png";
        }else if (basepath.lastIndexOf("/") != -1){
          image = basepath.substring(basepath.lastIndexOf("/")+1) + ".png";
        }
        
        // set our results
        results.put("plotted", run.getNPlotted());
        results.put("points", npoints);
        results.put("etags", aggregated_tags);
        results.put("image", image);
        
        JSON codec = new JSON(results);
        query.sendReply(codec.getJsonBytes());
        LOG.trace("Send graph request reply [" + codec.getJsonString() + "]");
        return true;
      }else{
        query.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Unable to find the graph");
        LOG.trace("Failed to fetch the graph");
        return false;
      }
    } catch (RejectedExecutionException e) {
      this.error = "Too many requests pending, please try again later";
      LOG.error(error);
    }
    return true;
  }

  /** 
   * Generates a cache entry object to store and/or send to the user. It will
   * not store the PNG in RAM, only on disk since GNU writes the graph straight
   * to the cache directory
   * @return Returns a new cache entry object
   */
  public final CacheEntry getCacheData(){
 // now we have all the metrics stored, serialize and return
    JSON_HTTP json = new JSON_HTTP(this.results);
    String response = json.getJsonString();
    
    return new CacheEntry(
        query_hash,
        basepath + ".json",
        computeExpire()
    );
  }
  
  /**
   * Default method to compute the amount of time before the
   * emitted data should expire from the local and client's cache
   * @return 0 if the data should not be cached, or a positive integer
   * reflecting how many seconds this data should be cached for
   */
  protected Long computeExpire(){
    // If the end time is in the future (1), make the graph uncacheable.
    // Otherwise, if the end time is far enough in the past (2) such that
    // no TSD can still be writing to rows for that time span and it's not
    // specified in a relative fashion (3) (e.g. "1d-ago"), make the graph
    // cacheable for a day since it's very unlikely that any data will change
    // for this time span.
    // Otherwise (4), allow the client to cache the graph for ~0.1% of the
    // time span covered by the request e.g., for 1h of data, it's OK to
    // serve something 3s stale, for 1d of data, 84s stale.
    final long now = System.currentTimeMillis() / 1000L;
    if (end_time > now) {                            // (1)
      return 0L;
    } else if (end_time < now - Const.MAX_TIMESPAN) { // (2)(3)
      return 86400L;
    } else {                                         // (4)
      return (long) ((end_time - start_time) >> 10);
    }
  }
  
  /**
   * Runs Gnuplot in a subprocess to generate the graph.
   */
  private static final class RunGnuplot implements Runnable {
    private final Plot plot;
    private final String basepath;
    private int nplotted = 0;

    private boolean finished = false;
    private String error = "";

    /**
     * Constructor
     * @param plot The plot to use for generating the data files and scripts that
     * Gnuplot will consume
     * @param basepath Directory and base filename to use for plot files and such
     */
    public RunGnuplot(final Plot plot,
                      final String basepath) {
      this.plot = plot;
      this.basepath = basepath;
    }

    /**
     * Executes the graph generation. Just a wrapper that catches errors
     * and feeds them to the error field
     */
    public void run() {
      try {
        execute();
      } catch (BadRequestException e) {
        error = e.getMessage();
        LOG.error(error);
      } catch (GnuplotException e) {
        error = e.getMessage();
        LOG.error(error);
      } catch (RuntimeException e) {
        error = e.getMessage();
        LOG.error(error);
      } catch (IOException e) {
        error = e.getMessage();
        LOG.error(error);
      }
    }

    /** 
     * Runs Gnuplot as a separate process. This will block until it's done
     * hence it sets the finished flag to true when finished.
     * @throws IOException
     */
    private void execute() throws IOException {
      nplotted = runGnuplot(basepath, plot);

      //graphlatency.add(query.processingTimeMillis());
      graphs_generated.incrementAndGet();
      LOG.debug("Finished with graph: " + basepath);
      finished = true;
    }

    /**
     * Lets the calling calss determine if gnuPlot has finished
     * @return True if we're done, false if not
     */
    public final boolean getFinished(){
      return finished;
    }
    
    /**
     * Returns the error string
     * @return An error string
     */
    public final String getError(){
      return error;
    }
  
    /** Returns the number of points plotted */
    public final int getNPlotted(){
      return nplotted;
    }
  }
  
  /**
   * Runs Gnuplot in a subprocess to generate the graph.
   * <strong>This function will block</strong> while Gnuplot is running.
   * @param query The query being handled (for logging purposes).
   * @param basepath The base path used for the Gnuplot files.
   * @param plot The plot object to generate Gnuplot's input files.
   * @return The number of points plotted by Gnuplot (0 or more).
   * @throws IOException if the Gnuplot files can't be written, or
   * the Gnuplot subprocess fails to start, or we can't read the
   * graph from the file it produces, or if we have been interrupted.
   * @throws GnuplotException if Gnuplot returns non-zero.
   */
  static int runGnuplot(final String basepath,
                        final Plot plot) throws IOException {
    final int nplotted = plot.dumpToFiles(basepath);
    final long start_time = System.nanoTime();
    final Process gnuplot = new ProcessBuilder(GNUPLOT,
      basepath + ".out", basepath + ".err", basepath + ".gnuplot").start();
    final int rv;
    try {
      rv = gnuplot.waitFor();  // Couldn't find how to do this asynchronously.
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();  // Restore the interrupted status.
      throw new IOException("interrupted", e);  // I hate checked exceptions.
    } finally {
      // We need to always destroy() the Process, otherwise we "leak" file
      // descriptors and pipes.  Unless I'm blind, this isn't actually
      // documented in the Javadoc of the !@#$%^ JDK, and in Java 6 there's no
      // way to ask the stupid-ass ProcessBuilder to not create fucking pipes.
      // I think when the GC kicks in the JVM may run some kind of a finalizer
      // that closes the pipes, because I've never seen this issue on long
      // running TSDs, except where ulimit -n was low (the default, 1024).
      gnuplot.destroy();
    }
    gnuplotlatency.add((int) ((System.nanoTime() - start_time) / 1000000));
    if (rv != 0) {
      LOG.error("Plotter returned " + rv);
      final byte[] stderr = HttpQuery.readFile(new File(basepath + ".err"),4096);
      
      // Sometimes Gnuplot will error out but still create the file.
      new File(basepath + ".png").delete();
      if (stderr == null) {
        throw new GnuplotException(rv);
      }
      throw new GnuplotException(new String(stderr));
    }
    // Remove the files for stderr/stdout if they're empty.
    deleteFileIfEmpty(basepath + ".out");
    deleteFileIfEmpty(basepath + ".err");
    return nplotted;
  }
  
  /**
   * Attempts to delete the file if it doesn't have any data
   * @param path Path to the file
   */
  private static void deleteFileIfEmpty(final String path) {
    try{
      final File file = new File(path);
      if (file.length() <= 0) {
        file.delete();
      }
    }catch(Exception e){
      LOG.error("Error deleting [" + path +"]: " + e.getMessage());
    }
  }
  
  /** Thread factory for scheduling Gnuplot executions */
  private static final PlotThdFactory thread_factory = new PlotThdFactory();
  
  /**
   * Local class to execute threads
   */
  private static final class PlotThdFactory implements ThreadFactory {
    private final AtomicInteger id = new AtomicInteger(0);

    public Thread newThread(final Runnable r) {
      return new Thread(r, "Gnuplot #" + id.incrementAndGet());
    }
  }
  
  /**
   * Iterate through the class path and look for the Gnuplot helper script.
   * @return The path to the wrapper script.
   */
  private static String findGnuplotHelperScript() {
    final URL url = GnuGraphFormatter.class.getClassLoader().getResource(WRAPPER);
    if (url == null) {
      throw new RuntimeException("Couldn't find " + WRAPPER + " on the"
        + " CLASSPATH: " + System.getProperty("java.class.path"));
    }
    final String path = url.getFile();
    LOG.debug("Using Gnuplot wrapper at {}", path);
    final File file = new File(path);
    final String error;
    if (!file.exists()) {
      error = "non-existent";
    } else if (!file.canExecute()) {
      error = "non-executable";
    } else if (!file.canRead()) {
      error = "unreadable";
    } else {
      return path;
    }
    throw new RuntimeException("The " + WRAPPER + " found on the"
      + " CLASSPATH (" + path + ") is a " + error + " file...  WTF?"
      + "  CLASSPATH=" + System.getProperty("java.class.path"));
  }
}
