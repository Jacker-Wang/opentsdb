// This file is part of OpenTSDB.
// Copyright (C) 2013  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.tools;

import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import net.opentsdb.core.TSDB;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.tree.Tree;
import net.opentsdb.tree.TreeBuilder;
import net.opentsdb.uid.NoSuchUniqueId;
import net.opentsdb.uid.UniqueId;

import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

/**
 * Helper tool class used to generate or synchronize a tree using TSMeta objects
 * stored in the UID table. Also can be used to delete a tree. This class should
 * be used only by the CLI tools.
 */
final class TreeSync {
  private static final Logger LOG = LoggerFactory.getLogger(TreeSync.class);
  
  /** Charset used to convert Strings to byte arrays and back. */
  private static final Charset CHARSET;
  static {
    final Class<UniqueId> uidclass = UniqueId.class;
    try {
      // Those are all implementation details so they're not part of the
      // interface.  We access them anyway using reflection.  I think this
      // is better than marking those public and adding a javadoc comment
      // "THIS IS INTERNAL DO NOT USE".  If only Java had C++'s "friend" or
      // a less stupid notion of a package.
      Field f;
      f = uidclass.getDeclaredField("CHARSET");
      f.setAccessible(true);
      CHARSET = (Charset) f.get(null);
    } catch (Exception e) {
      throw new RuntimeException("static initializer failed", e);
    }
  }
  
  /** TSDB to use for storage access */
  final TSDB tsdb;
  
  /**
   * Default constructor, stores the TSDB to use
   * @param tsdb The TSDB to use for access
   */
  public TreeSync(final TSDB tsdb) {
    this.tsdb = tsdb;
  }
  
  /**
   * Performs a tree synchronization using a table scanner across the UID table
   * @return 0 if completed successfully, something else if an error occurred
   */
  public int run() throws Exception {
    final byte[] start_row = new byte[TSDB.metrics_width()];
    final byte[] end_row = new byte[TSDB.metrics_width()];
    Arrays.fill(end_row, (byte)0xFF);

    final Scanner scanner = tsdb.getClient().newScanner(tsdb.uidTable());
    scanner.setStartKey(start_row);
    scanner.setStopKey(end_row);
    scanner.setFamily("name".getBytes(CHARSET));
    scanner.setQualifier("ts_meta".getBytes(CHARSET));

    /**
     * Called after loading all of the trees so we can setup a list of 
     * {@link TreeBuilder} objects to pass on to the table scanner. On success
     * this will return the a list of TreeBuilder objects or null if no trees
     * were defined.
     */
    final class LoadAllTreesCB implements Callback<ArrayList<TreeBuilder>, 
      List<Tree>> {

      @Override
      public ArrayList<TreeBuilder> call(List<Tree> trees) throws Exception {
        if (trees == null || trees.isEmpty()) {
          return null;
        }
        
        final ArrayList<TreeBuilder> tree_builders = 
          new ArrayList<TreeBuilder>(trees.size());
        for (Tree tree : trees) {
          final TreeBuilder builder = new TreeBuilder(tsdb, tree);
          tree_builders.add(builder);
        }
        
        return tree_builders;
      }
      
    }
    
    // start the process by loading all of the trees in the system
    final ArrayList<TreeBuilder> tree_builders = 
      Tree.fetchAllTrees(tsdb).addCallback(new LoadAllTreesCB())
      .joinUninterruptibly();
    
    if (tree_builders == null) {
      LOG.warn("No trees were defined in the system");
      return -1;
    } else {
      LOG.info("Found [" + tree_builders.size() + "] trees");
    }
    
    // load or initialize the root for every tree so we save time later on
    for (TreeBuilder builder : tree_builders) {
      builder.loadRoot(true).joinUninterruptibly();
    }
    
    // setup an array for storing the tree processing calls so we can block 
    // until each call has completed
    final ArrayList<Deferred<Boolean>> tree_calls = 
      new ArrayList<Deferred<Boolean>>();
    
    /**
     * Scanner callback that loops through the UID table recursively until 
     * the scanner returns a null row set.
     */
    final class TsuidScanner implements Callback<Deferred<Boolean>, 
      ArrayList<ArrayList<KeyValue>>> {

      /**
       * Fetches the next set of rows from the scanner, adding this class as a 
       * callback
       * @return A meaningless deferred used to wait on until processing has
       * completed
       */
      public Deferred<Boolean> scan() {
        return scanner.nextRows().addCallbackDeferring(this);
      }
      
      @Override
      public Deferred<Boolean> call(ArrayList<ArrayList<KeyValue>> rows)
          throws Exception {
        if (rows == null) {
          
          /**
           * Super simple final callback executed after we have scanned the
           * entire UID table. Used to wait on the list of storage calls before
           * returning so they have time to complete.
           */
          final class FinalCB implements Callback<Deferred<Boolean>, 
            ArrayList<Object>> {

            @Override
            public Deferred<Boolean> call(final ArrayList<Object> tree_calls)
                throws Exception {
              return Deferred.fromResult(true);
            }
            
          }
          
          // no more rows were found, so we collate the previous deferreds and
          // wait until they're finished
          return Deferred.group(tree_calls).addCallbackDeferring(new FinalCB());
        }
        
        for (final ArrayList<KeyValue> row : rows) {
          
          // convert to a string one time
          final String tsuid = UniqueId.uidToString(row.get(0).key());
          
          /**
           * A throttling callback used to wait for the current TSMeta to 
           * complete processing through the trees before continuing on with 
           * the next set.
           */
          final class TreeBuilderBufferCB implements Callback<Deferred<Boolean>, 
            ArrayList<Object>> {

            @Override
            public Deferred<Boolean> call(ArrayList<Object> builder_calls)
                throws Exception {
              LOG.debug("Processed [" + builder_calls.size() + "] tree_calls");
              return Deferred.fromResult(true);
            }
            
          }
          
          /**
           * Executed after parsing a TSMeta object and loading all of the
           * associated UIDMetas. Once the meta has been loaded, this callback
           * runs it through each of the configured TreeBuilder objects and
           * stores the resulting deferred in an array. Once processing of all
           * of the rules has completed, we group the deferreds and call
           * BufferCB() to wait for their completion.
           */
          final class ParseCB implements Callback<Deferred<Boolean>, TSMeta> {

            final ArrayList<Deferred<ArrayList<Object>>> builder_calls = 
              new ArrayList<Deferred<ArrayList<Object>>>();
            
            @Override
            public Deferred<Boolean> call(TSMeta meta) throws Exception {
              if (meta != null) {
                LOG.debug("Processing TSMeta: " + meta);
                for (TreeBuilder builder : tree_builders) {
                  builder_calls.add(builder.processTimeseriesMeta(meta));
                }
                return Deferred.group(builder_calls)
                  .addCallbackDeferring(new TreeBuilderBufferCB());
              } else {
                return Deferred.fromResult(false);
              }
            }
            
          }
          
          /**
           * An error handler used to catch issues when loading the TSMeta such
           * as a missing UID name. In these situations we want to log that the 
           * TSMeta had an issue and continue on.
           */
          final class ErrBack implements Callback<Deferred<Boolean>, Exception> {
            
            @Override
            public Deferred<Boolean> call(Exception e) throws Exception {
              
              if (e.getClass().equals(IllegalStateException.class)) {
                LOG.error("Invalid data when processing TSUID [" + tsuid + "]", e);
              } else if (e.getClass().equals(IllegalArgumentException.class)) {
                LOG.error("Invalid data when processing TSUID [" + tsuid + "]", e);
              } else if (e.getClass().equals(NoSuchUniqueId.class)) {
                LOG.warn("Timeseries [" + tsuid + 
                    "] includes a non-existant UID: " + e.getMessage());
              } else {
                throw e;
              }
              
              return Deferred.fromResult(false);
            }
            
          }

          // matched a TSMeta column, so request a parsing and loading of
          // associated UIDMeta objects, then pass it off to callbacks for 
          // parsing through the trees.
          tree_calls.add(TSMeta.parseFromColumn(tsdb, row.get(0), true)
              .addCallbackDeferring(new ParseCB()).addErrback(new ErrBack()));
        }
        
        /**
         * Another buffer callback that waits for the current set of TSMetas to
         * complete their tree calls before we fetch another set of rows from
         * the scanner. This necessary to avoid OOM issues.
         */
        final class ContinueCB implements Callback<Deferred<Boolean>, 
          ArrayList<Object>> {

          @Override
          public Deferred<Boolean> call(ArrayList<Object> tsuids)
              throws Exception {
            LOG.debug("Processed [" + tsuids.size() + "] tree_calls, continuing");
            tree_calls.clear();
            return scan();
          }
          
        }
        
        // request the next set of rows from the scanner, but wait until the
        // current set of TSMetas has been processed so we don't slaughter our
        // host
        return Deferred.group(tree_calls)
          .addCallbackDeferring(new ContinueCB());
      }
      
    }
    
    final TsuidScanner tree_scanner = new TsuidScanner();
    tree_scanner.scan().joinUninterruptibly();
    return 0;
  }

  /**
   * Attempts to delete all data generated by the given tree, and optionally,
   * the tree definition itself.
   * @param tree_id The tree with data to delete
   * @param delete_definition Whether or not the tree definition itself should
   * be removed from the system
   * @return 0 if completed successfully, something else if an error occurred
   */
  public int purgeTree(final int tree_id, final boolean delete_definition) 
    throws Exception {
    if (delete_definition) {
      LOG.info("Deleting tree branches and definition for: " + tree_id);
    } else {
      LOG.info("Deleting tree branches for: " + tree_id);
    }
    Tree.deleteTree(tsdb, tree_id, delete_definition).joinUninterruptibly();
    LOG.info("Completed tree deletion for: " + tree_id);
    return 0;
  }
}
