// This file is part of OpenTSDB.
// Copyright (C) 2011-2012  The OpenTSDB Authors.
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
package net.opentsdb.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.hbase.async.Bytes;
import org.hbase.async.HBaseRpc;
import org.hbase.async.KeyValue;
import org.hbase.async.PleaseThrottleException;

import net.opentsdb.meta.Annotation;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.utils.JSON;

/**
 * "Queue" of rows to compact.
 * <p>
 * Whenever we write a data point to HBase, the row key we write to is added
 * to this queue, which is effectively a sorted set.  There is a separate
 * thread that periodically goes through the queue and look for "old rows" to
 * compact.  A row is considered "old" if the timestamp in the row key is
 * older than a certain threshold.
 * <p>
 * The compaction process consists in reading all the cells within a given row
 * and writing them back out as a single big cell.  Once that writes succeeds,
 * we delete all the individual little cells.
 * <p>
 * This process is effective because in HBase the row key is repeated for
 * every single cell.  And because there is no way to efficiently append bytes
 * at the end of a cell, we have to do this instead.
 */
final class CompactionQueue extends ConcurrentSkipListMap<byte[], Boolean> {

  private static final Logger LOG = LoggerFactory.getLogger(CompactionQueue.class);

  /**
   * How many items are currently in the queue.
   * Because {@link ConcurrentSkipListMap#size} has O(N) complexity.
   */
  private final AtomicInteger size = new AtomicInteger();

  private final AtomicLong duplicates_different = new AtomicLong();
  private final AtomicLong duplicates_same = new AtomicLong();
  private final AtomicLong complex_compactions = new AtomicLong();
  private final AtomicLong written_cells = new AtomicLong();
  private final AtomicLong deleted_cells = new AtomicLong();

  /** The {@code TSDB} instance we belong to. */
  private final TSDB tsdb;

  /** On how many bytes do we encode metrics IDs.  */
  private final short metric_width;

  /**
   * Constructor.
   * @param tsdb The TSDB we belong to.
   */
  public CompactionQueue(final TSDB tsdb) {
    super(new Cmp(tsdb));
    this.tsdb = tsdb;
    metric_width = tsdb.metrics.width();
    if (tsdb.config.enable_compactions()) {
      startCompactionThread();
    }
  }

  @Override
  public int size() {
    return size.get();
  }

  public void add(final byte[] row) {
    if (super.put(row, Boolean.TRUE) == null) {
      size.incrementAndGet();  // We added a new entry, count it.
    }
  }

  /**
   * Forces a flush of the all old entries in the compaction queue.
   * @return A deferred that will be called back once everything has been
   * flushed (or something failed, in which case the deferred will carry the
   * exception).  In case of success, the kind of object returned is
   * unspecified.
   */
  public Deferred<ArrayList<Object>> flush() {
    final int size = size();
    if (size > 0) {
      LOG.info("Flushing all old outstanding rows out of " + size + " rows");
    }
    final long now = System.currentTimeMillis();
    return flush(now / 1000 - Const.MAX_TIMESPAN - 1, Integer.MAX_VALUE);
  }

  /**
   * Collects the stats and metrics tracked by this instance.
   * @param collector The collector to use.
   */
  void collectStats(final StatsCollector collector) {
    collector.record("compaction.count", complex_compactions);
    collector.record("compaction.duplicates", duplicates_same, "values=same");
    collector.record("compaction.duplicates", duplicates_different, "values=different");
    if (!tsdb.config.enable_compactions()) {
      return;
    }
    // The remaining stats only make sense with compactions enabled.
    collector.record("compaction.queue.size", size);
    collector.record("compaction.errors", handle_read_error.errors, "rpc=read");
    collector.record("compaction.errors", handle_write_error.errors, "rpc=put");
    collector.record("compaction.errors", handle_delete_error.errors,
                     "rpc=delete");
    collector.record("compaction.writes", written_cells);
    collector.record("compaction.deletes", deleted_cells);
  }

  /**
   * Flushes all the rows in the compaction queue older than the cutoff time.
   * @param cut_off A UNIX timestamp in seconds (unsigned 32-bit integer).
   * @param maxflushes How many rows to flush off the queue at once.
   * This integer is expected to be strictly positive.
   * @return A deferred that will be called back once everything has been
   * flushed.
   */
  private Deferred<ArrayList<Object>> flush(final long cut_off, int maxflushes) {
    assert maxflushes > 0: "maxflushes must be > 0, but I got " + maxflushes;
    // We can't possibly flush more entries than size().
    maxflushes = Math.min(maxflushes, size());
    if (maxflushes == 0) {  // Because size() might be 0.
      return Deferred.fromResult(new ArrayList<Object>(0));
    }
    final ArrayList<Deferred<Object>> ds =
      new ArrayList<Deferred<Object>>(Math.min(maxflushes,
                                               MAX_CONCURRENT_FLUSHES));
    int nflushes = 0;
    for (final byte[] row : this.keySet()) {
      if (maxflushes == 0) {
        break;
      }
      final long base_time = Bytes.getUnsignedInt(row, metric_width);
      if (base_time > cut_off) {
        break;
      } else if (nflushes == MAX_CONCURRENT_FLUSHES) {
        // We kicked off the compaction of too many rows already, let's wait
        // until they're done before kicking off more.
        break;
      }
      // You'd think that it would be faster to grab an iterator on the map
      // and then call remove() on the iterator to "unlink" the element
      // directly from where the iterator is at, but no, the JDK implements
      // it by calling remove(key) so it has to lookup the key again anyway.
      if (super.remove(row) == null) {  // We didn't remove anything.
        continue;  // So someone else already took care of this entry.
      }
      nflushes++;
      maxflushes--;
      size.decrementAndGet();
      ds.add(tsdb.get(row).addCallbacks(compactcb, handle_read_error));
    }
    final Deferred<ArrayList<Object>> group = Deferred.group(ds);
    if (nflushes == MAX_CONCURRENT_FLUSHES && maxflushes > 0) {
      // We're not done yet.  Once this group of flushes completes, we need
      // to kick off more.
      tsdb.flush();  // Speed up this batch by telling the client to flush.
      final int maxflushez = maxflushes;  // Make it final for closure.
      final class FlushMoreCB implements Callback<Deferred<ArrayList<Object>>,
                                                  ArrayList<Object>> {
        public Deferred<ArrayList<Object>> call(final ArrayList<Object> arg) {
          return flush(cut_off, maxflushez);
        }
        public String toString() {
          return "Continue flushing with cut_off=" + cut_off
            + ", maxflushes=" + maxflushez;
        }
      }
      group.addCallbackDeferring(new FlushMoreCB());
    }
    return group;
  }

  private final CompactCB compactcb = new CompactCB();

  /**
   * Callback to compact a row once it's been read.
   * <p>
   * This is used once the "get" completes, to actually compact the row and
   * write back the compacted version.
   */
  private final class CompactCB implements Callback<Object, ArrayList<KeyValue>> {
    public Object call(final ArrayList<KeyValue> row) {
      return compact(row, null);
    }
    public String toString() {
      return "compact";
    }
  }

  /**
   * Compacts a row into a single {@link KeyValue}.
   * @param row The row containing all the KVs to compact.
   * Must contain at least one element.
   * @return A compacted version of this row.
   */
  KeyValue compact(final ArrayList<KeyValue> row,
      List<Annotation> annotations) {
    final KeyValue[] compacted = { null };
    compact(row, compacted, annotations);
    return compacted[0];
  }

  /**
   * Maintains state for a single compaction; exists to break the steps down into manageable
   * pieces without having to worry about returning muliple values and passing many parameters
   * around.
   */
  private class Compaction {

    // parameters for the compaction
    private final ArrayList<KeyValue> row;
    private final KeyValue[] compacted;
    private final List<Annotation> annotations;

    private final int nkvs;

    // keeps a list of KeyValues to be deleted
    private final List<KeyValue> toDelete;

    // heap of columns, ordered by increasing timestamp
    private PriorityQueue<Column> heap;

    // true if any ms-resolution datapoints have been seen in the column
    private boolean ms_in_row;

    // true if any s-resolution datapoints have been seen in the column
    private boolean s_in_row;

    // KeyValue containing the longest qualifier for the datapoint, used to optimize
    // checking if the compacted qualifier already exists.
    private KeyValue longest;

    public Compaction(ArrayList<KeyValue> row, KeyValue[] compacted, List<Annotation> annotations) {
      nkvs = row.size();
      this.row = row;
      this.compacted = compacted;
      this.annotations = annotations;
      toDelete = new ArrayList<KeyValue>(nkvs);
    }

    /**
     * Check if there are no fixups or merges required.  This will be the case when:
     * <ul>
     *  <li>there are no columns in the heap</li>
     *  <li>there is only one single-valued column needing no fixups</li>
     * </ul>
     *
     * @return true if we know no additional work is required
     */
    private boolean noMergesOrFixups() {
      switch (heap.size()) {
        case 0:
          // no data points, nothing to do
          return true;
        default:
          // more than one column, need to merge
          return false;
        case 1:
          // only one column, check to see if it needs fixups
          Column col = heap.peek();
          // either a 2-byte qualifier or one 4-byte ms qualifier, and no fixups required
          return (col.qualifier.length == 2 || (col.qualifier.length == 4
              && Internal.inMilliseconds(col.qualifier))) && !col.needsFixup();
      }
    }

    /**
     * Perform the compaction.
     *
     * @return A {@link Deferred} if the compaction processed required a write
     * to HBase, otherwise {@code null}.
     */
    public Deferred<Object> compact() {
      // no columns in row, nothing to do
      if (nkvs == 0) {
        return null;
      }

      // go through all the columns, process annotations, and
      heap = new PriorityQueue<Column>(nkvs);
      int tot_values = buildHeapProcessAnnotations();

      // if there are no datapoints or only one that needs no fixup, we are done
      if (noMergesOrFixups()) {
        return null;
      }

      // merge the datapoints, ordered by timestamp and removing duplicates
      final ByteBufferList compacted_qual = new ByteBufferList(tot_values);
      final ByteBufferList compacted_val = new ByteBufferList(tot_values);
      complex_compactions.incrementAndGet();
      mergeDatapoints(compacted_qual, compacted_val);

      // if we wound up with no data in the compacted column, we are done
      if (compacted_qual.count() == 0) {
        return null;
      }

      // build the compacted columns
      final KeyValue compact = buildCompactedColumn(compacted_qual, compacted_val);

      boolean write = updateDeletesCheckForWrite(compact);

      if (compacted != null) {  // Caller is interested in the compacted form.
        compacted[0] = compact;
        final long base_time = Bytes.getUnsignedInt(compact.key(), metric_width);
        final long cut_off = System.currentTimeMillis() / 1000
            - Const.MAX_TIMESPAN - 1;
        if (base_time > cut_off) {  // If row is too recent...
          return null;              // ... Don't write back compacted.
        }
      }
      // if compactions aren't enabled or there is nothing to write, we're done
      if (!tsdb.config.enable_compactions() || (!write && toDelete.isEmpty())) {
        return null;
      }

      final byte[] key = compact.key();
      //LOG.debug("Compacting row " + Arrays.toString(key));
      deleted_cells.addAndGet(toDelete.size());  // We're going to delete this.
      if (write) {
        written_cells.incrementAndGet();
        Deferred<Object> deferred = tsdb.put(key, compact.qualifier(), compact.value());
        if (!toDelete.isEmpty()) {
          deferred = deferred.addCallbacks(new DeleteCompactedCB(toDelete), handle_write_error);
        }
        return deferred;
      } else {
        // We had nothing to write, because one of the cells is already the
        // correctly compacted version, so we can go ahead and delete the
        // individual cells directly.
        new DeleteCompactedCB(toDelete).call(null);
        return null;
      }
    }

    /**
     * Build a heap of columns containing datapoints.  Assumes that non-datapoint columns are
     * never merged.  Adds datapoint columns to the list of rows to be deleted.
     *
     * @return an estimate of the number of total values present, which may be high
     */
    private int buildHeapProcessAnnotations() {
      int tot_values = 0;
      for (KeyValue kv : row) {
        final byte[] qual = kv.qualifier();
        final int len = qual.length;
        tot_values += len / 2;  // at least 2 bytes per entry, overestimates are better
        if ((len & 1) != 0) {
          // process annotations and other extended formats
          if (qual[0] == Annotation.PREFIX()) {
            annotations.add(JSON.parseToObject(kv.value(), Annotation.class));
          } else {
            LOG.warn("Ignoring unexpected extended format type " + qual[0]);
          }
          continue;
        }
        if (longest == null || longest.qualifier().length < kv.qualifier().length) {
          longest = kv;
        }
        Column col = new Column(kv);
        if (col.hasMoreData()) {
          heap.add(col);
        }
        toDelete.add(kv);
      }
      return tot_values;
    }

    /**
     * Process datapoints from the heap in order, merging into a sorted list.  Handles duplicates
     * by keeping the most recent (based on HBase column timestamps; if duplicates in the )
     *
     * @param compacted_qual qualifiers for sorted datapoints
     * @param compacted_val values for sorted datapoints
     */
    private void mergeDatapoints(ByteBufferList compacted_qual, ByteBufferList compacted_val) {
      int prevTs = -1;
      while (!heap.isEmpty()) {
        Column col = heap.remove();
        int ts = col.getTimestampOffset();
        if (ts == prevTs) {
          // check to see if it is a complete duplicate, or if a value was overwritten
          byte[] existingVal = compacted_val.getLastAdd();
          byte[] discardedVal = col.getCopyOfCurrentValue();
          if (!Arrays.equals(existingVal, discardedVal)) {
            LOG.warn("Duplicate timestamp for key=" + Arrays.toString(row.get(0).key())
                + ", offset=" + ts + ", kept=" + Arrays.toString(existingVal) + ", discarded="
                + Arrays.toString(discardedVal));
            duplicates_different.incrementAndGet();
          } else {
            duplicates_same.incrementAndGet();
          }
        } else {
          prevTs = ts;
          col.writeToBuffers(compacted_qual, compacted_val);
          ms_in_row |= col.isMilliseconds();
          s_in_row |= !col.isMilliseconds();
        }
        if (col.advance()) {
          // there is still more data in this column, so add it back to the heap
          heap.add(col);
        }
      }
    }

    /**
     * Build the compacted column from the list of byte buffers that were
     * merged together.
     *
     * @param compacted_qual list of merged qualifiers
     * @param compacted_val list of merged values
     *
     * @return {@link KeyValue} instance for the compacted column
     */
    private KeyValue buildCompactedColumn(ByteBufferList compacted_qual,
        ByteBufferList compacted_val) {
      // metadata is a single byte for a multi-value column, otherwise nothing
      int metadata_length = compacted_val.count() > 1 ? 1 : 0;
      byte[] cq = compacted_qual.toBytes(0);
      byte[] cv = compacted_val.toBytes(metadata_length);

      // add the metadata flag, which right now only includes whether we mix s/ms datapoints
      if (metadata_length > 0) {
        byte metadata_flag = 0;
        if (ms_in_row && s_in_row) {
          metadata_flag |= Const.MS_MIXED_COMPACT;
        }
        cv[cv.length - 1] = metadata_flag;
      }

      final KeyValue first = row.get(0);
      return new KeyValue(first.key(), first.family(), cq, cv);
    }

    /**
     * Make sure we don't delete the row that is the result of the compaction, so we
     * remove the compacted value from the list of values to delete if it is there.
     *
     * @param compact the compacted column
     * @return true if we need to write the compacted value
     */
    private boolean updateDeletesCheckForWrite(KeyValue compact) {
      // if the longest entry isn't as long as the compacted one, obviously the compacted
      // one can't have already existed
      if (longest != null && longest.qualifier().length >= compact.qualifier().length) {
        Iterator<KeyValue> deleteIterator = toDelete.iterator();
        while (deleteIterator.hasNext()) {
          KeyValue cur = deleteIterator.next();
          if (Arrays.equals(cur.qualifier(), compact.qualifier())) {
            // the compacted row already existed, so remove from the list to delete
            deleteIterator.remove();
            // if the key and value are the same, we don't need to write it
            return !Arrays.equals(cur.value(), compact.value());
          }
        }
      }
      return true;
    }
  }

  /**
   * Compacts a row into a single {@link KeyValue}.
   * <p>
   * If the {@code row} is empty, this function does literally nothing.
   * If {@code compacted} is not {@code null}, then the compacted form of this
   * {@code row} will be stored in {@code compacted[0]}.  Obviously, if the
   * {@code row} contains a single cell, then that cell is the compacted form.
   * Otherwise the compaction process takes places.
   * @param row The row containing all the KVs to compact.  Must be non-null.
   * @param compacted If non-null, the first item in the array will be set to
   * a {@link KeyValue} containing the compacted form of this row.
   * If non-null, we will also not write the compacted form back to HBase
   * unless the timestamp in the row key is old enough.
   * @param annotations supplied list which will have all encountered
   * annotations added to it.
   * @return A {@link Deferred} if the compaction processed required a write
   * to HBase, otherwise {@code null}.
   */
  Deferred<Object> compact(final ArrayList<KeyValue> row,
      final KeyValue[] compacted,
      List<Annotation> annotations) {
    return new Compaction(row, compacted, annotations).compact();
  }

  /**
   * Callback to delete a row that's been successfully compacted.
   */
  private final class DeleteCompactedCB implements Callback<Object, Object> {

    /** What we're going to delete.  */
    private final byte[] key;
    private final byte[][] qualifiers;

    public DeleteCompactedCB(final List<KeyValue> cells) {
      final KeyValue first = cells.get(0);
      key = first.key();
      qualifiers = new byte[cells.size()][];
      for (int i = 0; i < qualifiers.length; i++) {
        qualifiers[i] = cells.get(i).qualifier();
      }
    }

    public Object call(final Object arg) {
      return tsdb.delete(key, qualifiers).addErrback(handle_delete_error);
    }

    public String toString() {
      return "delete compacted cells";
    }

  }

  private final HandleErrorCB handle_read_error = new HandleErrorCB("read");
  private final HandleErrorCB handle_write_error = new HandleErrorCB("write");
  private final HandleErrorCB handle_delete_error = new HandleErrorCB("delete");

  /**
   * Callback to handle exceptions during the compaction process.
   */
  private final class HandleErrorCB implements Callback<Object, Exception> {

    private volatile int errors;

    private final String what;

    /**
     * Constructor.
     * @param what String describing what kind of operation (e.g. "read").
     */
    public HandleErrorCB(final String what) {
      this.what = what;
    }

    public Object call(final Exception e) {
      if (e instanceof PleaseThrottleException) {  // HBase isn't keeping up.
        final HBaseRpc rpc = ((PleaseThrottleException) e).getFailedRpc();
        if (rpc instanceof HBaseRpc.HasKey) {
          // We failed to compact this row.  Whether it's because of a failed
          // get, put or delete, we should re-schedule this row for a future
          // compaction.
          add(((HBaseRpc.HasKey) rpc).key());
          return Boolean.TRUE;  // We handled it, so don't return an exception.
        } else {  // Should never get in this clause.
          LOG.error("WTF?  Cannot retry this RPC, and this shouldn't happen: "
                    + rpc);
        }
      }
      // `++' is not atomic but doesn't matter if we miss some increments.
      if (++errors % 100 == 1) {  // Basic rate-limiting to not flood logs.
        LOG.error("Failed to " + what + " a row to re-compact", e);
      }
      return e;
    }

    public String toString() {
      return "handle " + what + " error";
    }
  }

  static final long serialVersionUID = 1307386642;

  /** Starts a compaction thread.  Only one such thread is needed.  */
  private void startCompactionThread() {
    final Thrd thread = new Thrd();
    thread.setDaemon(true);
    thread.start();
  }

  /** How frequently the compaction thread wakes up flush stuff.  */
  // TODO(tsuna): Make configurable?
  private static final int FLUSH_INTERVAL = 10;  // seconds

  /** Minimum number of rows we'll attempt to compact at once.  */
  // TODO(tsuna): Make configurable?
  private static final int MIN_FLUSH_THRESHOLD = 100;  // rows

  /** Maximum number of rows we'll compact concurrently.  */
  // TODO(tsuna): Make configurable?
  private static final int MAX_CONCURRENT_FLUSHES = 10000;  // rows

  /** If this is X then we'll flush X times faster than we really need.  */
  // TODO(tsuna): Make configurable?
  private static final int FLUSH_SPEED = 2;  // multiplicative factor

  /**
   * Background thread to trigger periodic compactions.
   */
  final class Thrd extends Thread {
    public Thrd() {
      super("CompactionThread");
    }

    public void run() {
      while (true) {
        try {
          final int size = size();
          // Flush if  we have too many rows to recompact.
          // Note that in we might not be able to actually
          // flush anything if the rows aren't old enough.
          if (size > MIN_FLUSH_THRESHOLD) {
            // How much should we flush during this iteration?  This scheme is
            // adaptive and flushes at a rate that is proportional to the size
            // of the queue, so we flush more aggressively if the queue is big.
            // Let's suppose MAX_TIMESPAN = 1h.  We have `size' rows to compact,
            // and we better compact them all in less than 1h, otherwise we're
            // going to "fall behind" when after a new hour starts (as we'll be
            // inserting a ton of new rows then).  So slice MAX_TIMESPAN using
            // FLUSH_INTERVAL to compute what fraction of `size' we need to
            // flush at each iteration.  Note that `size' will usually account
            // for many rows that can't be flushed yet (not old enough) so we're
            // overshooting a bit (flushing more aggressively than necessary).
            // This isn't a problem at all.  The only thing that matters is that
            // the rate at which we flush stuff is proportional to how much work
            // is sitting in the queue.  The multiplicative factor FLUSH_SPEED
            // is added to make flush even faster than we need.  For example, if
            // FLUSH_SPEED is 2, then instead of taking 1h to flush what we have
            // for the previous hour, we'll take only 30m.  This is desirable so
            // that we evict old entries from the queue a bit faster.
            final int maxflushes = Math.max(MIN_FLUSH_THRESHOLD,
              size * FLUSH_INTERVAL * FLUSH_SPEED / Const.MAX_TIMESPAN);
            final long now = System.currentTimeMillis();
            flush(now / 1000 - Const.MAX_TIMESPAN - 1, maxflushes);
            if (LOG.isDebugEnabled()) {
              final int newsize = size();
              LOG.debug("flush() took " + (System.currentTimeMillis() - now)
                        + "ms, new queue size=" + newsize
                        + " (" + (newsize - size) + ')');
            }
          }
        } catch (Exception e) {
          LOG.error("Uncaught exception in compaction thread", e);
        } catch (OutOfMemoryError e) {
          // Let's free up some memory by throwing away the compaction queue.
          final int sz = size.get();
          CompactionQueue.super.clear();
          size.set(0);
          LOG.error("Discarded the compaction queue, size=" + sz, e);
        } catch (Throwable e) {
          LOG.error("Uncaught *Throwable* in compaction thread", e);
          // Catching this kind of error is totally unexpected and is really
          // bad.  If we do nothing and let this thread die, we'll run out of
          // memory as new entries are added to the queue.  We could always
          // commit suicide, but it's kind of drastic and nothing else in the
          // code does this.  If `enable_compactions' wasn't final, we could
          // always set it to false, but that's not an option.  So in order to
          // try to get a fresh start, let this compaction thread terminate
          // and spin off a new one instead.
          try {
            Thread.sleep(1000);  // Avoid busy looping creating new threads.
          } catch (InterruptedException i) {
            LOG.error("Compaction thread interrupted in error handling", i);
            return;  // Don't flush, we're truly hopeless.
          }
          startCompactionThread();
          return;
        }
        try {
          Thread.sleep(FLUSH_INTERVAL * 1000);
        } catch (InterruptedException e) {
          LOG.error("Compaction thread interrupted, doing one last flush", e);
          flush();
          return;
        }
      }
    }
  }

  /**
   * Helper to sort the byte arrays in the compaction queue.
   * <p>
   * This comparator sorts things by timestamp first, this way we can find
   * all rows of the same age at once.
   */
  private static final class Cmp implements Comparator<byte[]> {

    /** On how many bytes do we encode metrics IDs.  */
    private final short metric_width;

    public Cmp(final TSDB tsdb) {
      metric_width = tsdb.metrics.width();
    }

    public int compare(final byte[] a, final byte[] b) {
      final int c = Bytes.memcmp(a, b, metric_width, Const.TIMESTAMP_BYTES);
      // If the timestamps are equal, sort according to the entire row key.
      return c != 0 ? c : Bytes.memcmp(a, b);
    }
  }
}
