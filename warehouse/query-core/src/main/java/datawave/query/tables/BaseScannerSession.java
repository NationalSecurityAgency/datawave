package datawave.query.tables;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.MoreExecutors;
import datawave.query.tables.AccumuloResource.ResourceFactory;
import datawave.query.tables.stats.ScanSessionStats;
import datawave.query.tables.stats.ScanSessionStats.TIMERS;
import datawave.query.tables.stats.StatsListener;
import datawave.webservice.query.Query;
import datawave.webservice.query.util.QueryUncaughtExceptionHandler;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.log4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * This will handles running a scan against a set of ranges. The actual scan is performed in a separate thread which places the results in a result queue. The
 * result queue is polled in the actual next() and hasNext() calls. Note that the uncaughtExceptionHandler from the Query is used to pass exceptions up which
 * will also fail the overall query if something happens. If this is not desired then a local handler should be set.
 */
public abstract class BaseScannerSession<T> extends AbstractExecutionThreadService implements Iterator<T> {
    public abstract void close();
}
