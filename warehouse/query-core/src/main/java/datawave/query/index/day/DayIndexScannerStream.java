package datawave.query.index.day;

import java.io.Closeable;
import java.io.IOException;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.query.index.year.YearIndexIterator;
import datawave.query.index.year.YearIndexScanner;
import datawave.query.index.year.YearRowIterator;
import datawave.query.planner.QueryPlan;

/**
 * Bridges the query, the scanner of DayIndexEntries, and final QueryPlanIterator
 * <p>
 */
public class DayIndexScannerStream implements Iterator<QueryPlan>, Closeable {

    private static final Logger log = LoggerFactory.getLogger(DayIndexScannerStream.class);

    // executor pools
    private final ExecutorService dayProducerThreadPool;
    private final ExecutorService scannerBridge;
    private final ExecutorService indexScanners;
    private ExecutorService yearIndexScanners;

    // atomics that track execution state
    private final AtomicBoolean producingDays = new AtomicBoolean(true);
    private final AtomicBoolean producingScans = new AtomicBoolean(true);
    private final AtomicInteger runningScans = new AtomicInteger();

    // queues that bridge executor pools
    private final BlockingQueue<String> dayQueue;
    private final BlockingQueue<BitSetIndexEntry> entryQueue;

    // variables that handle hasNext/next calls
    private final DayIndexIterator planIterator;
    private QueryPlan nextPlan;

    // Stats for the index scanner stream are somewhat interesting
    // We want to know the hit ratio within a time unit (year or day)
    // - (in a two-year date range, how many years actually held data)
    // - (in a seventeen-day date range, how many days actually held data)
    // We also want to know the ratio between time units, i.e. how many days per year or shards per day
    // - year to day
    // - day to shard
    private final AtomicInteger yearsScanned = new AtomicInteger();
    private final AtomicInteger yearsHit = new AtomicInteger();
    private final AtomicInteger daysScanned = new AtomicInteger();
    private final AtomicInteger daysHit = new AtomicInteger();
    private final AtomicInteger shardsScanned = new AtomicInteger();

    public DayIndexScannerStream(DayIndexConfig config) {

        this.planIterator = new DayIndexIterator((ASTJexlScript) config.getNode());

        int threads = config.getNumIndexThreads();
        int dateRangeSize = daysInDateRange(config.getStartDate(), config.getEndDate());
        if (dateRangeSize < threads) {
            // do not create a pool of fifty threads for a query date range of three days
            threads = dateRangeSize;

            if (threads <= 0) {
                // must enforce a reasonable minimum
                threads = 1;
            }
        } else {
            int years = yearsInDateRange(config.getStartDate(), config.getEndDate());
            int yearThreads = Math.max(years, 3); // keep it simple
            yearIndexScanners = Executors.newFixedThreadPool(yearThreads);
        }

        // initialize thread pools
        dayProducerThreadPool = Executors.newFixedThreadPool(1);
        scannerBridge = Executors.newFixedThreadPool(1);
        indexScanners = Executors.newFixedThreadPool(threads);

        // be smarter about queue sizes
        dayQueue = new PriorityBlockingQueue<>(10); // need to find a reasonable default value
        entryQueue = new PriorityBlockingQueue<>(10);

        // execute in reverse order
        scannerBridge.execute(
                        new DayScanProducer(config, dayQueue, entryQueue, indexScanners, producingDays, producingScans, runningScans, daysScanned, daysHit));

        boolean fullIndexScanRequired = config.getValuesAndFields().isEmpty();
        if (config.getDayIndexThreshold() != -1 && dateRangeSize > config.getDayIndexThreshold() && !fullIndexScanRequired) {
            // need to scan year index first
            dayProducerThreadPool.execute(new YearScanProducer(config, dayQueue, producingDays, yearsScanned, yearsHit));
        } else {
            // range is small enough we can just scan each day
            dayProducerThreadPool.execute(new DateGenerator(config, dayQueue, producingDays));
        }
    }

    @Override
    public boolean hasNext() {

        if (nextPlan != null) {
            return true;
        }

        // might not need producing days...in theory producing scans and running scans handles checking for previous state
        while (nextPlan == null && (producingDays.get() || producingScans.get() || runningScans.get() > 0 || planIterator.hasNext() || !dayQueue.isEmpty()
                        || !entryQueue.isEmpty())) {

            if (planIterator.hasNext()) {
                nextPlan = planIterator.next();
            } else {
                // find next entry and set it on the plan iterator
                try {
                    BitSetIndexEntry entry = entryQueue.poll(25, TimeUnit.MILLISECONDS);
                    if (entry != null) {
                        planIterator.setDay(entry.getYearOrDay());
                        planIterator.setShards(entry.getEntries());
                    }
                } catch (InterruptedException e) {
                    // continue
                }
            }
        }

        return nextPlan != null;
    }

    @Override
    public QueryPlan next() {
        QueryPlan next = nextPlan;
        if (nextPlan == null) {
            log.warn("next called on null value");
        } else {
            shardsScanned.getAndIncrement();
        }
        nextPlan = null;
        return next;
    }

    /**
     * Get the number of days in the query date range
     *
     * @param start
     *            the start date
     * @param stop
     *            the stop date
     * @return the number of days in the query date range
     */
    private int daysInDateRange(Date start, Date stop) {
        long elapsed = Math.abs(stop.getTime() - start.getTime());

        if (elapsed == 0) {
            return 1; // minimum of one
        }

        return (int) TimeUnit.DAYS.convert(elapsed, TimeUnit.MILLISECONDS);
    }

    /**
     * Get the number of years in the query date range
     *
     * @param start
     *            the start date
     * @param stop
     *            the stop date
     * @return the number of years in the query date range
     */
    private int yearsInDateRange(Date start, Date stop) {
        long elapsed = Math.abs(stop.getTime() - start.getTime());

        if (elapsed == 0) {
            return 1; // minimum of one
        }

        int days = (int) TimeUnit.DAYS.convert(elapsed, TimeUnit.MILLISECONDS);
        int years = days / 365;
        if (years == 0) {
            return 1;
        }
        return years;
    }

    @Override
    public void close() throws IOException {
        if (indexScanners != null) {
            indexScanners.shutdownNow();
        }

        if (scannerBridge != null) {
            scannerBridge.shutdownNow();
        }

        if (dayProducerThreadPool != null) {
            dayProducerThreadPool.shutdownNow();
        }

        if (yearIndexScanners != null) {
            yearIndexScanners.shutdownNow();
        }

        DecimalFormat df = new DecimalFormat("#.##");
        df.setRoundingMode(RoundingMode.HALF_EVEN);

        // conditionally log hit ratio for years
        if (yearsScanned.get() > 0) {
            String ratio = getHitRatio(yearsScanned.get(), yearsHit.get());
            log.info("years searched: {}, years hit: {}, year hit ratio: {}", yearsScanned, yearsHit, ratio);
        }

        // always log hit ratio for days
        String dayHitRatio = getHitRatio(daysScanned.get(), daysHit.get());
        log.info("days searched: {}, days hit: {}, day hit ratio: {}", daysScanned, daysHit, dayHitRatio);

        // conditionally log the year to day hit ratio
        if (yearsScanned.get() > 0) {
            String yearToDayRatio = getHitRatio(yearsScanned.get(), daysScanned.get());
            log.info("years scanned: {}, days scanned: {}, year-to-day ratio: {}", yearsScanned, daysScanned, yearToDayRatio);
        }

        // always log the day to shard hit ratio
        String dayToShardRatio = getHitRatio(daysScanned.get(), shardsScanned.get());
        log.info("days searched: {}, shards searched: {}, day-to-shard ratio: {}", daysScanned, shardsScanned, dayToShardRatio);
    }

    private String getHitRatio(int scanned, int hit) {
        if (scanned == 0) {
            return "0";
        }

        DecimalFormat df = new DecimalFormat("#.##");
        df.setRoundingMode(RoundingMode.HALF_EVEN);

        return df.format((double) hit / scanned);
    }

    /**
     * Bridges the {@link #dayQueue} and {@link #indexScanners} service.
     */
    private static class DayScanProducer implements Runnable {

        private final BlockingQueue<String> dayQueue;
        private final BlockingQueue<BitSetIndexEntry> entryQueue;
        private final DayIndexConfig config;
        private final ExecutorService indexScanners;
        private final AtomicBoolean producingDays;
        private final AtomicBoolean producingScans;
        private final AtomicInteger runningScans;
        private final AtomicInteger daysScanned;
        private final AtomicInteger daysHit;

        public DayScanProducer(DayIndexConfig config, BlockingQueue<String> dayQueue, BlockingQueue<BitSetIndexEntry> entryQueue, ExecutorService indexScanners,
                        AtomicBoolean producingDays, AtomicBoolean producingScans, AtomicInteger runningScans, AtomicInteger daysScanned,
                        AtomicInteger daysHit) {
            this.config = config;
            this.dayQueue = dayQueue;
            this.entryQueue = entryQueue;
            this.indexScanners = indexScanners;
            this.producingDays = producingDays;
            this.producingScans = producingScans;
            this.runningScans = runningScans;
            this.daysScanned = daysScanned;
            this.daysHit = daysHit;
        }

        @Override
        public void run() {
            try {
                while (producingDays.get() || !dayQueue.isEmpty()) {
                    try {
                        String day = dayQueue.poll(25, TimeUnit.MILLISECONDS);
                        if (day != null) {
                            DayIndexScanner scanner = new DayIndexScanner(config);
                            DayIndexScan scan = new DayIndexScan(day, scanner, runningScans, entryQueue, daysHit);
                            indexScanners.execute(scan);
                            log.trace("scan submitted for day {}", day);
                            daysScanned.getAndIncrement();
                        }
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            } finally {
                producingScans.set(false);
            }
        }
    }

    /**
     * Wrapper around {@link DayIndexScanner#scan(String)} that allows for multithreaded execution
     */
    private static class DayIndexScan implements Runnable {

        private final String day;
        private final DayIndexScanner scanner;
        private final AtomicInteger runningScans;
        private final BlockingQueue<BitSetIndexEntry> entryQueue;
        private final AtomicInteger daysHit;

        public DayIndexScan(String day, DayIndexScanner scanner, AtomicInteger runningScans, BlockingQueue<BitSetIndexEntry> entryQueue,
                        AtomicInteger daysHit) {
            this.day = day;
            this.scanner = scanner;
            this.runningScans = runningScans;
            this.runningScans.getAndIncrement();
            this.entryQueue = entryQueue;
            this.daysHit = daysHit;
        }

        @Override
        public void run() {
            try {
                BitSetIndexEntry entry = scanner.scan(day);
                if (entry != null) {
                    log.trace("put day entry");
                    daysHit.getAndIncrement();
                    entryQueue.put(entry);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                runningScans.getAndDecrement();
            }
        }
    }

    /**
     * Bridges the {@link YearRowIterator} and the {@link #yearIndexScanners} service.
     * <p>
     * TODO -- multi-thread int a YearIndexScannerStream
     */
    private static class YearScanProducer implements Runnable {

        private final DayIndexConfig config;
        private final BlockingQueue<String> dayQueue;
        private final AtomicBoolean producingDays;

        private final AtomicInteger yearsScanned;
        private final AtomicInteger yearsHit;

        public YearScanProducer(DayIndexConfig config, BlockingQueue<String> dayQueue, AtomicBoolean producingDays, AtomicInteger yearsScanned,
                        AtomicInteger yearsHit) {
            this.config = config;
            this.dayQueue = dayQueue;
            this.producingDays = producingDays;
            this.yearsScanned = yearsScanned;
            this.yearsHit = yearsHit;
        }

        @Override
        public void run() {
            try {
                YearIndexScanner scanner = new YearIndexScanner(config);
                YearIndexIterator yearIter = new YearIndexIterator((ASTJexlScript) config.getNode());
                YearRowIterator iter = new YearRowIterator(config.getStartDate(), config.getEndDate());
                while (iter.hasNext()) {
                    String year = iter.next();
                    yearsScanned.getAndIncrement();
                    BitSetIndexEntry entry = scanner.scan(year);

                    if (entry == null) {
                        continue;
                    }

                    // day index iterator produces query plans
                    // year index iterator produces shard offsets
                    yearIter.setYear(entry.getYearOrDay());
                    yearIter.setShards(entry.getEntries());
                    yearsHit.getAndIncrement();

                    while (yearIter.hasNext()) {
                        // TODO -- standardize names based on what is produced
                        String day = yearIter.next();
                        try {
                            dayQueue.put(day);
                        } catch (InterruptedException e) {
                            log.error("interrupted while putting from year to day");
                            throw new RuntimeException(e);
                        }
                    }
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
                throw new RuntimeException(e);
            } finally {
                producingDays.set(false);
            }
        }
    }

    /**
     * Wrapper around the {@link DayRowIterator} to push days into a queue
     */
    private static class DateGenerator implements Runnable {

        private final Date startDate;
        private final Date endDate;

        private final BlockingQueue<String> dayQueue;
        private final AtomicBoolean producing;

        public DateGenerator(DayIndexConfig config, BlockingQueue<String> dayQueue, AtomicBoolean producing) {
            this.dayQueue = dayQueue;
            this.producing = producing;
            this.startDate = config.getStartDate();
            this.endDate = config.getEndDate();
        }

        @Override
        public void run() {
            try {
                DayRowIterator dayIterator = new DayRowIterator(startDate, endDate);
                while (dayIterator.hasNext()) {
                    String day = dayIterator.next();
                    log.trace("putting day: {}", day);
                    dayQueue.put(day);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                producing.set(false);
            }
        }
    }

}
