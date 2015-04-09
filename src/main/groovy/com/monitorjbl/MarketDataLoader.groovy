package com.monitorjbl

import au.com.bytecode.opencsv.CSVParser
import groovy.sql.Sql
import groovy.util.logging.Slf4j

import java.text.DecimalFormat
import java.util.concurrent.TimeUnit

import static com.monitorjbl.Utils.loadResource

@Slf4j
class MarketDataLoader {
  static final def csv = new CSVParser(',' as char, '"' as char)
  static final def numFmt = new DecimalFormat("#,###")
  static final def bytesPerLine = 166L

  //mutable
  private def action
  private def running = false
  private def totalLines = 0L
  private def lineCount = 0L

  def final chunkSize = 50
  def final threads
  def final sql

  MarketDataLoader(Sql sql, Integer threads) {
    this.sql = sql
    this.threads = threads
  }

  private def createTables() {
    sql.execute("drop table if exists orders_raw")
    sql.execute("create table orders_raw (id BIGINT AUTO_INCREMENT, region_id INTEGER, system_id INTEGER, station_id INTEGER, " +
        "type_id INTEGER, bid DECIMAL, price float, volume INTEGER, PRIMARY KEY (id)) ENGINE=MEMORY")
  }

  private def createIndexes() {
    sql.execute("create index orders_by_system using hash on orders_raw (system_id)")
  }

  private def loadData(File file) {
    def pool = new BlockingExecutorService(threads, 100)
    lineCount = 0L
    action = "Load"
    def chunk = []
    file.eachLine {
      if (lineCount > 0 && lineCount % chunkSize == 0) {
        log.trace("Submitting $chunkSize lines for parsing")
        pool.execute(new Submission(chunk: chunk));
        chunk = []
      }

      if (lineCount++ > 0) {
        chunk << it
      }
    }

    //clean up stragglers
    pool.execute(new Submission(chunk: chunk));
    pool.shutdown()
    pool.awaitTermination(10, TimeUnit.MINUTES)
  }

  def removeDuplicates() {
    def time = System.currentTimeMillis()
    log.debug("Creating duplicate table")
    sql.execute("drop table if exists orders")
    sql.execute("create table orders (id BIGINT, region_id INTEGER, system_id INTEGER, station_id INTEGER, " +
        "type_id INTEGER, bid DECIMAL, price float, volume INTEGER, PRIMARY KEY (id))")

    def pool = new BlockingExecutorService(threads)
    def q = loadResource("find_duplicate_orders.sql").text
    sql.rows("select distinct system_id from orders_raw").each { row ->
      pool.execute({
        def dupes = sql.executeUpdate(q, [row.system_id, row.system_id])
        log.trace("Found $dupes duplicates from system ${row.system_id}")
      })
    }

    log.debug("Waiting for threads to complete")
    pool.shutdown()
    pool.awaitTermination(10, TimeUnit.MINUTES)

    log.debug("Duplicates identified, deleting")
    sql.execute("drop table if exists orders_raw")
    sql.execute("create index orders_by_system on orders (system_id)")
    log.debug("Deduplication complete in ${(System.currentTimeMillis() - time) / 1000} seconds.")
  }

  def load(String dumpLocation) {
    running = true
    def time = System.currentTimeMillis()
    try {
      log.debug("Creating tables")
      action = "Table"
      createTables()

      def file = new File(dumpLocation)
      totalLines = file.size() / bytesPerLine
      log.debug("Starting read (estimating ${numFmt.format(totalLines)} lines)")
      loadData(file)
      log.debug("Read ${numFmt.format(lineCount)} lines")

      log.debug("Creating indexes")
      action = "Index"
      createIndexes()

      log.debug("Removing duplicates")
      action = "Deduplication"
      removeDuplicates()

      log.debug("Load complete in ${(System.currentTimeMillis() - time) / 1000} seconds.")
    } catch (Exception e) {
      e.printStackTrace()
    } finally {
      running = false
    }
  }

  def progress() {
    return totalLines == 0 ? 0.0 : lineCount / totalLines
  }

  def running() {
    return running
  }

  def currentAction() {
    return action
  }

  static def retry(int times, Closure body) {
    int retries = 0
    while (retries++ < times) {
      try {
        body.call()
        return true
      } catch (e) {
        log.warn(e.message, e)
        sleep(1000 * new Random().nextLong(1000) * 3)
      }
    }
    return false
  }

  class Submission implements Runnable {
    def chunk = []

    void run() {
      chunk = chunk.collect { new Row(csv.parseLine(it)) }
      try {
        def query = "insert into orders_raw (region_id, system_id, station_id, type_id, bid, price, volume) values"
        chunk.each { query += "(?,?,?,?,?,?,?)," }
        query = query.substring(0, query.length() - 1)

        def params = []
        chunk.each { r -> params.addAll([r.regionId, r.systemId, r.stationId, r.typeId, r.bid, r.price, r.volremain]) }
        sql.execute(query, params)
      } catch (Exception e) {
        e.printStackTrace()
      }
    }
  }
}

class Row {
  def orderId, regionId, systemId, stationId, typeId, bid, price, minvolume, volremain

  Row(array) {
    orderId = array[0]
    regionId = array[1]
    systemId = array[2]
    stationId = array[3]
    typeId = array[4]
    bid = array[5]
    price = array[6]
    minvolume = array[7]
    volremain = array[8]
  }
}