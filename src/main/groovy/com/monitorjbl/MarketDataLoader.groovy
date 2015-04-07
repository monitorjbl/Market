package com.monitorjbl

import au.com.bytecode.opencsv.CSVParser
import groovy.sql.Sql
import groovy.util.logging.Slf4j

import java.text.DecimalFormat

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
    sql.execute("drop table if exists orders")
    sql.execute("create table orders (id BIGINT, region_id INTEGER, system_id INTEGER, station_id INTEGER, " +
        "type_id INTEGER, bid DECIMAL, price float, volume INTEGER)")
  }

  private def createIndexes() {
    sql.execute("create index orders_station on orders (region_id, system_id, bid)")
  }

  def load(String dumpLocation) {
    running = true
    try {
      log.debug("Creating tables")
      action = "Table"
      createTables()

      def time = System.currentTimeMillis()
      def pool = new BlockingExecutorService(threads, 100)
      def file = new File(dumpLocation)
      lineCount = 0L
      totalLines = file.size() / bytesPerLine

      log.debug("Starting read (estimating ${numFmt.format(totalLines)} lines)")
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
      log.debug("Read ${numFmt.format(lineCount)} lines")

      log.debug("Creating indexes")
      action = "Index"
      createIndexes()
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

  def currentAction(){
    return action
  }

  class Submission implements Runnable {
    def chunk = []

    void run() {
      chunk = chunk.collect { new Row(csv.parseLine(it)) }
      try {
        def query = "insert into orders values"
        chunk.each { query += "(?,?,?,?,?,?,?,?)," }
        query = query.substring(0, query.length() - 1)

        def params = []
        chunk.each { r -> params.addAll([r.orderId, r.regionId, r.systemId, r.stationId, r.typeId, r.bid, r.price, r.volremain]) }
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