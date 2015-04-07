package com.monitorjbl

import groovy.sql.Sql
import groovy.util.logging.Slf4j

import static com.monitorjbl.Utils.loadResource

@Slf4j
class ViewGenerator {
  def final sql
  def final threads

  ViewGenerator(Sql sql, Integer threads) {
    this.sql = sql
    this.threads = threads
  }

  def createTables() {
    sql.execute("drop table if exists profit_mv")
    sql.execute("create table profit_mv(type_id INTEGER, region_id BIGINT, system_id INTEGER, " +
        "buy float, sell float, buy_volume bigint, sell_volume bigint)")
  }

  def createIndexes() {
    sql.execute("create index profit_mv_system on profit_mv (region_id, system_id, type_id)")
    sql.execute("create index profit_mv_region on profit_mv (region_id, type_id)")
  }

  def generate() {
    def time = System.currentTimeMillis()

    log.debug("Creating tables")
    createTables()

    log.debug("Generating data")
    loadProfitView()

    log.debug("Creating indexes")
    createIndexes()

    log.debug("Generation complete in ${(System.currentTimeMillis() - time) / 1000} seconds.")
  }

  def loadProfitView() {
    //thread by loading per system
    def pool = new BlockingExecutorService(threads, threads * 10)
    def q = loadResource("profit.sql").text
    sql.rows("select distinct region_id, system_id from orders").each { row ->
      pool.execute({
        sql.executeUpdate(q, [row.region_id, row.system_id, row.region_id, row.system_id])
        log.trace("Loaded sytem ${row.system_id}")
      })
    }
    pool.shutdown()
  }
}
