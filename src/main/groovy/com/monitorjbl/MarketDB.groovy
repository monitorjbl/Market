package com.monitorjbl

import groovy.sql.Sql

class MarketDB {
  def sql

  MarketDB(sql) {
    this.sql = new Sql(ds)
  }

  def createTables() {
    sql.execute("drop table if exists orders")
    sql.execute("create table orders (id BIGINT, region_id INTEGER, system_id INTEGER, station_id INTEGER, " +
        "type_id INTEGER, bid DECIMAL, price float, volume INTEGER)")
  }

  def createIndexes() {
    sql.execute("create index orders_station on orders (region_id, system_id, bid)")
  }

  def insertOrder(List<Row> rows) {
    try {
      def query = "insert into orders values"
      rows.each { query += "(?,?,?,?,?,?,?,?)," }
      query = query.substring(0, query.length() - 1)

      def params = []
      rows.each { r -> params.addAll([r.orderId, r.regionId, r.systemId, r.stationId, r.typeId, r.bid, r.price, r.volremain]) }
      sql.execute(query, params)
    } catch (Exception e) {
      e.printStackTrace()
    }
  }
}

//class Row {
//  def orderId, regionId, systemId, stationId, typeId, bid, price, minvolume, volremain
//
//  Row(array) {
//    orderId = array[0]
//    regionId = array[1]
//    systemId = array[2]
//    stationId = array[3]
//    typeId = array[4]
//    bid = array[5]
//    price = array[6]
//    minvolume = array[7]
//    volremain = array[8]
//  }
//}