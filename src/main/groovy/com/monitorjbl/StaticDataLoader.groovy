package com.monitorjbl

import groovy.sql.Sql

import static com.monitorjbl.Utils.loadResource

class StaticDataLoader {
  def sql

  StaticDataLoader(Sql sql) {
    this.sql = sql
  }

  def createTables() {
    sql.execute("drop table if exists regions")
    sql.execute("drop table if exists systems")
    sql.execute("drop table if exists types")
    sql.execute("create table regions(id INTEGER, name VARCHAR(64), PRIMARY KEY (id))")
    sql.execute("create table systems(id INTEGER, region_id INTEGER, name VARCHAR(64), PRIMARY KEY (id))")
    sql.execute("create table types(id INTEGER, name VARCHAR(128), PRIMARY KEY (id))")
  }

  def load() {
    createTables()
    loadResource('regions.sql').eachLine {
      sql.executeUpdate(it)
    }
    loadResource('systems.sql').eachLine {
      sql.executeUpdate(it)
    }
    loadResource('types.sql').eachLine {
      sql.executeUpdate(it)
    }
  }
}
