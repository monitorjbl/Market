package com.monitorjbl

import groovy.sql.Sql
import org.apache.commons.dbcp2.BasicDataSource

import java.text.DecimalFormat

class Main {
  def numFmt = new DecimalFormat("###")
  def threads = 20
  def sql

  def loadData() {
    def mkt = new MarketDataLoader(sql, threads)
    def started = false
    Thread.start {
      started = true
      mkt.load('/Users/thundermoose/Downloads/2015-04-05.dump')
    }

    while (!started)
      Thread.sleep(10)

    while (mkt.loading()) {
      println(numFmt.format(mkt.progress() * 100) + "%")
      Thread.sleep(500)
    }
  }

  def generateViews(){
    def generator = new ViewGenerator(sql, threads)
    generator.generate()
  }

  def dbConnect() {
    def ds = new BasicDataSource();
    ds.setDriverClassName("com.mysql.jdbc.Driver");
    ds.setUsername("root");
    ds.setPassword("");
    ds.setUrl("jdbc:mysql://localhost:3306/market");
    ds.setMinIdle(5);
    ds.setMaxIdle(20);
    ds.setMaxTotal(20)
    ds.setMaxOpenPreparedStatements(180);
    sql = new Sql(ds)
  }

  public static void main(String[] args) {
    def main = new Main()
    main.dbConnect()
    main.loadData()
    main.generateViews()
  }
}
