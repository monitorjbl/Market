package com.monitorjbl

import groovy.sql.Sql
import org.apache.commons.dbcp2.BasicDataSource
import org.fusesource.jansi.AnsiConsole

import java.text.DecimalFormat

import static org.fusesource.jansi.Ansi.*;
import static org.fusesource.jansi.Ansi.Color.*;

class Main {
  static {
    AnsiConsole.systemInstall();
  }

  def numFmt = new DecimalFormat("###")
  def threads = 50
  def sql

  def loadMarketData() {
    def mkt = new MarketDataLoader(sql, threads)
    Thread.start {
      mkt.load('./dump.csv')
    }

    while (!mkt.running())
      Thread.sleep(50)

    printImmediate(ansi().fg(GREEN).a("Loading market data").fg(WHITE).a("...0%"))
    while (mkt.running() && ["Table", "Load"].contains(mkt.currentAction())) {
      printImmediate('\r')
      printImmediate(ansi().fg(GREEN).a("Loading market data").fg(WHITE).a("...${numFmt.format(mkt.progress() * 100)}%"))
      Thread.sleep(500)
    }
    printImmediate('\r')
    println(ansi().eraseLine().fg(GREEN).a("Loading market data").fg(WHITE).a("...done!"))

    printImmediate(ansi().fg(GREEN).a("Generating indexes").fg(WHITE).a("..."))
    while (mkt.running() && mkt.currentAction() == 'Index')
      Thread.sleep(1000)
    printImmediate('\r')
    println(ansi().eraseLine().fg(GREEN).a("Generating indexes").fg(WHITE).a("...done!"))

    printImmediate(ansi().fg(GREEN).a("Removing duplicate orders").fg(WHITE).a("..."))
    while (mkt.running() && mkt.currentAction() == 'Deduplication')
      Thread.sleep(1000)
    printImmediate('\r')
    println(ansi().eraseLine().fg(GREEN).a("Removing duplicate orders").fg(WHITE).a("...done!"))
  }

  def loadStaticData() {
    printImmediate(ansi().fg(GREEN).a("Loading static data").fg(WHITE).a("..."))
    new StaticDataLoader(sql).load()
    println(ansi().fg(WHITE).a("done!"))
  }

  def generateViews() {
    printImmediate(ansi().fg(GREEN).a("Generating views").fg(WHITE).a("..."))
    new ViewGenerator(sql, threads).generate()
    println(ansi().fg(WHITE).a("done!"))
  }

  def dbConnect() {
    def ds = new BasicDataSource();
    ds.setDriverClassName("com.mysql.jdbc.Driver");
    ds.setUsername("root");
    ds.setPassword("");
    ds.setUrl("jdbc:mysql://localhost:3306/market");
    ds.setMinIdle(1);
    ds.setMaxIdle(threads);
    ds.setMaxTotal(threads)
    ds.setMaxOpenPreparedStatements(180);
    sql = new Sql(ds)
  }

  def printImmediate(Object val) {
    System.out.print(val)
    System.out.flush()
  }

  def splash() {
    println(
        "" +
            "-------------------\n" +
            "| EVE Market Data |\n" +
            "-------------------"
    );
  }

  public static void main(String[] args) {
    def main = new Main()
    main.splash()
    main.dbConnect()

    if (args.contains("help")) {
      println("Usage information")
      println(ansi().fg(GREEN).a("\tstatic\t").fg(WHITE).a("Loads static content into database"))
      println(ansi().fg(GREEN).a("\tmarket\t").fg(WHITE).a("Loads EVE-Central data into database"))
      println(ansi().fg(GREEN).a("\tviews\t").fg(WHITE).a("Generates aggregated views for querying"))
      println(ansi().fg(GREEN).a("\tall\t").fg(WHITE).a("Runs all commands to fully populate database"))
    } else if (args.contains("all")) {
      main.loadStaticData()
      main.loadMarketData()
      main.generateViews()
    } else {
      if (args.contains("static")) {
        main.loadStaticData()
      }
      if (args.contains("market")) {
        main.loadMarketData()
      }
      if (args.contains("views")) {
        main.generateViews()
      }
    }
  }
}
