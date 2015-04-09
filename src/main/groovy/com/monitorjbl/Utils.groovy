package com.monitorjbl

class Utils {
  static def loadResource(String name) {
    Utils.class.classLoader.getResource(name).openStream()
  }

}
