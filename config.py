import json
import os


class DefoeConfig:
  def __init__(self, spark_url="", fuseki_url="", chapbooks_scotland_url = ""):
    self.spark_url = spark_url
    self.fuseki_url = fuseki_url
    self.chapbooks_scotland_url = chapbooks_scotland_url
    self.remote = None

  @staticmethod
  def from_dict(vals):
    config = DefoeConfig()
    config.spark_url = vals["sparkUrl"]
    config.fuseki_url = vals["fusekiUrl"]
    config.chapbooks_scotland_url = vals["chapbooksScotlandUrl"]
    if "remote" in vals:
      config.remote = RemoteConfig.from_dict(vals["remote"])
    return config

class RemoteConfig:
  def __init__(self, module="", environment="", driver_host=""):
    self.module = module
    self.environment = environment
    self.driver_host = driver_host

  @staticmethod
  def from_dict(vals):
    config = RemoteConfig()
    config.module = vals["module"]
    config.environment = vals["environment"]
    config.driver_host = vals["driverHost"]
    return config

