import json


class DefoeConfig:
  def __init__(self, spark_url="", fuseki_url=""):
    self.spark_url = spark_url
    self.fuseki_url = fuseki_url
    self.cluster = None

  @staticmethod
  def from_dict(vals):
    config = DefoeConfig()
    config.spark_url = vals["sparkUrl"]
    config.fuseki_url = vals["fusekiUrl"]
    if "cluster" in vals:
      config.cluster = ClusterConfig.from_dict(vals["cluster"])
    return config

class ClusterConfig:
  def __init__(self, module="", environment=""):
    self.module = module
    self.environment = environment
  
  @staticmethod
  def from_dict(vals):
    config = ClusterConfig()
    config.module = vals.get("module", None)
    config.environment = vals.get("environment", None)
    return config

