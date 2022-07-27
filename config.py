import json

class DefoeConfig:
  def __init__(self, spark_url="", fuseki_url=""):
    self.spark_url = spark_url
    self.fuseki_url = fuseki_url

  @staticmethod
  def from_dict(vals):
    config = DefoeConfig()
    config.spark_url = vals["sparkUrl"]
    config.fuseki_url = vals["fusekiUrl"]
    return config
