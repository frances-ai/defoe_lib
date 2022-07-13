import json

class DefoeConfig:
  def __init__(self):
    spark_url = ""
  
  @staticmethod
  def from_dict(vals):
    config = DefoeConfig()
    config.spark_url = vals["sparkUrl"]
    return config
