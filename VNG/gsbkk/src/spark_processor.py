# -*- coding: utf-8 -*-
import json
import sys
from functools import reduce
from typing import Dict, Any
from jinja2 import Environment

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType


#region ===== Helper Classes =====
class JobConfigs(object):
  def __init__(self, models, processor='spark', game_id='', event_date='', 
               config_dir='hdfs:///user/gsbkk-workspace-yc9t6/configs'):
    self.models = models
    self.processor = processor
    self.game_id = game_id
    self.event_date = event_date
    self.config_dir = config_dir


class ModelConfigs(object):
  def __init__(self, location, name=None, type='parquet', case_sensitive=False,
               options=None, preprocess='', default_when_blank=False,
               num_partitions=1, partition_by=None, merge=False,
               postprocess='', sql_model=None):
    self.location = location
    self.name = name
    self.type = type
    self.case_sensitive = case_sensitive
    self.options = options if options is not None else {}
    self.preprocess = preprocess
    self.default_when_blank = default_when_blank
    self.num_partitions = num_partitions
    self.partition_by = partition_by if partition_by is not None else []
    self.merge = merge
    self.postprocess = postprocess
    self.sql_model = sql_model
#endregion

# Initialize Jinja
jinja_env = Environment()


def handler(args):
  job_configs = convert_args_to_dict(args)
  config_dir = job_configs.get('config_dir', 'hdfs:///user/gsbkk-workspace-yc9t6/configs')
  
  # Initialize Spark session first (needed to read HDFS files)
  spark = (
    SparkSession.builder
    .getOrCreate()  # Gets an existing SparkSession or creates a new one
  )
  
  # Read game info from HDFS
  game_info_path = f"{config_dir}/{job_configs['game_id']}/info.json"
  game_info_content = read_hadoop_file(game_info_path, spark)
  game_info = json.loads(game_info_content)

  for file_name in job_configs['models'].split(','):
    file_path = f"{config_dir}/{file_name}"
    sql = read_hadoop_file(file_path, spark)
    process(sql, spark, game_info, job_configs)

  spark.stop()


def process(sql_model, spark, game_info, job_configs):
  targets = []

  vars = game_info
  vars['config_dir'] = job_configs['config_dir']
  vars['game_id'] = job_configs['game_id']
  vars['event_date'] = job_configs['event_date']

  env = Environment(extensions=['jinja2.ext.do'])
  env.globals['source'] = lambda params: source(spark, ModelConfigs(**params), vars)
  env.globals['create_or_replace_table'] = lambda params: create_or_replace_table(targets, ModelConfigs(**params))

  sql_query = parse_template(sql_model, vars, env)
  target_data = spark.sql(f"""{sql_query}""")
  save_data(target_data, spark, targets[0], vars)


def source(spark, model, vars):
  data = load_data(spark, model, vars)
  if data is None and model.default_when_blank:
    print('`default_when_blank` is enabled, try to create empty DataFrame')
    data = spark.createDataFrame([], StructType([]))

  data.createOrReplaceTempView(model.name)
  return model.name


def create_or_replace_table(targets, target_model):
  targets.append(target_model)
  return ''


#region ===== Adapter =====
def load_data(spark, model, vars):
  if model.case_sensitive:
    spark.conf.set('spark.sql.caseSensitive', 'true')

  # if model.type == 'jdbc':
  #   return load_data_with_jdbc(spark, model, vars)
  
  try:
    path = parse_template(model.location, vars, None)
    print('Load {0} data from "{1}"'.format(model.type, path))

    data = load_data_with_jdbc(spark, path, model, vars) if model.type == 'jdbc' else spark.read.format(model.type).options(**model.options).load(path)
    if data.head(1):
      return data
    else:
      print('Empty from "{0}"'.format(path))
      return None
  except Exception as ex:
    print('Cannot load {0} data from "{1}" caused by: {2}'.format(model.type, model.location, ex))
    return None


def load_data_with_jdbc(spark: SparkSession, path: str, model: ModelConfigs, vars: Dict[str, Any]) -> DataFrame:
  cred = json.loads(read_hadoop_file(path, spark))

  dbtable = model.name
  if model.preprocess:
    sql_file_path = parse_template(f'{{{{config_dir}}}}/{model.preprocess}', vars, None)
    sql_query = read_hadoop_file(sql_file_path, spark)
    dbtable = f'({parse_template(sql_query, vars, None)}) as {model.name}'
    print(f'DEBUG: dbtable = {dbtable}')
  options = {
    'driver': cred['driver'],
    'url': cred['url'],
    'dbtable': dbtable, # Can be table name or subquery
    'user': cred['user'],
    'password': cred['password'],
    'fetchsize': '10000',
    **model.options
  }

  return spark.read.format(model.type).options(**options).load()


def save_data(data, spark, model, vars):
  if model.type == 'jdbc':
    save_data_with_jdbc(data, spark, model, vars)
  else:
    path = parse_template(model.location, vars, None)
    print('Save data to "{0}" as {1}, num_partitions = {2}, partition_by = {3}, columns = {4}'.format(
      path, model.type, model.num_partitions, model.partition_by, "|".join(data.columns)))

    writer = data.coalesce(model.num_partitions).write.format(model.type).options(**model.options).mode('overwrite')
    if len(model.partition_by) > 0:
      writer = writer.partitionBy(model.partition_by)

    writer.save(path)


def save_data_with_jdbc(data, spark, model, vars):
  path = parse_template(model.location, vars, None)
  cred = json.loads(read_hadoop_file(path, spark))
  options = {
    'driver': cred['driver'],
    'url': cred['url'],
    'dbtable': model.name,
    'user': cred['user'],
    'password': cred['password'],
    'batchsize': '10000',
  }
  data.write.format(model.type).options(**options).mode('overwrite').save()

  print("Save data to {0}".format(model.name))
#endregion


#region ===== Utils =====
def parse_template(str, vars, env):
  try:
    if env is None:
      env = Environment()
    return env.from_string(str).render(**vars)
  except Exception as ex:
    print('ERROR: Template parsing failed!')
    print('Exception: {0}'.format(ex))
    print('Template (first 500 chars): {0}'.format(str[:500]))
    raise  # Let it fail fast with clear error


def read_hadoop_file(file_path, spark):
  path = parse_template(file_path, {}, None)
  lines = spark.sparkContext.textFile(path).collect()
  return "".join(lines)


def convert_args_to_dict(args):
  def combine_dict(dict, arg):
    arr = arg.split('=')
    dict[arr[0]] = arr[1]
    return dict

  return reduce(lambda dict, arg: combine_dict(dict, arg), args, {})
#endregion


if __name__ == "__main__":
  if len(sys.argv) < 2:
    raise ValueError('No arguments passed.')

  handler(sys.argv[1:])
