"""
Counts total number of volumes, terms, words per year.
Use this query ONLY for normalizing the EB articles stored in the HTO Knowledge Graph previously.
"""
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType

from ..sparql_service import NLSCollection
from ..query_utils import word_count_for_string_column, word_count_for_list_column


def do_query(df, config=None, logger=None, context=None):
    """
    Iterate through archives and count total number of documents,
    pages and words per year.

    For EB-ontology derived Knowledge Graphs, it returns result of form:

        {
          <YEAR>: [<NUM_VOLS>, <NUM_PAGES>, <NUM_TERMS>, <NUM_WORDS>],
          ...
        }
    Example:
      '1771':
      	- 3
      	- 2722
      	- 8923
      	- 1942064
      '1773':
     	- 3
     	- 2740
     	- 9187
     	- 1923682

    For NLS-ontology derived Knowledge Graphs, it returns result of form:
        {
          <YEAR>: [<NUM_VOLS>, <NUM_PAGES>,<NUM_WORDS>],
          ...
        }
    Example:
      '1671':
        - 1
        - 24
        - 4244
      '1681':
        - 1
        - 16
        - 4877

    :param archives: RDD of defoe.es.archive.Archive
    :type archives: pyspark.rdd.PipelinedRDD
    :param config_file: query configuration file (unused)
    :type config_file: str or unicode
    :param logger: logger (unused)
    :type logger: py4j.java_gateway.JavaObject
    :return: total number of documents, pages and words per year
    :rtype: list
    """
    collection = NLSCollection.EB.value
    if "collection" in config:
        collection = config["collection"]
    if collection == NLSCollection.EB.value:
        # Calculate word counts for each column and sum them
        fdf = df.withColumn("term_name_word_count", word_count_for_string_column("term_name")) \
            .withColumn("note_word_count", word_count_for_string_column("note")) \
            .withColumn("description_word_count", word_count_for_string_column("description")) \
            .withColumn("alter_names_word_count", word_count_for_list_column("alter_names")) \
            .withColumn("num_words", col("term_name_word_count") +
                        col("note_word_count") + col("description_word_count") + col("alter_names_word_count"))
        # number of words includes word counts from term_name, note, alter_names and description
        newdf = fdf.select(fdf.year, fdf.volume_uri, fdf.volume_number, fdf.num_pages, fdf.num_words)
    else:
        fdf = df.withColumn("num_words", word_count_for_string_column("description"))
        # number of words includes word counts from description
        newdf = fdf.select(fdf.year, fdf.volume_uri, fdf.volume_number, fdf.num_pages, fdf.num_words)

    num_words = newdf.groupBy("year").sum("num_words").withColumnRenamed("sum(num_words)", "total_num_words")

    df_groups = newdf.groupBy("year", "volume_uri", "volume_number", "num_pages").count()
    if collection == NLSCollection.EB.value:
        num_terms = df_groups.groupBy("year").sum("count").withColumnRenamed("sum(count)", "total_num_terms")

    num_pages= df_groups.groupBy("year").sum("num_pages").withColumnRenamed("sum(num_pages)", "total_num_pages")

    num_vols= df_groups.groupBy("year").count().withColumnRenamed("count", "total_num_volumes")

    vol_pages = num_vols.join(num_pages, on=["year"], how="inner")
    if collection == NLSCollection.EB.value:
        vol_pages_terms = vol_pages.join(num_terms, on=["year"], how="inner")
        result = vol_pages_terms.join(num_words, on=["year"], how="inner")
    else:
        result = vol_pages.join(num_words, on=["year"], how="inner")

    result = result.toPandas()

    r = {}
    for index, row in result.iterrows():
        year = row['year']
        r[year] = []
        r[year].append(row["total_num_volumes"])
        r[year].append(row["total_num_pages"])
        if "total_num_terms" in row:
            r[year].append(row["total_num_terms"])
        r[year].append(row["total_num_words"])
    return r