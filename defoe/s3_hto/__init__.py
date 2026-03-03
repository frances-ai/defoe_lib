from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType

from .queries import publication_normalized, frequency_keysearch_by_year, snippet_keysearch_by_year, \
    fulltext_keysearch_by_year, geoparser_by_year, lexicon_diversity, frequency_distribution, person_entity_recognition
from .sparql_service import get_hto_object, NLSCollection

default_hto_sparql_endpoint = "http://127.0.0.1:3030/hto"


def get_queries():
    return {
        "publication_normalized": publication_normalized.do_query,
        "frequency_keysearch_by_year": frequency_keysearch_by_year.do_query,
        "snippet_keysearch_by_year": snippet_keysearch_by_year.do_query,
        "fulltext_keysearch_by_year": fulltext_keysearch_by_year.do_query,
        "geoparser_by_year": geoparser_by_year.do_query,
        "lexicon_diversity": lexicon_diversity.do_query,
        "frequency_distribution": frequency_distribution.do_query,
        "person_entity_recognition": person_entity_recognition.do_query
    }


def get_hto_df(sparql_endpoint, collection_name, source, context):
    hto_object = get_hto_object(sparql_endpoint, collection_name, source)
    if collection_name == NLSCollection.EB.value:
        schema = StructType([
            StructField("term_uri", StringType()),
            StructField("alter_names", ArrayType(StringType())),
            StructField("term_name", StringType()),
            StructField("note", StringType()),
            StructField("year", StringType()),
            StructField("source_file_uri", StringType()),
            StructField("source_file_path", StringType()),
            StructField("description", StringType()),
            StructField("edition_uri", StringType()),
            StructField("edition_title", StringType()),
            StructField("edition_number", IntegerType()),
            StructField("volume_uri", StringType()),
            StructField("volume_title", StringType()),
            StructField("volume_number", IntegerType()),
            StructField("num_pages", IntegerType()),
            StructField("letters", StringType()),
            StructField("part", IntegerType()),
            StructField("start_page_uri", StringType()),
            StructField("start_page_number", IntegerType()),
        ])
    else:
        schema = StructType([
            StructField("page_uri", StringType()),
            StructField("page_number", IntegerType()),
            StructField("year", StringType()),
            StructField("source_file_uri", StringType()),
            StructField("source_file_path", StringType()),
            StructField("description", StringType()),
            StructField("series_uri", StringType()),
            StructField("series_title", StringType()),
            StructField("series_number", IntegerType()),
            StructField("volume_uri", StringType()),
            StructField("volume_title", StringType()),
            StructField("volume_number", IntegerType()),
            StructField("num_pages", IntegerType())
        ])
    df = context.createDataFrame(hto_object, schema=schema)
    return df
