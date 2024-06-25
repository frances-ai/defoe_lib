"""
Gets the snippet of each term (from a list of keywords or keysentences) along with the metadata.
We recommend to use this query when we want to select a window of words (snippet lenght) around each term, instead of selecting
all the words of the page in which the term was found. 
"""

from operator import add

from pyspark.sql.functions import concat_ws, col

from defoe import query_utils
from defoe.hto.sparql_service import NLSCollection
from defoe.hto.query_utils import get_articles_list_matches, blank_as_null
from defoe.nls.query_utils import get_text_keysentence_idx, get_concordance_string
from defoe.nls.query_utils import preprocess_clean_page
from lexicalrichness import LexicalRichness


from functools import  reduce

def do_query(df, config=None, logger=None, context=None):
    """
    Gets concordance using a window of words (here it is configured to 10), for keywords and groups by date.
    Store the snippet (10 words before and after each term). 

    Data in sparql have the following colums:
    
    "archive_filename", definition, edition, header|page|term| title| uri|volume|year"

    config_file must be the path to a lexicon file with a list of the keywords 
    to search for, one per line.
    
    Also the config_file can indicate the preprocess treatment, along with the defoe
    path, and the type of operating system. 

      For EB-ontology (e.g. total_eb.ttl) derived Knowledge Graphs, it returns result of form:
        {
          <YEAR>:
          [
            [- title: 
             - edition:
             - archive_filename:
             - volume:
             - letters:
             - part:
             - page number:
             - header:
             - keysearch-term:
             - term:
             - uri:
             - snippet: ], 
             [], 
            ...
         
          <YEAR>:
          ...
        }

      For NLS-ontology (e.g. chapbooks_scotland.ttl) derived Knowledge Graphs, it returns result of form:
       {
          <YEAR>:
          [
            [- title:
             - serie:
             - archive_filename:
             - volume:
             - volumeTitle 
             - part:
             - page number:
             - volumeId:
             - keysearch-term:
             - term:
             - numWords:
             - snippet: ],
             [],
            ...

          <YEAR>:
          ...
        }

    :type issues: pyspark.rdd.PipelinedRDD
    :param config_file: query configuration file
    :type config_file: str or unicode
    :param logger: logger (unused)
    :type logger: py4j.java_gateway.JavaObject
    :return: information on documents in which keywords occur grouped
    by date
    :rtype: dict
    """

    preprocess_type = query_utils.extract_preprocess_word_type(config)

    if "start_year" in config:
        start_year = int(config["start_year"])
    else:
        start_year = None

    if "end_year" in config:
        end_year = int(config["end_year"])
    else:
        end_year = None

    if "level" in config:
        # volume, edition, series, year
        level = config["level"]
    else:
        level = "year"

    collection = NLSCollection.EB.value
    if "collection" in config:
        collection = config["collection"]

    if collection == NLSCollection.EB.value:
        fdf = df.withColumn("description",
                            concat_ws(" ", col("term_name"), col("note"),
                                      concat_ws(" ", col("alter_names"))
                                      , col("description")))
        if start_year and end_year:
            newdf = fdf.filter(fdf.description.isNotNull()).filter(fdf.year >= start_year).filter(
                fdf.year <= end_year).select(fdf.year, fdf.edition_title, fdf.edition_uri,
                                             fdf.edition_number, fdf.volume_uri, fdf.volume_title, fdf.volume_number, fdf.description)
        elif start_year:
            newdf = fdf.filter(fdf.description.isNotNull()).filter(fdf.year >= start_year).select(fdf.year, fdf.edition_title, fdf.edition_uri,
                                             fdf.edition_number, fdf.volume_uri, fdf.volume_title, fdf.volume_number, fdf.description)
        elif end_year:
            newdf = fdf.filter(fdf.description.isNotNull()).filter(fdf.year <= end_year).select(fdf.year, fdf.edition_title, fdf.edition_uri,
                                             fdf.edition_number, fdf.volume_uri, fdf.volume_title, fdf.volume_number, fdf.description)
        else:
            newdf = fdf.filter(fdf.description.isNotNull()).select(fdf.year, fdf.edition_title, fdf.edition_uri,
                                             fdf.edition_number, fdf.volume_uri, fdf.volume_title, fdf.volume_number, fdf.description)
    else:
        fdf = df.withColumn("description", blank_as_null("description"))
        if start_year and end_year:
            newdf = fdf.filter(fdf.description.isNotNull()).filter(fdf.year >= start_year).filter(
                fdf.year <= end_year).select(fdf.year, fdf.series_title, fdf.series_uri,
                                             fdf.series_number, fdf.volume_uri, fdf.volume_title, fdf.volume_number, fdf.description)
        elif start_year:
            newdf = fdf.filter(fdf.description.isNotNull()).filter(fdf.year >= start_year).select(fdf.year, fdf.series_title, fdf.series_uri,
                                             fdf.series_number, fdf.volume_uri, fdf.volume_title, fdf.volume_number, fdf.description)
        elif end_year:
            newdf = fdf.filter(fdf.description.isNotNull()).filter(fdf.year <= end_year).select(fdf.year, fdf.series_title, fdf.series_uri,
                                             fdf.series_number, fdf.volume_uri, fdf.volume_title, fdf.volume_number, fdf.description)
        else:
            newdf = fdf.filter(fdf.description.isNotNull()).select(fdf.year, fdf.series_title, fdf.series_uri,
                                             fdf.series_number, fdf.volume_uri, fdf.volume_title, fdf.volume_number, fdf.description)

    articles = newdf.rdd.map(tuple)
    # preprocess
    # EB: year-0, edition_title-1, edition_uri-2, edition_number-3, volume_uri-4, volume_title-5, volume_number-6, description-7,
    # NLS: year-0, series_title-1, series_uri-2, series_number-3, volume_uri-4, volume_title-5, volume_number-6, description-7,
    preprocess_articles = articles.flatMap(lambda t_articles: [(t_articles[0], t_articles[1], t_articles[2], t_articles[3],
                                                               t_articles[4], t_articles[5], t_articles[6],
                                                               preprocess_clean_page(t_articles[7], preprocess_type))])

    result = None

    if level == "volume":
        # Map to key-value pairs ((year-0, edition_title-1, edition_uri-2, edition_number-3, volume_uri-4, volume_title-5, volume_number-6), description-7)
        volumes_texts = preprocess_articles.map(lambda p_article: ((p_article[0], p_article[1], p_article[2],
                                                                    p_article[3], p_article[4], p_article[5], p_article[6]),
                                                                   p_article[7]))
        volumes = volumes_texts.groupByKey().mapValues(lambda descriptions: ' '.join(descriptions))

        volumes_ld = volumes.map(lambda volume: (volume[0][0], volume[0][1], volume[0][2], volume[0][3], volume[0][4],
                                                 volume[0][5], volume[0][6],
                                                 LexicalRichness(text=volume[1], preprocessor=None, tokenizer=query_utils.tokenize)))
        # year-0, (edition_title-1, edition_uri-2, edition_number-3, volume_uri-4, volume_title-5, volume_number-6, terms-7, words-8, ttr-9, Maas-10, mtld-11)
        volumes_ld = volumes_ld.map(lambda ld: (ld[0], (ld[1], ld[2], ld[3], ld[4],ld[5], ld[6], ld[7].terms, ld[7].words, ld[7].ttr, ld[7].Maas, ld[7].mtld())))
        result = volumes_ld.groupByKey()\
            .map(lambda year_volumes_ld: (year_volumes_ld[0], list(year_volumes_ld[1])))\
            .collect()
    elif level == "edition" or level == "series":
        # Map to key-value pairs ((year-0, edition_title-1, edition_uri-2, edition_number-3), description-4)
        es_texts = preprocess_articles.map(lambda p_article: ((p_article[0], p_article[1], p_article[2], p_article[3]),
                                                                   p_article[7]))
        es = es_texts.groupByKey().mapValues(lambda descriptions: ' '.join(descriptions))

        es_ld = es.map(lambda volume: (volume[0][0], volume[0][1], volume[0][2], volume[0][3],
                                                 LexicalRichness(text=volume[1], preprocessor=None,
                                                                 tokenizer=query_utils.tokenize)))
        # year-0, (edition_title-1, edition_uri-2, edition_number-3, terms-4, words-5, ttr-6, Maas-7, mtld-8)
        es_ld = es_ld.map(lambda ld: (ld[0], (ld[1], ld[2], ld[3], ld[4].terms, ld[4].words, ld[4].ttr, ld[4].Maas, ld[4].mtld())))
        result = es_ld.groupByKey() \
            .map(lambda year_volumes_ld: (year_volumes_ld[0], list(year_volumes_ld[1]))) \
            .collect()

    elif level == "year":
        # Map to key-value pairs ((year-0), description-1)
        es_texts = preprocess_articles.map(lambda p_article: (p_article[0],
                                                              p_article[7]))
        es = es_texts.groupByKey().mapValues(lambda descriptions: ' '.join(descriptions))

        es_ld = es.map(lambda volume: (volume[0],
                                       LexicalRichness(text=volume[1], preprocessor=None,
                                                       tokenizer=query_utils.tokenize)))
        # year-0, (terms-1, words-2, ttr-3, Maas-4, mtld-5)
        es_ld = es_ld.map(lambda ld: (ld[0], (ld[1].terms, ld[1].words, ld[1].ttr, ld[1].Maas, ld[1].mtld())))
        result = es_ld.groupByKey() \
            .map(lambda year_volumes_ld: (year_volumes_ld[0], list(year_volumes_ld[1]))) \
            .collect()

    return result


