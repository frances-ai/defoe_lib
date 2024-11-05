"""
Gets the frequency distribution of the most 100 words across a collection, an edition, a series, or a volume.
User can specify the level (collection, edition/series, volume), and the words to be excluded.
"""

from operator import add

from nltk.corpus import stopwords
from pyspark.sql.functions import concat_ws, col

from defoe import query_utils
from defoe.hto.sparql_service import NLSCollection
from defoe.hto.query_utils import get_articles_list_matches, blank_as_null
from defoe.nls.query_utils import get_text_keysentence_idx, get_concordance_string
from defoe.nls.query_utils import preprocess_clean_page
from lexicalrichness import LexicalRichness

from functools import reduce


def do_query(df, config=None, logger=None, context=None):
    """
    Gets the frequency distribution of the most 100 words across a collection, an edition, a series, or a volume.

    Data in sparql have the following columns:

    "archive_filename", definition, edition, header|page|term| title| uri|volume|year"

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
        level = "collection"

    # the most common TOP_N words
    TOP_N = 100
    exclude_words = list(set(stopwords.words('english')))
    if "exclude_words" in config:
        exclude_words += config["exclude_words"]

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
                                             fdf.edition_number, fdf.volume_uri, fdf.volume_title, fdf.volume_number,
                                             fdf.description)
        elif start_year:
            newdf = fdf.filter(fdf.description.isNotNull()).filter(fdf.year >= start_year).select(fdf.year,
                                                                                                  fdf.edition_title,
                                                                                                  fdf.edition_uri,
                                                                                                  fdf.edition_number,
                                                                                                  fdf.volume_uri,
                                                                                                  fdf.volume_title,
                                                                                                  fdf.volume_number,
                                                                                                  fdf.description)
        elif end_year:
            newdf = fdf.filter(fdf.description.isNotNull()).filter(fdf.year <= end_year).select(fdf.year,
                                                                                                fdf.edition_title,
                                                                                                fdf.edition_uri,
                                                                                                fdf.edition_number,
                                                                                                fdf.volume_uri,
                                                                                                fdf.volume_title,
                                                                                                fdf.volume_number,
                                                                                                fdf.description)
        else:
            newdf = fdf.filter(fdf.description.isNotNull()).select(fdf.year, fdf.edition_title, fdf.edition_uri,
                                                                   fdf.edition_number, fdf.volume_uri, fdf.volume_title,
                                                                   fdf.volume_number, fdf.description)
    else:
        fdf = df.withColumn("description", blank_as_null("description"))
        if start_year and end_year:
            newdf = fdf.filter(fdf.description.isNotNull()).filter(fdf.year >= start_year).filter(
                fdf.year <= end_year).select(fdf.year, fdf.series_title, fdf.series_uri,
                                             fdf.series_number, fdf.volume_uri, fdf.volume_title, fdf.volume_number,
                                             fdf.description)
        elif start_year:
            newdf = fdf.filter(fdf.description.isNotNull()).filter(fdf.year >= start_year).select(fdf.year,
                                                                                                  fdf.series_title,
                                                                                                  fdf.series_uri,
                                                                                                  fdf.series_number,
                                                                                                  fdf.volume_uri,
                                                                                                  fdf.volume_title,
                                                                                                  fdf.volume_number,
                                                                                                  fdf.description)
        elif end_year:
            newdf = fdf.filter(fdf.description.isNotNull()).filter(fdf.year <= end_year).select(fdf.year,
                                                                                                fdf.series_title,
                                                                                                fdf.series_uri,
                                                                                                fdf.series_number,
                                                                                                fdf.volume_uri,
                                                                                                fdf.volume_title,
                                                                                                fdf.volume_number,
                                                                                                fdf.description)
        else:
            newdf = fdf.filter(fdf.description.isNotNull()).select(fdf.year, fdf.series_title, fdf.series_uri,
                                                                   fdf.series_number, fdf.volume_uri, fdf.volume_title,
                                                                   fdf.volume_number, fdf.description)

    articles = newdf.rdd.map(tuple)
    # preprocess
    # EB: year-0, edition_title-1, edition_uri-2, edition_number-3, volume_uri-4, volume_title-5, volume_number-6, description-7,
    # NLS: year-0, series_title-1, series_uri-2, series_number-3, volume_uri-4, volume_title-5, volume_number-6, description-7,
    preprocess_articles = articles.flatMap(
        lambda t_articles: [(t_articles[0], t_articles[1], t_articles[2], t_articles[3],
                             t_articles[4], t_articles[5], t_articles[6],
                             preprocess_clean_page(t_articles[7], preprocess_type))])

    result = None

    if level == "volume":
        # ((year-0, edition_title-1, edition_uri-2, edition_number-3, volume_uri-4, volume_title-5, volume_number-6), word-7)
        volumes_words = preprocess_articles.flatMap(lambda p_article:
                                                    [(p_article[0], p_article[1], p_article[2], p_article[3],
                                                      p_article[4], p_article[5], p_article[6], word)
                                                     for word in query_utils.tokenize(p_article[7])])
        volumes_filtered_words = volumes_words.filter(lambda word: len(word[7]) > 2 and word[7] not in exclude_words)
        volumes_words_freq = volumes_filtered_words.map(lambda word: ((word[0], word[1], word[2], word[3], word[4],
                                                                       word[5], word[6], word[7]), 1)) \
            .reduceByKey(lambda x, y: x + y).sortBy(lambda x: x[1], ascending=False)
        volumes_words_freq_grouped = volumes_words_freq.map(
            lambda words_freq: ((words_freq[0][0], words_freq[0][1], words_freq[0][2], words_freq[0][3],
                                 words_freq[0][4], words_freq[0][5], words_freq[0][6]),
                                (words_freq[0][7], words_freq[1]))).groupByKey().mapValues(list)
        # ((year-0, edition_title-1, edition_uri-2, edition_number-3, volume_uri-4, volume_title-5, volume_number-6), word-freq)
        volumes_words_freq_group_by_year = volumes_words_freq_grouped.map(lambda words_freq:
                                                                (words_freq[0][0],
                                                                 (words_freq[0][1], words_freq[0][2], words_freq[0][3],
                                                                  words_freq[0][4], words_freq[0][5], words_freq[0][6],
                                                                  words_freq[1][:TOP_N]))
                                                                ) \
            .groupByKey() \
            .map(lambda year_es_words_freq: (year_es_words_freq[0], list(year_es_words_freq[1])))
        result = volumes_words_freq_group_by_year.collect()
    elif level == "edition" or level == "series":
        # (year-0, edition_title-1, edition_uri-2, edition_number-3, word-4)
        es_words = preprocess_articles.flatMap(
            lambda p_article: [(p_article[0], p_article[1], p_article[2], p_article[3], word)
                               for word in query_utils.tokenize(p_article[7])])
        es_filtered_words = es_words.filter(lambda word: len(word[4]) > 2 and word[4] not in exclude_words)
        es_words_freq = es_filtered_words.map(lambda word: ((word[0], word[1], word[2], word[3], word[4]), 1)) \
            .reduceByKey(lambda x, y: x + y).sortBy(lambda x: x[1], ascending=False)
        es_words_freq_grouped = es_words_freq.map(
            lambda words_freq: ((words_freq[0][0], words_freq[0][1], words_freq[0][2], words_freq[0][3]),
                                (words_freq[0][4], words_freq[1]))).groupByKey().mapValues(list)
        # ((year-0, edition_title-1, edition_uri-2, edition_number-3), word-freq)
        es_words_freq_group_by_year = es_words_freq_grouped.map(lambda words_freq:
                                                                (words_freq[0][0], (words_freq[0][1], words_freq[0][2],
                                                                                    words_freq[0][3], words_freq[1][:TOP_N]))
                                                                ) \
            .groupByKey() \
            .map(lambda year_es_words_freq: (year_es_words_freq[0], list(year_es_words_freq[1])))
        result = es_words_freq_group_by_year.collect()

    elif level == "collection":
        words = preprocess_articles.flatMap(lambda p_article: query_utils.tokenize(p_article[7]))
        filtered_words = words.filter(lambda word: len(word) > 2 and word not in exclude_words)
        words_freq = filtered_words.map(lambda word: (word, 1)).reduceByKey(lambda x, y: x + y) \
            .sortBy(lambda x: x[1], ascending=False)
        result = {"words_freq": words_freq.take(TOP_N)}

    elif level == "year":
        # (year-0, word-1)
        year_words = preprocess_articles.flatMap(lambda p_article: [(p_article[0], word)
                                                                    for word in query_utils.tokenize(p_article[7])])
        year_filtered_words = year_words.filter(lambda word: len(word[1]) > 2 and word[1] not in exclude_words)
        year_words_freq = year_filtered_words.map(lambda word: ((word[0], word[1]), 1)) \
            .reduceByKey(lambda x, y: x + y).sortBy(lambda x: x[1], ascending=False)
        year_words_freq_grouped = year_words_freq.map(
            lambda words_freq: ((words_freq[0][0]),
                                (words_freq[0][1], words_freq[1]))).groupByKey().mapValues(lambda words_freq: list(words_freq)[:TOP_N])
        result = year_words_freq_grouped.collect()

    return result
