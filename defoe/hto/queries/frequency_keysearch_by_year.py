"""
Select the EB articles using a keysentences or keywords list and groups by date.
Use this query ONLY for searching in the EB articles stored in the Knowledge Graph previously.
"""

from operator import add

from pyspark.sql.functions import concat_ws, col

from defoe import query_utils
from defoe.hto.sparql_service import NLSCollection
from defoe.hto.query_utils import get_articles_list_matches, blank_as_null, get_articles_text_matches
from defoe.nls.query_utils import preprocess_clean_page
from functools import reduce


def do_query(df, config=None, logger=None, context=None):
    """
    IMPORTANT: SAME AS "keysearch_by_year_term_count.py" in NLS!!

    Data in sparql have the following colums:
    
    config_file must be the path to a lexicon file with a list of the keywords 
    to search for, one per line.
    
    Also the config_file can indicate the preprocess treatment, along with the defoe
    path, and the type of operating system. 

      Returns result of form:
        {
          <YEAR>:
          [
            [ -<SENTENCE|WORD>, NUM_SENTENCES|NUM_WORDS 
             - <SENTENCE|WORD>, NUM_SENTENCES|NUM_WORDS 
             - <SENTENCE|WORD>, NUM_SENTENCES|NUM_WORDS 
            ], 
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
    # by default, preprocess type will be lemmatisation
    preprocess_type = query_utils.extract_preprocess_word_type(config)
    data_file = config.get("data", None)

    if "start_year" in config:
        start_year = int(config["start_year"])
    else:
        start_year = None

    if "end_year" in config:
        end_year = int(config["end_year"])
    else:
        end_year = None

    if "target_sentences" in config:
        target_sentences = config["target_sentences"]
    else:
        target_sentences = None

    if "target_filter" in config:
        target_filter = config["target_filter"]
    else:
        target_filter = "or"

    if "hit_count" in config:
        hit_count = config["hit_count"]
    else:
        hit_count = "term"

    collection = NLSCollection.EB.value
    if "collection" in config:
        collection = config["collection"]

    ###### Supporting New NLS KG #######
    if collection == NLSCollection.EB.value:
        fdf = df.withColumn("description",
                            concat_ws(" ", col("term_name"), col("note"),
                                      concat_ws(" ", col("alter_names"))
                                      , col("description")))
    else:
        fdf = df.withColumn("description", blank_as_null("description"))

    if start_year and end_year:
        newdf = fdf.filter(fdf.description.isNotNull()).filter(fdf.year >= start_year).filter(
            fdf.year <= end_year).select(fdf.year, fdf.description)
    elif start_year:
        newdf = fdf.filter(fdf.description.isNotNull()).filter(fdf.year >= start_year).select(fdf.year,
                                                                                             fdf.description)
    elif end_year:
        newdf = fdf.filter(fdf.description.isNotNull()).filter(fdf.year <= end_year).select(fdf.year,
                                                                                           fdf.description)
    else:
        newdf = fdf.filter(fdf.description.isNotNull()).select(fdf.year, fdf.description)

    articles = newdf.rdd.map(tuple)

    preprocess_articles = articles.flatMap(
        lambda t_articles: [(t_articles[0], preprocess_clean_page(t_articles[1], preprocess_type))])

    keysentences = []
    if data_file:
        if isinstance(data_file, str):
            # local file
            data_stream = open(data_file, 'r')
        else:
            # cloud file
            data_stream = data_file.open('r')

        with data_stream as f:
            for keysentence in list(f):
                k_split = keysentence.split()
                # apply the same preprocess the lexicons
                sentence_word = [query_utils.preprocess_word(
                    word, preprocess_type) for word in k_split]
                sentence_norm = ''
                for word in sentence_word:
                    if sentence_norm == '':
                        sentence_norm = word
                    else:
                        sentence_norm += " " + word
                keysentences.append(sentence_norm)

    if target_sentences:
        clean_target_sentences = []
        for target_s in list(target_sentences):
            t_split = target_s.split()
            sentence_word = [query_utils.preprocess_word(
                word, preprocess_type) for word in t_split]
            sentence_norm = ''
            for word in sentence_word:
                if sentence_norm == '':
                    sentence_norm = word
                else:
                    sentence_norm += " " + word
            clean_target_sentences.append(sentence_norm)
        if target_filter == "or":
            target_articles = preprocess_articles.filter(
                lambda year_page: any(target_s in year_page[1] for target_s in clean_target_sentences))
        else:
            target_articles = preprocess_articles
            target_articles = reduce(lambda r, target_s: r.filter(lambda year_page: target_s in year_page[1]),
                                     clean_target_sentences, target_articles)
    else:
        target_articles = preprocess_articles

    if len(keysentences) > 0:
        filter_articles = target_articles.filter(
            lambda year_page: any( keysentence in year_page[1] for keysentence in keysentences))
    else:
        filter_articles = target_articles

    if hit_count == "term" or hit_count == "page":
        matching_articles = filter_articles.map(
            lambda year_article: (year_article[0],
                                  get_articles_list_matches(year_article[1], keysentences)))
    else:
        matching_articles = filter_articles.map(
            lambda year_article: (year_article[0],
                                  get_articles_text_matches(year_article[1], keysentences)))

    # (year-0, sentence-1)
    matching_sentences = matching_articles.flatMap(
        lambda year_sentence: [((year_sentence[0], sentence), 1) for sentence in year_sentence[1]])

    result = matching_sentences \
        .reduceByKey(add) \
        .map(lambda yearsentence_count:
             (yearsentence_count[0][0],
              (yearsentence_count[0][1], yearsentence_count[1]))) \
        .groupByKey() \
        .map(lambda year_sentencecount:
             (year_sentencecount[0], list(year_sentencecount[1]))) \
        .collect()
    return result
