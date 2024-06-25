"""
Gets the snippet of each term (from a list of keywords or keysentences) along with the metadata.
We recommend to use this query when we want to select a window of words (snippet lenght) around each term, instead of selecting
all the words of the page in which the term was found.
"""
from pyspark.sql.functions import concat_ws, col

from defoe import query_utils, get_root_path, get_geo_supported_os_type
from defoe.hto.query_utils import blank_as_null
from defoe.hto.sparql_service import NLSCollection

from defoe.nls.query_utils import preprocess_clean_page

import yaml, os
from functools import reduce


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

    collection = NLSCollection.EB.value
    if "collection" in config:
        collection = config["collection"]

    if "gazetteer" in config:
        gazetteer = config["gazetteer"]
    else:
        gazetteer = "geonames"

    if "bounding_box" in config:
        bounding_box = " -lb " + config["bounding_box"] + " 2"
    else:
        bounding_box = ""

    defoe_path = get_root_path() + "/"
    print(defoe_path)
    os_type = get_geo_supported_os_type()

    if collection == NLSCollection.EB.value:
        fdf = df.withColumn("description",
                            concat_ws(" ", col("term_name"), col("note"),
                                      concat_ws(" ", col("alter_names"))
                                      , col("description")))
        if start_year and end_year:
            newdf = fdf.filter(fdf.description.isNotNull()).filter(fdf.year >= start_year).filter(
                fdf.year <= end_year).select(fdf.year, fdf.term_uri, fdf.description, fdf.term_name, fdf.edition_title,
                                             fdf.edition_uri,
                                             fdf.edition_number, fdf.source_file_uri, fdf.source_file_path,
                                             fdf.volume_uri, fdf.volume_number, fdf.start_page_uri,
                                             fdf.start_page_number)
        elif start_year:
            newdf = fdf.filter(fdf.description.isNotNull()).filter(fdf.year >= start_year).select(fdf.year,
                                                                                                  fdf.term_uri,
                                                                                                  fdf.description,
                                                                                                  fdf.term_name,
                                                                                                  fdf.edition_title,
                                                                                                  fdf.edition_uri,
                                                                                                  fdf.edition_number,
                                                                                                  fdf.source_file_uri,
                                                                                                  fdf.source_file_path,
                                                                                                  fdf.volume_uri,
                                                                                                  fdf.volume_number,
                                                                                                  fdf.start_page_uri,
                                                                                                  fdf.start_page_number)
        elif end_year:
            newdf = fdf.filter(fdf.description.isNotNull()).filter(fdf.year <= end_year).select(fdf.year, fdf.term_uri,
                                                                                                fdf.description,
                                                                                                fdf.term_name,
                                                                                                fdf.edition_title,
                                                                                                fdf.edition_uri,
                                                                                                fdf.edition_number,
                                                                                                fdf.source_file_uri,
                                                                                                fdf.source_file_path,
                                                                                                fdf.volume_uri,
                                                                                                fdf.volume_number,
                                                                                                fdf.start_page_uri,
                                                                                                fdf.start_page_number)
        else:
            newdf = fdf.filter(fdf.description.isNotNull()).select(fdf.year, fdf.term_uri, fdf.description,
                                                                   fdf.term_name, fdf.edition_title, fdf.edition_uri,
                                                                   fdf.edition_number, fdf.source_file_uri,
                                                                   fdf.source_file_path,
                                                                   fdf.volume_uri, fdf.volume_number,
                                                                   fdf.start_page_uri,
                                                                   fdf.start_page_number)
    else:
        fdf = df.withColumn("description", blank_as_null("description"))
        if start_year and end_year:
            newdf = fdf.filter(fdf.description.isNotNull()).filter(fdf.year >= start_year).filter(
                fdf.year <= end_year).select(fdf.year, fdf.page_uri, fdf.description, fdf.page_number, fdf.series_title,
                                             fdf.series_uri,
                                             fdf.series_number, fdf.source_file_uri, fdf.source_file_path,
                                             fdf.volume_uri, fdf.volume_title, fdf.volume_number)
        elif start_year:
            newdf = fdf.filter(fdf.description.isNotNull()).filter(fdf.year >= start_year).select(fdf.year,
                                                                                                  fdf.page_uri,
                                                                                                  fdf.description,
                                                                                                  fdf.page_number,
                                                                                                  fdf.series_title,
                                                                                                  fdf.series_uri,
                                                                                                  fdf.series_number,
                                                                                                  fdf.source_file_uri,
                                                                                                  fdf.source_file_path,
                                                                                                  fdf.volume_uri,
                                                                                                  fdf.volume_title,
                                                                                                  fdf.volume_number)
        elif end_year:
            newdf = fdf.filter(fdf.description.isNotNull()).filter(fdf.year <= end_year).select(fdf.year, fdf.page_uri,
                                                                                                fdf.description,
                                                                                                fdf.page_number,
                                                                                                fdf.series_title,
                                                                                                fdf.series_uri,
                                                                                                fdf.series_number,
                                                                                                fdf.source_file_uri,
                                                                                                fdf.source_file_path,
                                                                                                fdf.volume_uri,
                                                                                                fdf.volume_title,
                                                                                                fdf.volume_number)
        else:
            newdf = fdf.filter(fdf.description.isNotNull()).select(fdf.year, fdf.page_uri, fdf.description,
                                                                   fdf.page_number, fdf.series_title, fdf.series_uri,
                                                                   fdf.series_number, fdf.source_file_uri,
                                                                   fdf.source_file_path,
                                                                   fdf.volume_uri, fdf.volume_title, fdf.volume_number)

    articles = newdf.rdd.map(tuple)

    if collection == NLSCollection.EB.value:
        # (year-0, term_uri-1, description-2, term_name-3, edition_title-4, edition_uri-5, edition_number-6, source_file_uri-7, source_file_path-8, volume_uri-9, volume_number-10, start_page_uri-11, start_page_number-12)
        preprocess_articles = articles.flatMap(
            lambda t_articles: [
                (t_articles[0], t_articles[1], preprocess_clean_page(t_articles[2], preprocess_type), t_articles[2],
                 t_articles[3], t_articles[4], t_articles[5],
                 t_articles[6], t_articles[7], t_articles[8], t_articles[9], t_articles[10],
                 t_articles[11], t_articles[12])])
    else:
        # (year-0, page_uri-1, description-2, page_number-3, series_title-4, series_uri-5, series_number-6, source_file_uri-7, source_file_path-8, volume_uri-9, volume_title-10, volume_number-11)
        preprocess_articles = articles.flatMap(
            lambda t_articles: [
                (t_articles[0], t_articles[1], preprocess_clean_page(t_articles[2], preprocess_type), t_articles[2],
                 t_articles[3], t_articles[4], t_articles[5],
                 t_articles[6], t_articles[7], t_articles[8], t_articles[9], t_articles[10],
                 t_articles[11])])

    if data_file:
        keysentences = []
        if isinstance(data_file, str):
            # local file
            data_stream = open(data_file, 'r')
        else:
            # cloud file
            data_stream = data_file.open('r')
        with data_stream as f:
            for keysentence in list(f):
                k_split = keysentence.split()
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
                lambda year_page: any(target_s in year_page[2] for target_s in clean_target_sentences))
        else:
            target_articles = preprocess_articles
            target_articles = reduce(lambda r, target_s: r.filter(lambda year_page: target_s in year_page[2]),
                                     clean_target_sentences, target_articles)
    else:
        target_articles = preprocess_articles

    if data_file:
        filter_articles = target_articles.filter(
            lambda year_page: any(keysentence in year_page[2] for keysentence in keysentences))

    else:
        filter_articles = target_articles
        #keysentences = clean_target_sentences

    # (year-0, term_uri-1, preprocessed-description-2, description-3 term_name-4, edition_title-5, edition_uri-6, edition_number-7, source_file_uri-8, source_file_path-9, volume_uri-10, volume_number-11, start_page_uri-12, start_page_number-13)
    if collection == NLSCollection.EB.value:
        geo_xml = filter_articles.flatMap(
            lambda year_page: [
                (year_page[0],
                 year_page[1],
                 query_utils.get_geoparser_xml(year_page[3], defoe_path, os_type, gazetteer, bounding_box),
                 year_page[4],
                 year_page[5],
                 year_page[6],
                 year_page[7],
                 year_page[8],
                 year_page[9],
                 year_page[10],
                 year_page[11],
                 year_page[12],
                 year_page[13])])
        geo_data = geo_xml.map(
            lambda sentence_data:
            (sentence_data[0],
             {"term_uri": sentence_data[1],
              "term_name": sentence_data[3],
              "edition_title": sentence_data[4],
              "edition_uri": sentence_data[5],
              "edition_number": sentence_data[6],
              "source_file_uri": sentence_data[7],
              "source_file_path": sentence_data[8],
              "volume_uri": sentence_data[9],
              "volume_number": sentence_data[10],
              "start_page_uri": sentence_data[11],
              "start_page_number": sentence_data[12],
              "georesolution": query_utils.geoparser_coord_xml(sentence_data[2])}))
    else:
        # (year-0, page_uri-1, preprocessed-description-2, description-3, page_number-4, series_title-5, series_uri-6, series_number-7, source_file_uri-8, source_file_path-9, volume_uri-10, volume_title-11, volume_number-12)
        geo_xml = filter_articles.flatMap(
            lambda year_page: [
                (year_page[0],
                 year_page[1],
                 query_utils.get_geoparser_xml(year_page[3], defoe_path, os_type, gazetteer, bounding_box),
                 year_page[4],
                 year_page[5],
                 year_page[6],
                 year_page[7],
                 year_page[8],
                 year_page[9],
                 year_page[10],
                 year_page[11],
                 year_page[12])])
        geo_data = geo_xml.map(
            lambda sentence_data:
            (sentence_data[0],
             {"page_uri": sentence_data[1],
              "page_number": sentence_data[3],
              "series_title": sentence_data[4],
              "series_uri": sentence_data[5],
              "series_number": sentence_data[6],
              "source_file_uri": sentence_data[7],
              "source_file_path": sentence_data[8],
              "volume_uri": sentence_data[9],
              "volume_title": sentence_data[10],
              "volume_number": sentence_data[11],
              "georesolution": query_utils.geoparser_coord_xml(sentence_data[2])}))

    # remove data with no places identified
    geo_data = geo_data.filter(lambda sentence_data: bool(sentence_data[1]["georesolution"]) and isinstance(sentence_data[1]["georesolution"], dict))

    result = geo_data \
        .groupByKey() \
        .map(lambda date_context:
             (date_context[0], list(date_context[1]))) \
        .collect()
    return result
