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
import gender_guesser.detector as gender

gender_detector = gender.Detector()

title_gender_map = {
    "mrs": "female",
    "miss": "female",
    "ms": "female",
    "sister": "female",
    "ma'am": "female",
    "lady": "female",
    "queen": "female",
    "madame": "female",
    "mademoiselle": "female",
    "duchess": "female",
    "sir": "male",
    "lord": "male",
    "master": "male",
    "king": "male",
    "duke": "male",
    "monsieur": "male",
    "father": "male",
    "brother": "male"
}

gender_neutral_titles = ["dr", "prof", "governor", "mayor", "professor", "dean", "chancellor"]


def get_gender(person_name):
    name_parts = person_name.split()
    if len(name_parts) > 1:
        name_first_part_lower = name_parts[0].lower()
        if name_first_part_lower in title_gender_map:
            # guess gender based on title
            return title_gender_map[name_first_part_lower]
        elif name_first_part_lower in gender_neutral_titles:
            # guess gender based on the first name after title
            return gender_detector.get_gender(name_parts[1].lower())
        else:
            # guess gender based on the first name
            return gender_detector.get_gender(name_parts[0])
    else:
        return gender_detector.get_gender(person_name)


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
    # EB: year-0, edition_title-1, edition_uri-2, edition_number-3, volume_uri-4, volume_title-5, volume_number-6, description-7,
    # NLS: year-0, series_title-1, series_uri-2, series_number-3, volume_uri-4, volume_title-5, volume_number-6, description-7,

    result = None

    if level == "volume":
        # ((year-0, edition_title-1, edition_uri-2, edition_number-3, volume_uri-4, volume_title-5, volume_number-6), sentence-7)
        volumes_sentences = articles.flatMap(
            lambda article: [
                (article[0], article[1], article[2], article[3], article[4], article[5], article[6], sentence)
                for sentence in query_utils.get_sentences(article[7])])
        volumes_persons = volumes_sentences.flatMap(lambda sentence:
                                                    [(sentence[0], sentence[1], sentence[2], sentence[3], sentence[4],
                                                      sentence[5], sentence[6], person)
                                                     for person in
                                                     query_utils.extract_persons_from_text(sentence[7], exclude_words)])

        volumes_persons_with_gender = volumes_persons.map(lambda person:
                                                (person[0], person[1], person[2], person[3], person[4], person[5],
                                                 person[6], person[7], get_gender(person[7])))
        volumes_persons_freq = volumes_persons_with_gender.map(lambda person_with_gender: (person_with_gender, 1)).reduceByKey(
            lambda x, y: x + y).sortBy(lambda x: x[1], ascending=False)
        volumes_persons_freq_grouped = volumes_persons_freq.map(
            lambda person_freq: ((person_freq[0][0], person_freq[0][1], person_freq[0][2], person_freq[0][3],
                                 person_freq[0][4], person_freq[0][5], person_freq[0][6]),
                                 (person_freq[0][7], person_freq[0][8], person_freq[1]))) \
            .groupByKey().mapValues(list)

        volumes_genders_freq = volumes_persons_with_gender.map(
            lambda person_with_gender: ((person_with_gender[0], person_with_gender[1], person_with_gender[2],
                                         person_with_gender[3], person_with_gender[4], person_with_gender[5],
                                         person_with_gender[6], person_with_gender[8]), 1)) \
            .reduceByKey(
            lambda x, y: x + y) \
            .sortBy(lambda x: x[1], ascending=False)
        volumes_genders_freq_grouped = volumes_genders_freq.map(
            lambda gender_freq: ((gender_freq[0][0], gender_freq[0][1], gender_freq[0][2], gender_freq[0][3],
                                  gender_freq[0][4], gender_freq[0][5], gender_freq[0][6]),
                                 (gender_freq[0][7], gender_freq[1]))) \
            .groupByKey().mapValues(list)
        volumes_result_combined = volumes_persons_freq_grouped.union(volumes_genders_freq_grouped)\
            .reduceByKey(lambda x, y: {"persons_freq": x[:TOP_N], "genders_freq": y})
        volumes_result_combined_grouped_by_year = volumes_result_combined.map(
            lambda vol_freq: (vol_freq[0][0], (vol_freq[0][1], vol_freq[0][2], vol_freq[0][3], vol_freq[0][4],
                                             vol_freq[0][5], vol_freq[0][6], vol_freq[1]))) \
            .groupByKey() \
            .map(lambda year_vol_freq: (year_vol_freq[0], list(year_vol_freq[1])))

        result = volumes_result_combined_grouped_by_year.collect()

    elif level == "edition" or level == "series":
        # (year-0, edition_title-1, edition_uri-2, edition_number-3, sentence-4)
        es_sentences = articles.flatMap(
            lambda article: [(article[0], article[1], article[2], article[3], sentence)
                             for sentence in query_utils.get_sentences(article[7])])
        es_persons = es_sentences.flatMap(lambda sentence:
                                          [(sentence[0], sentence[1], sentence[2], sentence[3], person)
                                           for person in
                                           query_utils.extract_persons_from_text(sentence[4], exclude_words)])

        es_persons_with_gender = es_persons.map(lambda person:
                                                (person[0], person[1], person[2], person[3], person[4],
                                                 get_gender(person[4])))
        es_persons_freq = es_persons_with_gender.map(lambda person_with_gender: (person_with_gender, 1)).reduceByKey(
            lambda x, y: x + y).sortBy(lambda x: x[1], ascending=False)
        es_persons_freq_grouped = es_persons_freq.map(
            lambda person_freq: ((person_freq[0][0], person_freq[0][1], person_freq[0][2], person_freq[0][3]),
                                 (person_freq[0][4], person_freq[0][5], person_freq[1]))) \
            .groupByKey().mapValues(list)

        es_genders_freq = es_persons_with_gender.map(
            lambda person_with_gender: ((person_with_gender[0], person_with_gender[1], person_with_gender[2],
                                         person_with_gender[3], person_with_gender[5]), 1)) \
            .reduceByKey(
            lambda x, y: x + y) \
            .sortBy(lambda x: x[1], ascending=False)
        es_genders_freq_grouped = es_genders_freq.map(
            lambda gender_freq: ((gender_freq[0][0], gender_freq[0][1], gender_freq[0][2], gender_freq[0][3]),
                                 (gender_freq[0][4], gender_freq[1]))) \
            .groupByKey().mapValues(list)
        es_result_combined = es_persons_freq_grouped.union(es_genders_freq_grouped)\
            .reduceByKey(lambda x, y: {"persons_freq": x[:TOP_N], "genders_freq": y})
        es_result_combined_grouped_by_year = es_result_combined.map(
            lambda es_freq: (es_freq[0][0], (es_freq[0][1], es_freq[0][2],
                                             es_freq[0][3], es_freq[1]))) \
            .groupByKey() \
            .map(lambda year_es_freq: (year_es_freq[0], list(year_es_freq[1])))

        result = es_result_combined_grouped_by_year.collect()

    elif level == "collection":
        sentences = articles.flatMap(lambda article: query_utils.get_sentences(article[7]))
        persons = sentences.flatMap(lambda sentence: query_utils.extract_persons_from_text(sentence, exclude_words))
        persons_with_gender = persons.map(lambda person: (person, get_gender(person)))
        persons_freq = persons_with_gender.map(lambda person_with_gender: (person_with_gender, 1)).reduceByKey(
            lambda x, y: x + y) \
            .sortBy(lambda x: x[1], ascending=False).map(lambda person_freq: (person_freq[0][0], person_freq[0][1],
                                                                              person_freq[1]))
        genders_freq = persons_with_gender.map(lambda person_with_gender: (person_with_gender[1], 1)).reduceByKey(
            lambda x, y: x + y) \
            .sortBy(lambda x: x[1], ascending=False)
        result = {
            "persons_freq": persons_freq.take(TOP_N),
            "genders_freq": genders_freq.collect()
        }
    elif level == "year":
        # (year-0, sentences-1)
        year_sentences = articles.flatMap(lambda article: [(article[0], sentence)
                                                           for sentence in query_utils.get_sentences(article[7])])
        year_persons = year_sentences.flatMap(lambda sentence: [(sentence[0], person)
                                                                for person in
                                                                query_utils.extract_persons_from_text(sentence[1], exclude_words)])
        year_persons_with_gender = year_persons.map(lambda person: (person[0], person[1], get_gender(person[1])))
        year_persons_freq = year_persons_with_gender.map(lambda person_with_gender: (person_with_gender, 1)).reduceByKey(
            lambda x, y: x + y) \
            .sortBy(lambda x: x[1], ascending=False).map(lambda person_freq: (person_freq[0][0], (person_freq[0][1],
                                                                              person_freq[0][2], person_freq[1])))
        year_persons_freq_grouped = year_persons_freq.groupByKey()\
            .mapValues(lambda year_persons_freq_list: list(year_persons_freq_list)[:TOP_N])
        year_genders_freq = year_persons_with_gender.map(lambda person_with_gender:
                                                    ((person_with_gender[0], person_with_gender[2]), 1))\
            .reduceByKey(lambda x, y: x + y) \
            .sortBy(lambda x: x[1], ascending=False).map(lambda gender_freq: (gender_freq[0][0],
                                                                              (gender_freq[0][1], gender_freq[1])))
        year_genders_freq_grouped = year_genders_freq.groupByKey().mapValues(list)
        result = year_persons_freq_grouped.union(year_genders_freq_grouped)\
            .reduceByKey(lambda x, y: {"persons_freq": x, "genders_freq": y}).collect()

    return result
