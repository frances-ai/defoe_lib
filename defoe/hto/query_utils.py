"""
Query-related utility functions.
"""
import re

from pyspark.sql.functions import col, when, size, split, expr


def get_sentences_list_matches(text, keysentence):
    """
    Check which key-sentences from occurs within a string
    and return the list of matches.

    :param text: text
    :type text: str or unicode
    :type: list(str or uniocde)
    :return: Set of sentences
    :rtype: set(str or unicode)
    """
    match = []
    text_list = text.split()
    for sentence in keysentence:
        if len(sentence.split()) > 1:
            if sentence in text:
                count = text.count(sentence)
                for i in range(0, count):
                    match.append(sentence)
        else:
            pattern = re.compile(r'^%s$' % sentence)
            for word in text_list:
                if re.search(pattern, word):
                    match.append(sentence)
    return sorted(match)


def get_articles_list_matches(text, keysentences):
    """
    o	Article count: The query counts as a “hint” every time that finds an article with a particular term from our lexicon in the previously selected articles (by the target words or/and time period).  So, if a term is repeated several times in an article, it will be counted just as ONE. In this way, we are basically calculating the “frequency of articles” over time.

    Check which key-sentences from occurs within a string
    and return the list of matches.

    :param text: text
    :type text: str or unicode
    :type: list(str or uniocde)
    :return: Set of sentences
    :rtype: set(str or unicode)
    """

    match = []
    text_list = text.split()
    for sentence in keysentences:
        if len(sentence.split()) > 1:
            if sentence in text:
                match.append(sentence)

        else:
            pattern = re.compile(r'^%s$' % sentence)
            for word in text_list:
                if re.search(pattern, word) and (sentence not in match):
                    match.append(sentence)
    return sorted(match)


def get_articles_text_matches(text, keysentences):
    """
     TERM count: The query counts as a “hint” every time that finds a hit with a particular term from our lexicon in the previously selected articles (by the target words or/and time period).  So, if a term is repeated several times in an article, it will be counted SEVERAL TIMES

    Check which key-sentences from occurs within a string
    and return the list of matches.

    :param text: text
    :type text: str or unicode
    :type: list(str or uniocde)
    :return: Set of sentences
    :rtype: set(str or unicode)
    """
    match = []
    text_list = text.split()
    for sentence in keysentences:
        if len(sentence.split()) > 1:
            if sentence in text:
                results = [matches.start() for matches in re.finditer(sentence, text)]
                for r in results:
                    match.append(sentence)
        else:
            pattern = re.compile(r'^%s$' % sentence)
            for word in text_list:
                if re.search(pattern, word):
                    match.append(sentence)
    return sorted(match)


def word_count_for_string_column(column):
    return when(
        col(column).isNotNull() & (col(column) != ""),
        size(split(col(column), " "))
    ).otherwise(0)


def word_count_for_list_column(column):
    return when(
        col(column).isNotNull(),
        expr("aggregate(alter_names, 0, (acc, x) -> acc + size(split(x, ' ')))")
    ).otherwise(0)


def blank_as_null(x):
    return when(col(x) != "", col(x)).otherwise(None)
