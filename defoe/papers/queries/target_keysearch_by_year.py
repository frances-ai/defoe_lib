"""
Count number of articles in which appear keywords or keysentences.
Filter those by target words, and group results by year
This query is the recommended to use when there are target words. 
"""

from operator import add

from defoe import query_utils
from defoe.papers.query_utils import preprocess_clean_article, clean_article_as_string
from defoe.papers.query_utils import get_sentences_list_matches,  get_articles_list_matches

import yaml, os

def do_query(issues, config_file=None, logger=None, context=None):
    """
    Counts number of occurrences of keywords or keysentences and groups by year.
    
    Furthermore, this query allow us to specify the target words, to select all articles 
    which have at least one of the target words.  

    config_file must be the path to a lexicon file with a list
    of the keywords to search for, one per line.
    
    Also the config_file can indicate the preprocess treatment, along with the defoe
    path, and the type of operating system. We can also configure how many target words 
    we want to use, and in which position the lexicon words starts. 
    
    For indicating the number of target words to use from the lexicon file, we can indicate it 
    in the configuration file as, num_target: 1. That means, that we have only one word/sentence
    as the target word (the first one). 
    
    If we want to include the target words in the lexicon, we should indicate in 
    the configuration file as, lexicon_start: 0.
    
    If we do not want to include the target words (lets image that we have just one target word) 
    in the lexicon, we should indicate in the configuration file as, lexicon_start: 1.
    


    Returns result of form:

        {
          <YEAR>:
          [
            [<SENTENCE|WORD>, <NUM_SENTENCES|WORDS>],
            ...
          ],
          <YEAR>:
          ...
        }

    :param issues: RDD of defoe.papers.issue.Issue
    :type archives: pyspark.rdd.PipelinedRDD
    :param config_file: query configuration file
    :type config_file: str or unicode
    :param logger: logger (unused)
    :type logger: py4j.java_gateway.JavaObject
    :return: number of occurrences of keywords grouped by year
    :rtype: dict
    """
    with open(config_file, "r") as f:
        config = yaml.load(f)
    if "os_type" in config:
        if config["os_type"] == "linux":
            os_type = "sys-i386-64"
        else:
            os_type= "sys-i386-snow-leopard"
    else:
            os_type = "sys-i386-64"
    if "defoe_path" in config :
        defoe_path= config["defoe_path"]
    else:
        defoe_path = "./"

    preprocess_type = query_utils.extract_preprocess_word_type(config)
    data_file = query_utils.extract_data_file(config,
                                              os.path.dirname(config_file))
    num_target=int(config["num_target"])
    lexicon_start= int(config["lexicon_start"])
    keysentences = []
    with open(data_file, 'r') as f:
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
    # [(year, article_string), ...]
    
    target_sentences = keysentences[0:num_target]
    keysentences = keysentences[lexicon_start:]
    clean_articles = issues.flatMap(
        lambda issue: [(issue.date.year, clean_article_as_string(
            article, defoe_path, os_type)) for article in issue.articles])


    # [(year, preprocess_article_string), ...]
    t_articles = clean_articles.flatMap(
        lambda cl_article: [(cl_article[0], 
                                    preprocess_clean_article(cl_article[1], preprocess_type))]) 

    # [(year, clean_article_string)
    target_articles = t_articles.filter(
        lambda year_article: any(
            target_s in year_article[1] for target_s in target_sentences))

    # [(year, clean_article_string)
    filter_articles = target_articles.filter(
        lambda year_article: any(
            keysentence in year_article[1] for keysentence in keysentences))
    
    # [(year, [keysentence, keysentence]), ...]
    # Note: get_articles_list_matches ---> articles count
    # Note: get_sentences_list_matches ---> word_count 
    matching_articles = filter_articles.map(
        lambda year_article: (year_article[0],
                               get_articles_list_matches(
                                  year_article[1],
                                  keysentences)))

    # [[(year, keysentence), 1) ((year, keysentence), 1) ] ...]
    matching_sentences = matching_articles.flatMap(
        lambda year_sentence: [((year_sentence[0], sentence), 1)
                               for sentence in year_sentence[1]])


    # [((year, keysentence), num_keysentences), ...]
    # =>
    # [(year, (keysentence, num_keysentences)), ...]
    # =>
    # [(year, [keysentence, num_keysentences]), ...]
    result = matching_sentences\
        .reduceByKey(add)\
        .map(lambda yearsentence_count:
             (yearsentence_count[0][0],
              (yearsentence_count[0][1], yearsentence_count[1]))) \
        .groupByKey() \
        .map(lambda year_sentencecount:
             (year_sentencecount[0], list(year_sentencecount[1]))) \
        .collect()
    return result
