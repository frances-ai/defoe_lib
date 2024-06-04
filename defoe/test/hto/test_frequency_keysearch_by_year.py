"""
defoe.hto.queries.frequency_keysearch_by_year tests.
"""

from .pyspark_testcase import PySparkTestCase

from defoe.hto.queries import frequency_keysearch_by_year


class TestFrequencyKeysearchByYear(PySparkTestCase):
    def test_chapbooks_page_hit(self):
        sample_data = [{'page_uri': 'https://w3id.org/hto/Page/test_page_1',
                        'page_number': 1,
                        "year": '1842',
                        # word count: 12
                        'description': 'test under, and y Xuirra, tongue, in Anatomy, two glands of the tongue. test',
                        'volume_uri': 'https://w3id.org/hto/Volume/7_12',
                        'volume_number': 12,
                        'num_pages': 902, },
                       {'page_uri': 'https://w3id.org/hto/Page/test_page_2',
                        'page_number': 2,
                        "year": '1842',
                        # word count: 7
                        'description': "in Ancient Geography, a district of Petraea. note",
                        'volume_uri': 'https://w3id.org/hto/Volume/7_8',
                        'volume_number': 8,
                        'num_pages': 974},
                       {'page_uri': 'https://w3id.org/hto/Page/test_page_3',
                        'page_number': 3,
                        "year": '1771',
                        # word count: 7
                        'description': "in Ancient Geography, a district of Petraea.",
                        'volume_uri': 'https://w3id.org/hto/Volume/1_3',
                        'volume_number': 3,
                        'num_pages': 974},
                       {'page_uri': 'https://w3id.org/hto/Page/test_page_4',
                        'page_number': 4,
                        "year": '1771',
                        # word count: 7
                        'description': "in Ancient Geography, a district of Petraea. note",
                        'volume_uri': 'https://w3id.org/hto/Volume/1_3',
                        'volume_number': 3,
                        'num_pages': 974}]
        config = {
            "collection": "Chapbooks printed in Scotland",
            "hit_count": "term",
            "data": "/Users/ly40/Documents/frances-ai/defoe_lib/defoe/test/hto/test_lexicon.txt"
        }
        # Create a Spark DataFrame
        original_df = self.spark.createDataFrame(sample_data)
        context = self.spark.sparkContext
        logger = context._jvm.org.apache.log4j.LogManager.getLogger(__name__)
        result = frequency_keysearch_by_year.do_query(original_df, config, logger, context)
        expect_result = [('1771', [('note', 1)]),
                         ('1842', [('note', 1), ('test', 1)])]
        self.assertEqual(result, expect_result)

    def test_chapbooks_word_hit(self):
        sample_data = [{'page_uri': 'https://w3id.org/hto/Page/test_page_1',
                        'page_number': 1,
                        "year": '1842',
                        # word count: 12
                        'description': 'test under, and y Xuirra, tongue, in Anatomy, two glands of the tongue. test',
                        'volume_uri': 'https://w3id.org/hto/Volume/7_12',
                        'volume_number': 12,
                        'num_pages': 902, },
                       {'page_uri': 'https://w3id.org/hto/Page/test_page_2',
                        'page_number': 2,
                        "year": '1842',
                        # word count: 7
                        'description': "in Ancient Geography, a district of Petraea. note",
                        'volume_uri': 'https://w3id.org/hto/Volume/7_8',
                        'volume_number': 8,
                        'num_pages': 974},
                       {'page_uri': 'https://w3id.org/hto/Page/test_page_3',
                        'page_number': 3,
                        "year": '1771',
                        # word count: 7
                        'description': "in Ancient Geography, a district of Petraea.",
                        'volume_uri': 'https://w3id.org/hto/Volume/1_3',
                        'volume_number': 3,
                        'num_pages': 974},
                       {'page_uri': 'https://w3id.org/hto/Page/test_page_4',
                        'page_number': 4,
                        "year": '1771',
                        # word count: 7
                        'description': "in Ancient Geography, a district of Petraea. note",
                        'volume_uri': 'https://w3id.org/hto/Volume/1_3',
                        'volume_number': 3,
                        'num_pages': 974}]
        config = {
            "collection": "Chapbooks printed in Scotland",
            "hit_count": "word",
            "data": "/Users/ly40/Documents/frances-ai/defoe_lib/defoe/test/hto/test_lexicon.txt"
        }
        # Create a Spark DataFrame
        original_df = self.spark.createDataFrame(sample_data)
        context = self.spark.sparkContext
        logger = context._jvm.org.apache.log4j.LogManager.getLogger(__name__)
        result = frequency_keysearch_by_year.do_query(original_df, config, logger, context)
        expect_result = [('1771', [('note', 1)]),
                         ('1842', [('note', 1), ('test', 2)])]
        self.assertEqual(result, expect_result)

    def test_eb_terms_term_hit(self):
        sample_data = [{'term_uri': 'https://w3id.org/hto/ArticleTermRecord/test_term_1',
                        'term_name': 'HYPOGLOTTIS',
                        'alter_names': ['HYPOGLOSSIS'],
                        'note': "",
                        "year": '1842',
                        # word count: 14
                        'description': 'under, and y Xuirra, tongue, in Anatomy, two glands of the tongue.',
                        'volume_uri': 'https://w3id.org/hto/Volume/7_12',
                        'volume_number': 12,
                        'num_pages': 902},
                       {'term_uri': 'https://w3id.org/hto/ArticleTermRecord/test_term_2',
                        'term_name': 'EDOM',
                        'alter_names': ['IDUMAEA'],
                        'note': "test note",
                        "year": '1842',
                        # word count: 11
                        'description': "in Ancient Geography, a district of Petraea. note",
                        'volume_uri': 'https://w3id.org/hto/Volume/7_8',
                        'volume_number': 8,
                        'num_pages': 974, },
                       {'term_uri': 'https://w3id.org/hto/ArticleTermRecord/test_term_3',
                        'term_name': 'EDOM',
                        'alter_names': ['IDUMAEA'],
                        'note': "test note",
                        "year": '1771',
                        # word count: 11
                        'description': "in Ancient Geography, a district of Petraea. test",
                        'volume_uri': 'https://w3id.org/hto/Volume/1_3',
                        'volume_number': 3,
                        'num_pages': 974, },
                       {'term_uri': 'https://w3id.org/hto/ArticleTermRecord/test_term_4',
                        'term_name': 'EDOM_2',
                        'alter_names': ['IDUMAEA'],
                        'note': "test note",
                        "year": '1771',
                        # word count: 11
                        'description': "in Ancient Geography, a district of Petraea.",
                        'volume_uri': 'https://w3id.org/hto/Volume/1_3',
                        'volume_number': 3,
                        'num_pages': 974, }
                       ]
        config = {
            "collection": "Encyclopaedia Britannica",
            "hit_count": "term",
            "data": "/Users/ly40/Documents/frances-ai/defoe_lib/defoe/test/hto/test_lexicon.txt"
        }
        # Create a Spark DataFrame
        original_df = self.spark.createDataFrame(sample_data)
        context = self.spark.sparkContext
        logger = context._jvm.org.apache.log4j.LogManager.getLogger(__name__)
        result = frequency_keysearch_by_year.do_query(original_df, config, logger, context)
        expect_result = [('1771', [('test', 2), ('note', 2)]), ('1842', [('hypoglottis', 1), ('note', 1), ('test', 1)])]
        self.assertEqual(result, expect_result)

    def test_eb_terms_word_hit(self):
        sample_data = [{'term_uri': 'https://w3id.org/hto/ArticleTermRecord/test_term_1',
                        'term_name': 'HYPOGLOTTIS',
                        'alter_names': ['HYPOGLOSSIS'],
                        'note': "",
                        "year": '1842',
                        # word count: 14
                        'description': 'under, and y Xuirra, tongue, in Anatomy, two glands of the tongue.',
                        'volume_uri': 'https://w3id.org/hto/Volume/7_12',
                        'volume_number': 12,
                        'num_pages': 902},
                       {'term_uri': 'https://w3id.org/hto/ArticleTermRecord/test_term_2',
                        'term_name': 'EDOM',
                        'alter_names': ['IDUMAEA'],
                        'note': "test note",
                        "year": '1842',
                        # word count: 11
                        'description': "in Ancient Geography, a district of Petraea. note",
                        'volume_uri': 'https://w3id.org/hto/Volume/7_8',
                        'volume_number': 8,
                        'num_pages': 974, },
                       {'term_uri': 'https://w3id.org/hto/ArticleTermRecord/test_term_3',
                        'term_name': 'EDOM',
                        'alter_names': ['IDUMAEA'],
                        'note': "test note",
                        "year": '1771',
                        # word count: 11
                        'description': "in Ancient Geography, a district of Petraea. test",
                        'volume_uri': 'https://w3id.org/hto/Volume/1_3',
                        'volume_number': 3,
                        'num_pages': 974, },
                       {'term_uri': 'https://w3id.org/hto/ArticleTermRecord/test_term_4',
                        'term_name': 'EDOM_2',
                        'alter_names': ['IDUMAEA'],
                        'note': "test note",
                        "year": '1771',
                        # word count: 11
                        'description': "in Ancient Geography, a district of Petraea.",
                        'volume_uri': 'https://w3id.org/hto/Volume/1_3',
                        'volume_number': 3,
                        'num_pages': 974, }
                       ]
        config = {
            "collection": "Encyclopaedia Britannica",
            "hit_count": "word",
            "data": "/Users/ly40/Documents/frances-ai/defoe_lib/defoe/test/hto/test_lexicon.txt"
        }
        # Create a Spark DataFrame
        original_df = self.spark.createDataFrame(sample_data)
        context = self.spark.sparkContext
        logger = context._jvm.org.apache.log4j.LogManager.getLogger(__name__)
        result = frequency_keysearch_by_year.do_query(original_df, config, logger, context)
        expect_result = [('1771', [('test', 3), ('note', 2)]),
                         ('1842', [('hypoglottis', 1), ('note', 2), ('test', 1)])]
        self.assertEqual(result, expect_result)
