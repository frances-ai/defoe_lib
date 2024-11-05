"""
defoe.hto.queries.publication_normalized tests.
"""

from .pyspark_testcase import PySparkTestCase

from defoe.hto.queries import publication_normalized


class TestPublicationNormalization(PySparkTestCase):

    def test_eb_term_counts(self):
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
                        'description': "in Ancient Geography, a district of Petraea.",
                        'volume_uri': 'https://w3id.org/hto/Volume/7_8',
                        'volume_number': 8,
                        'num_pages': 974, },
                       {'term_uri': 'https://w3id.org/hto/ArticleTermRecord/test_term_3',
                        'term_name': 'EDOM',
                        'alter_names': ['IDUMAEA'],
                        'note': "test note",
                        "year": '1771',
                        # word count: 11
                        'description': "in Ancient Geography, a district of Petraea.",
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
            "collection": "Encyclopaedia Britannica"
        }
        # Create a Spark DataFrame
        original_df = self.spark.createDataFrame(sample_data)
        context = self.spark.sparkContext
        logger = context._jvm.org.apache.log4j.LogManager.getLogger(__name__)
        result = publication_normalized.do_query(original_df, config, logger, context)
        expect_result = {'1771': [1, 974, 2, 22], '1842': [2, 1876, 2, 25]}
        self.assertEqual(result, expect_result)

    def test_nls_pages_counts(self):
        sample_data = [{'page_uri': 'https://w3id.org/hto/ArticleTermRecord/test_page_1',
                        'page_number': 1,
                        "year": '1842',
                        # word count: 12
                        'description': 'under, and y Xuirra, tongue, in Anatomy, two glands of the tongue.',
                        'volume_uri': 'https://w3id.org/hto/Volume/7_12',
                        'volume_number': 12,
                        'num_pages': 902, },
                       {'page_uri': 'https://w3id.org/hto/ArticleTermRecord/test_page_2',
                        'page_number': 2,
                        "year": '1842',
                        # word count: 7
                        'description': "in Ancient Geography, a district of Petraea.",
                        'volume_uri': 'https://w3id.org/hto/Volume/7_8',
                        'volume_number': 8,
                        'num_pages': 974},
                       {'page_uri': 'https://w3id.org/hto/ArticleTermRecord/test_page_3',
                        'page_number': 3,
                        "year": '1771',
                        # word count: 7
                        'description': "in Ancient Geography, a district of Petraea.",
                        'volume_uri': 'https://w3id.org/hto/Volume/1_3',
                        'volume_number': 3,
                        'num_pages': 974},
                       {'page_uri': 'https://w3id.org/hto/ArticleTermRecord/test_page_4',
                        'page_number': 4,
                        "year": '1771',
                        # word count: 7
                        'description': "in Ancient Geography, a district of Petraea.",
                        'volume_uri': 'https://w3id.org/hto/Volume/1_3',
                        'volume_number': 3,
                        'num_pages': 974}]
        config = {
            "collection": "Chapbooks printed in Scotland"
        }
        # Create a Spark DataFrame
        original_df = self.spark.createDataFrame(sample_data)
        context = self.spark.sparkContext
        logger = context._jvm.org.apache.log4j.LogManager.getLogger(__name__)
        result = publication_normalized.do_query(original_df, config, logger, context)
        expect_result = {'1771': [1, 974,  14], '1842': [2, 1876, 19]}
        self.assertEqual(result, expect_result)
