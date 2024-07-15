"""
defoe.hto.queries.lexicon_diversity tests.
"""

from .pyspark_testcase import PySparkTestCase

from defoe.hto.queries import frequency_distribution


class TestFrequencyDistribution(PySparkTestCase):
    def setUp(self):
        self.chapbooks_sample_data = [{'page_uri': 'https://w3id.org/hto/Page/test_page_1',
                        'page_number': 1,
                        "year": '1771',
                        "series_title": "Test Series Title",
                        "series_uri": "https://w3id.org/hto/Series/7",
                        "series_number": 7,
                        "source_file_uri": "https://w3id.org/hto/InformationResource/1",
                        "source_file_path": "source_path1.xml",
                        "volume_uri": "https://w3id.org/hto/Volume/1",
                        "volume_title": "Test Volume Title",
                        "volume_number": 1,
                        'description': 'test under, and y Xuirra, tongue, in Anatomy, two glands of the tongue. test, China.'},
                       {'page_uri': 'https://w3id.org/hto/Page/test_page_2',
                        'page_number': 2,
                        "year": '1771',
                        "series_title": "Test Series Title",
                        "series_uri": "https://w3id.org/hto/Series/7",
                        "series_number": 7,
                        "source_file_uri": "https://w3id.org/hto/InformationResource/1",
                        "source_file_path": "source_path1.xml",
                        "volume_uri": "https://w3id.org/hto/Volume/1",
                        "volume_title": "Test Volume Title",
                        "volume_number": 1,
                        'description': "in Ancient Geography, a district of Petraea. note"},
                       {'page_uri': 'https://w3id.org/hto/Page/test_page_3',
                        'page_number': 3,
                        "year": '1771',
                        # word count: 7
                        "series_title": "Test Series Title",
                        "series_uri": "https://w3id.org/hto/Series/1",
                        "series_number": 1,
                        "source_file_uri": "https://w3id.org/hto/InformationResource/2",
                        "source_file_path": "source_path2.xml",
                        "volume_uri": "https://w3id.org/hto/Volume/1",
                        "volume_title": "Test Volume Title",
                        "volume_number": 1,
                        'description': "in Ancient Geography, a district of Petraea. Edinburgh"},
                       {'page_uri': 'https://w3id.org/hto/Page/test_page_4',
                        'page_number': 4,
                        "year": '1771',
                        # word count: 7
                        "series_title": "Test Series Title",
                        "series_uri": "https://w3id.org/hto/Series/1",
                        "series_number": 1,
                        "source_file_uri": "https://w3id.org/hto/InformationResource/4",
                        "source_file_path": "source_path4.xml",
                        "volume_uri": "https://w3id.org/hto/Volume/2",
                        "volume_title": "Test Volume Title",
                        "volume_number": 2,
                        'description': "in Ancient Geography, a district of Petraea. note"},
                      {'page_uri': 'https://w3id.org/hto/Page/test_page_4',
                       'page_number': 4,
                       "year": '1842',
                       # word count: 7
                       "series_title": "Test Series Title",
                       "series_uri": "https://w3id.org/hto/Series/7",
                       "series_number": 1,
                       "source_file_uri": "https://w3id.org/hto/InformationResource/4",
                       "source_file_path": "source_path4.xml",
                       "volume_uri": "https://w3id.org/hto/Volume/2",
                       "volume_title": "Test Volume Title",
                       "volume_number": 2,
                       'description': "in Ancient Geography, a district of Petraea. note"}]

    def test_chapbooks_year_level(self):
        sample_data = self.chapbooks_sample_data
        config = {
            "collection": "Chapbooks printed in Scotland",
            "level": "year"
        }
        # Create a Spark DataFrame
        original_df = self.spark.createDataFrame(sample_data)
        context = self.spark.sparkContext
        logger = context._jvm.org.apache.log4j.LogManager.getLogger(__name__)

        result = frequency_distribution.do_query(original_df, config, logger, context)
        print(result)
        expect_result = [('1771', [('geography', 3), ('district', 3), ('ancient', 3), ('petraea', 3), ('tongue', 2), ('test', 2), ('note', 2), ('two', 1), ('xuirra', 1), ('edinburgh', 1), ('anatomy', 1), ('china', 1), ('gland', 1)]), ('1842', [('petraea', 1), ('note', 1), ('ancient', 1), ('geography', 1), ('district', 1)])]
        self.assertEqual(result, expect_result)

    def test_chapbooks_collection_level(self):
        sample_data = self.chapbooks_sample_data
        config = {
            "collection": "Chapbooks printed in Scotland",
            "level": "collection"
        }
        # Create a Spark DataFrame
        original_df = self.spark.createDataFrame(sample_data)
        context = self.spark.sparkContext
        logger = context._jvm.org.apache.log4j.LogManager.getLogger(__name__)

        result = frequency_distribution.do_query(original_df, config, logger, context)
        expect_result = {"words_freq": [('petraea', 4), ('ancient', 4), ('geography', 4), ('district', 4), ('note', 3), ('test', 2), ('tongue', 2), ('two', 1), ('gland', 1), ('china', 1), ('anatomy', 1), ('edinburgh', 1), ('xuirra', 1)]}
        self.assertEqual(result, expect_result)

    def test_chapbooks_collection_level_exclude_words(self):
        sample_data = self.chapbooks_sample_data
        exclude_words = ["two", "test"]
        config = {
            "collection": "Chapbooks printed in Scotland",
            "level": "collection",
            "exclude_words": exclude_words
        }
        # Create a Spark DataFrame
        original_df = self.spark.createDataFrame(sample_data)
        context = self.spark.sparkContext
        logger = context._jvm.org.apache.log4j.LogManager.getLogger(__name__)

        result = frequency_distribution.do_query(original_df, config, logger, context)
        expect_result = {"words_freq":[('petraea', 4), ('ancient', 4), ('geography', 4), ('district', 4), ('note', 3), ('tongue', 2), ('gland', 1), ('china', 1), ('anatomy', 1), ('edinburgh', 1), ('xuirra', 1)]}
        self.assertEqual(result, expect_result)

    def test_chapbooks_series_level(self):
        sample_data = self.chapbooks_sample_data
        config = {
            "collection": "Chapbooks printed in Scotland",
            "level": "series",
        }
        # Create a Spark DataFrame
        original_df = self.spark.createDataFrame(sample_data)
        context = self.spark.sparkContext
        logger = context._jvm.org.apache.log4j.LogManager.getLogger(__name__)
        result = frequency_distribution.do_query(original_df, config, logger, context)
        print(result)
        expect_result = [('1771', [('Test Series Title', 'https://w3id.org/hto/Series/7', 7, [('test', 2), ('tongue', 2), ('china', 1), ('two', 1), ('petraea', 1), ('note', 1), ('ancient', 1), ('xuirra', 1), ('geography', 1), ('district', 1), ('anatomy', 1), ('gland', 1)]), ('Test Series Title', 'https://w3id.org/hto/Series/1', 1, [('geography', 2), ('district', 2), ('ancient', 2), ('petraea', 2), ('note', 1), ('edinburgh', 1)])]), ('1842',[('Test Series Title','https://w3id.org/hto/Series/7',1,[('ancient', 1),('petraea', 1),('note', 1), ('geography', 1),('district', 1)])])]
        self.assertEqual(result, expect_result)

    def test_chapbooks_volume_level(self):
        sample_data = self.chapbooks_sample_data
        config = {
            "collection": "Chapbooks printed in Scotland",
            "level": "volume",
        }
        # Create a Spark DataFrame
        original_df = self.spark.createDataFrame(sample_data)
        context = self.spark.sparkContext
        logger = context._jvm.org.apache.log4j.LogManager.getLogger(__name__)
        result = frequency_distribution.do_query(original_df, config, logger, context)
        print(result)
        expect_result = [('1771', [('Test Series Title', 'https://w3id.org/hto/Series/1', 1, 'https://w3id.org/hto/Volume/1', 'Test Volume Title', 1, [('ancient', 1), ('edinburgh', 1), ('geography', 1), ('district', 1), ('petraea', 1)]), ('Test Series Title', 'https://w3id.org/hto/Series/1', 1, 'https://w3id.org/hto/Volume/2', 'Test Volume Title', 2, [('geography', 1), ('district', 1), ('ancient', 1), ('petraea', 1), ('note', 1)]), ('Test Series Title', 'https://w3id.org/hto/Series/7', 7, 'https://w3id.org/hto/Volume/1', 'Test Volume Title', 1, [('tongue', 2), ('test', 2), ('china', 1), ('two', 1), ('gland', 1), ('xuirra', 1), ('geography', 1), ('district', 1), ('anatomy', 1), ('petraea', 1), ('note', 1), ('ancient', 1)])]), ('1842', [('Test Series Title', 'https://w3id.org/hto/Series/7', 1, "https://w3id.org/hto/Volume/2", "Test Volume Title", 2,[('ancient', 1),('geography', 1), ('district', 1), ('petraea', 1), ('note', 1)])])]
        self.assertEqual(result, expect_result)