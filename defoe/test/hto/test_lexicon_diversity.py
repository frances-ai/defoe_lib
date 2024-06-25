"""
defoe.hto.queries.lexicon_diversity tests.
"""

from .pyspark_testcase import PySparkTestCase

from defoe.hto.queries import lexicon_diversity
from lexicalrichness import LexicalRichness


class TestLexiconDiversity(PySparkTestCase):
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
                        'description': "in Ancient Geography, a district of Petraea. note"}]

    def test_chapbooks_volume_level(self):
        sample_data = self.chapbooks_sample_data
        config = {
            "collection": "Chapbooks printed in Scotland",
            "level": "volume",
            "data": "/Users/ly40/Documents/frances-ai/defoe_lib/defoe/test/hto/test_lexicon.txt"
        }
        # Create a Spark DataFrame
        original_df = self.spark.createDataFrame(sample_data)
        context = self.spark.sparkContext
        logger = context._jvm.org.apache.log4j.LogManager.getLogger(__name__)

        result = lexicon_diversity.do_query(original_df, config, logger, context)
        print(result)
        expect_result = [('1771', [('Test Series Title', 'https://w3id.org/hto/Series/1', 1, 'https://w3id.org/hto/Volume/1', 'Test Volume Title', 1, 8, 8, 1.0, 0.0, 8.0), ('Test Series Title', 'https://w3id.org/hto/Series/1', 1, 'https://w3id.org/hto/Volume/2', 'Test Volume Title', 2, 8, 8, 1.0, 0.0, 8.0), ('Test Series Title', 'https://w3id.org/hto/Series/7', 7, 'https://w3id.org/hto/Volume/1', 'Test Volume Title', 1, 19, 23, 0.8260869565217391, 0.019433317138954045, 37.03)])]
        self.assertEqual(len(result), len(expect_result))
        for index in range(len(result)):
            year_result = result[index]
            year_expect = expect_result[index]
            self.assertEqual(year_result[0], year_expect[0])
            for (row, expect_row) in zip(year_result[1], year_expect[1]):
                self.assertEqual(row[0], expect_row[0])
                self.assertEqual(row[1], expect_row[1])
                self.assertEqual(row[2], expect_row[2])
                self.assertEqual(row[3], expect_row[3])
                self.assertEqual(row[4], expect_row[4])
                self.assertEqual(row[5], expect_row[5])
                self.assertEqual(row[6], expect_row[6])
                self.assertEqual(row[7], expect_row[7])
                # ttr
                self.assertAlmostEqual(row[8], expect_row[8])
                # Maas
                self.assertAlmostEqual(row[9], expect_row[9])
                # mtld
                self.assertAlmostEqual(row[10], expect_row[10])

    def test_chapbooks_series_level(self):
        sample_data = self.chapbooks_sample_data
        config = {
            "collection": "Chapbooks printed in Scotland",
            "level": "series",
            "data": "/Users/ly40/Documents/frances-ai/defoe_lib/defoe/test/hto/test_lexicon.txt"
        }
        # Create a Spark DataFrame
        original_df = self.spark.createDataFrame(sample_data)
        context = self.spark.sparkContext
        logger = context._jvm.org.apache.log4j.LogManager.getLogger(__name__)
        result = lexicon_diversity.do_query(original_df, config, logger, context)
        print(result)
        expect_result = [('1771', [('Test Series Title', 'https://w3id.org/hto/Series/7', 7, 19, 23, 0.8260869565217391, 0.019433317138954045, 37.03), ('Test Series Title', 'https://w3id.org/hto/Series/1', 1, 9, 16, 0.5625, 0.07484656774906806, 16.0)])]
        self.assertEqual(len(result), len(expect_result))
        for index in range(len(result)):
            year_result = result[index]
            year_expect = expect_result[index]
            self.assertEqual(year_result[0], year_expect[0])
            for (row, expect_row) in zip(year_result[1], year_expect[1]):
                self.assertEqual(row[0], expect_row[0])
                self.assertEqual(row[1], expect_row[1])
                self.assertEqual(row[2], expect_row[2])
                self.assertEqual(row[3], expect_row[3])
                self.assertEqual(row[4], expect_row[4])
                # ttr
                self.assertAlmostEqual(row[5], expect_row[5])
                # Maas
                self.assertAlmostEqual(row[6], expect_row[6])
                # mtld
                self.assertAlmostEqual(row[7], expect_row[7])

    def test_chapbooks_year_level(self):
        sample_data = self.chapbooks_sample_data
        config = {
            "collection": "Chapbooks printed in Scotland",
            "level": "year",
            "data": "/Users/ly40/Documents/frances-ai/defoe_lib/defoe/test/hto/test_lexicon.txt"
        }
        # Create a Spark DataFrame
        original_df = self.spark.createDataFrame(sample_data)
        context = self.spark.sparkContext
        logger = context._jvm.org.apache.log4j.LogManager.getLogger(__name__)
        result = lexicon_diversity.do_query(original_df, config, logger, context)
        expect_result = [('1771', [(20, 39, 0.5128205128205128, 0.04975749509601646, 20.05188679245283)])]
        self.assertEqual(len(result), len(expect_result))
        for index in range(len(result)):
            year_result = result[index]
            year_expect = expect_result[index]
            self.assertEqual(year_result[0], year_expect[0])
            for (row, expect_row) in zip(year_result[1], year_expect[1]):
                self.assertEqual(row[0], expect_row[0])
                self.assertEqual(row[1], expect_row[1])
                # ttr
                self.assertAlmostEqual(row[2], expect_row[2])
                # Maas
                self.assertAlmostEqual(row[3], expect_row[3])
                # mtld
                self.assertAlmostEqual(row[4], expect_row[4])
