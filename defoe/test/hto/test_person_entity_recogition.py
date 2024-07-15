"""
defoe.hto.queries.person_entity_recognition tests.
"""

from .pyspark_testcase import PySparkTestCase

from defoe.hto.queries import person_entity_recognition


class TestPersonEntityRecognition(PySparkTestCase):
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
                        'description': "Elon Musk announced the new Tesla model in California."},
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
                        'description': "Marie Curie was awarded the Nobel Prize in Physics in 1903."},
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
                        'description': "Taylor Swift performed her latest song at the Madison Square Garden."}]

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

        result = person_entity_recognition.do_query(original_df, config, logger, context)
        expect_result = {'persons_freq': [('Marie Curie', 'female', 1), ('Elon Musk', 'male', 1), ('Taylor Swift', 'mostly_male', 1)], 'genders_freq': [('mostly_male', 1), ('female', 1), ('male', 1)]}
        self.assertEqual(result, expect_result)

    def test_chapbooks_collection_level_exclude_words(self):
        sample_data = self.chapbooks_sample_data
        exclude_words = ["Taylor Swift"]
        config = {
            "collection": "Chapbooks printed in Scotland",
            "level": "collection",
            "exclude_words": exclude_words
        }
        # Create a Spark DataFrame
        original_df = self.spark.createDataFrame(sample_data)
        context = self.spark.sparkContext
        logger = context._jvm.org.apache.log4j.LogManager.getLogger(__name__)

        result = person_entity_recognition.do_query(original_df, config, logger, context)
        expect_result = {'persons_freq': [('Marie Curie', 'female', 1), ('Elon Musk', 'male', 1)], 'genders_freq': [ ('female', 1), ('male', 1)]}
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
        result = person_entity_recognition.do_query(original_df, config, logger, context)
        print(result)
        expect_result = {'persons_freq': [('1771', [('Test Series Title', 'https://w3id.org/hto/Series/7', 7, [('Elon Musk', 'male', 1)]), ('Test Series Title', 'https://w3id.org/hto/Series/1', 1, [('Marie Curie', 'female', 1), ('Taylor Swift', 'mostly_male', 1)])])], 'genders_freq': [('1771', [('Test Series Title', 'https://w3id.org/hto/Series/7', 7, [('male', 1)]), ('Test Series Title', 'https://w3id.org/hto/Series/1', 1, [('mostly_male', 1), ('female', 1)])])]}
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
        result = person_entity_recognition.do_query(original_df, config, logger, context)
        print(result)
        expect_result = {'persons_freq': [('1771', [('Test Series Title', 'https://w3id.org/hto/Series/1', 1, 'https://w3id.org/hto/Volume/1', 'Test Volume Title', 1, [('Marie Curie', 'female', 1)]), ('Test Series Title', 'https://w3id.org/hto/Series/1', 1, 'https://w3id.org/hto/Volume/2', 'Test Volume Title', 2, [('Taylor Swift', 'mostly_male', 1)]), ('Test Series Title', 'https://w3id.org/hto/Series/7', 7, 'https://w3id.org/hto/Volume/1', 'Test Volume Title', 1, [('Elon Musk', 'male', 1)])])], 'genders_freq': [('1771', [('Test Series Title', 'https://w3id.org/hto/Series/1', 1, 'https://w3id.org/hto/Volume/1', 'Test Volume Title', 1, [('female', 1)]), ('Test Series Title', 'https://w3id.org/hto/Series/1', 1, 'https://w3id.org/hto/Volume/2', 'Test Volume Title', 2, [('mostly_male', 1)]), ('Test Series Title', 'https://w3id.org/hto/Series/7', 7, 'https://w3id.org/hto/Volume/1', 'Test Volume Title', 1, [('male', 1)])])]}
        self.assertEqual(result, expect_result)

    def test_chapbooks_year_level(self):
        sample_data = self.chapbooks_sample_data
        config = {
            "collection": "Chapbooks printed in Scotland",
            "level": "year",
        }
        # Create a Spark DataFrame
        original_df = self.spark.createDataFrame(sample_data)
        context = self.spark.sparkContext
        logger = context._jvm.org.apache.log4j.LogManager.getLogger(__name__)
        result = person_entity_recognition.do_query(original_df, config, logger, context)
        print(result)
        expect_result = {'persons_freq': [('1771', [('Marie Curie', 'female', 1), ('Taylor Swift', 'mostly_male', 1), ('Elon Musk', 'male', 1)])], 'genders_freq': [('1771', [('male', 1), ('female', 1), ('mostly_male', 1)])]}
        self.assertEqual(result, expect_result)
