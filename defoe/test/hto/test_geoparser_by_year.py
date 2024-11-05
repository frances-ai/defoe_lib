"""
defoe.hto.queries.geoparser_by_year tests.
"""

from .pyspark_testcase import PySparkTestCase

from defoe.hto.queries import geoparser_by_year


class TestGeoparserByYear(PySparkTestCase):
    def test_chapbooks_page(self):
        sample_data = [{'page_uri': 'https://w3id.org/hto/Page/test_page_1',
                        'page_number': 1,
                        "year": '1842',
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
                        "year": '1842',
                        "series_title": "Test Series Title",
                        "series_uri": "https://w3id.org/hto/Series/7",
                        "series_number": 7,
                        "source_file_uri": "https://w3id.org/hto/InformationResource/1",
                        "source_file_path": "source_path1.xml",
                        "volume_uri": "https://w3id.org/hto/Volume/8",
                        "volume_title": "Test Volume Title",
                        "volume_number": 8,
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
                        "series_number": 7,
                        "source_file_uri": "https://w3id.org/hto/InformationResource/4",
                        "source_file_path": "source_path4.xml",
                        "volume_uri": "https://w3id.org/hto/Volume/1",
                        "volume_title": "Test Volume Title",
                        "volume_number": 1,
                        'description': "in Ancient Geography, a district of Petraea. note"}]
        config = {
            "collection": "Chapbooks printed in Scotland",
            "data": "/Users/ly40/Documents/frances-ai/defoe_lib/defoe/test/hto/test_lexicon.txt"
        }
        # Create a Spark DataFrame
        original_df = self.spark.createDataFrame(sample_data)
        context = self.spark.sparkContext
        logger = context._jvm.org.apache.log4j.LogManager.getLogger(__name__)
        result = geoparser_by_year.do_query(original_df, config, logger, context)
        expect_result = [('1771', [{'page_uri': 'https://w3id.org/hto/Page/test_page_4', 'page_number': 4, 'series_title': 'Test Series Title', 'series_uri': 'https://w3id.org/hto/Series/1', 'series_number': 7, 'source_file_uri': 'https://w3id.org/hto/InformationResource/4', 'source_file_path': 'source_path4.xml', 'volume_uri': 'https://w3id.org/hto/Volume/1', 'volume_title': 'Test Volume Title', 'volume_number': 1, 'georesolution': {'district of Petraea-rb1': {'lat': '', 'long': '', 'pop': '', 'in-cc': '', 'type': '', 'snippet': 'in Ancient Geography , a district of Petraea . note   '}}}]), ('1842', [{'page_uri': 'https://w3id.org/hto/Page/test_page_1', 'page_number': 1, 'series_title': 'Test Series Title', 'series_uri': 'https://w3id.org/hto/Series/7', 'series_number': 7, 'source_file_uri': 'https://w3id.org/hto/InformationResource/1', 'source_file_path': 'source_path1.xml', 'volume_uri': 'https://w3id.org/hto/Volume/1', 'volume_title': 'Test Volume Title', 'volume_number': 1, 'georesolution': {'China-rb1': {'lat': '35', 'long': '105', 'pop': '1306313812', 'in-cc': 'CN', 'type': 'country', 'snippet': 'the tongue . test , China .    '}}}, {'page_uri': 'https://w3id.org/hto/Page/test_page_2', 'page_number': 2, 'series_title': 'Test Series Title', 'series_uri': 'https://w3id.org/hto/Series/7', 'series_number': 7, 'source_file_uri': 'https://w3id.org/hto/InformationResource/1', 'source_file_path': 'source_path1.xml', 'volume_uri': 'https://w3id.org/hto/Volume/8', 'volume_title': 'Test Volume Title', 'volume_number': 8, 'georesolution': {'district of Petraea-rb1': {'lat': '', 'long': '', 'pop': '', 'in-cc': '', 'type': '', 'snippet': 'in Ancient Geography , a district of Petraea . note   '}}}])]
        self.assertEqual(result, expect_result)

    def test_eb_terms(self):
        sample_data = [{'term_uri': 'https://w3id.org/hto/ArticleTermRecord/test_term_1',
                        'term_name': 'HYPOGLOTTIS',
                        'alter_names': ['HYPOGLOSSIS'],
                        'note': "",
                        "year": '1842',
                        "edition_title": "Test Edition Title",
                        "edition_uri": "https://w3id.org/hto/Edition/7",
                        "edition_number": 7,
                        "source_file_uri": "https://w3id.org/hto/InformationResource/1",
                        "source_file_path": "source_path1.xml",
                        'volume_uri': 'https://w3id.org/hto/Volume/7_12',
                        'volume_number': 12,
                        "start_page_uri": 'https://w3id.org/hto/Page/test_page_1',
                        "start_page_number": 1,
                        # word count: 14
                        'description': 'under, and y Xuirra, tongue, in Anatomy, two glands of the tongue. Edinburgh'},
                       {'term_uri': 'https://w3id.org/hto/ArticleTermRecord/test_term_2',
                        'term_name': 'EDOM',
                        'alter_names': ['IDUMAEA'],
                        'note': "test note",
                        "year": '1842',
                        "edition_title": "Test Edition Title",
                        "edition_uri": "https://w3id.org/hto/Edition/7",
                        "edition_number": 7,
                        "source_file_uri": "https://w3id.org/hto/InformationResource/1",
                        "source_file_path": "source_path1.xml",
                        'volume_uri': 'https://w3id.org/hto/Volume/7_8',
                        'volume_number': 8,
                        "start_page_uri": 'https://w3id.org/hto/Page/test_page_2',
                        "start_page_number": 2,
                        # word count: 11
                        'description': "in Ancient Geography, a district of Petraea. note"},
                       {'term_uri': 'https://w3id.org/hto/ArticleTermRecord/test_term_3',
                        'term_name': 'EDOM',
                        'alter_names': ['IDUMAEA'],
                        'note': "test note",
                        "year": '1771',
                        "edition_title": "Test Edition Title",
                        "edition_uri": "https://w3id.org/hto/Edition/1",
                        "edition_number": 1,
                        "source_file_uri": "https://w3id.org/hto/InformationResource/1",
                        "source_file_path": "source_path1.xml",
                        'volume_uri': 'https://w3id.org/hto/Volume/7_3',
                        'volume_number': 3,
                        "start_page_uri": 'https://w3id.org/hto/Page/test_page_1',
                        "start_page_number": 1,
                        # word count: 11
                        'description': "in Ancient Geography, a district of Petraea. test"},
                       {'term_uri': 'https://w3id.org/hto/ArticleTermRecord/test_term_4',
                        'term_name': 'EDOM_2',
                        'alter_names': ['IDUMAEA'],
                        'note': "test note",
                        "year": '1771',
                        "edition_title": "Test Edition Title",
                        "edition_uri": "https://w3id.org/hto/Edition/1",
                        "edition_number": 1,
                        "source_file_uri": "https://w3id.org/hto/InformationResource/1",
                        "source_file_path": "source_path1.xml",
                        'volume_uri': 'https://w3id.org/hto/Volume/7_3',
                        'volume_number': 3,
                        "start_page_uri": 'https://w3id.org/hto/Page/test_page_1',
                        "start_page_number": 1,
                        # word count: 11
                        'description': "in Ancient Geography, a district of Petraea. China"}
                       ]
        config = {
            "collection": "Encyclopaedia Britannica",
            "data": "/Users/ly40/Documents/frances-ai/defoe_lib/defoe/test/hto/test_lexicon.txt"
        }
        # Create a Spark DataFrame
        original_df = self.spark.createDataFrame(sample_data)
        context = self.spark.sparkContext
        logger = context._jvm.org.apache.log4j.LogManager.getLogger(__name__)
        result = geoparser_by_year.do_query(original_df, config, logger, context)
        expect_result = [('1771', [{'term_uri': 'https://w3id.org/hto/ArticleTermRecord/test_term_3', 'term_name': 'EDOM', 'edition_title': 'Test Edition Title', 'edition_uri': 'https://w3id.org/hto/Edition/1', 'edition_number': 1, 'source_file_uri': 'https://w3id.org/hto/InformationResource/1', 'source_file_path': 'source_path1.xml', 'volume_uri': 'https://w3id.org/hto/Volume/7_3', 'volume_number': 3, 'start_page_uri': 'https://w3id.org/hto/Page/test_page_1', 'start_page_number': 1, 'georesolution': {'district of Petraea-rb1': {'lat': '', 'long': '', 'pop': '', 'in-cc': '', 'type': '', 'snippet': 'in Ancient Geography , a district of Petraea . test   '}}}, {'term_uri': 'https://w3id.org/hto/ArticleTermRecord/test_term_4', 'term_name': 'EDOM_2', 'edition_title': 'Test Edition Title', 'edition_uri': 'https://w3id.org/hto/Edition/1', 'edition_number': 1, 'source_file_uri': 'https://w3id.org/hto/InformationResource/1', 'source_file_path': 'source_path1.xml', 'volume_uri': 'https://w3id.org/hto/Volume/7_3', 'volume_number': 3, 'start_page_uri': 'https://w3id.org/hto/Page/test_page_1', 'start_page_number': 1, 'georesolution': {'district of Petraea-rb1': {'lat': '', 'long': '', 'pop': '', 'in-cc': '', 'type': '', 'snippet': 'in Ancient Geography , a district of Petraea . China   '}, 'China-rb2': {'lat': '35', 'long': '105', 'pop': '1306313812', 'in-cc': 'CN', 'type': 'country', 'snippet': 'a district of Petraea . China     '}}}]), ('1842', [{'term_uri': 'https://w3id.org/hto/ArticleTermRecord/test_term_1', 'term_name': 'HYPOGLOTTIS', 'edition_title': 'Test Edition Title', 'edition_uri': 'https://w3id.org/hto/Edition/7', 'edition_number': 7, 'source_file_uri': 'https://w3id.org/hto/InformationResource/1', 'source_file_path': 'source_path1.xml', 'volume_uri': 'https://w3id.org/hto/Volume/7_12', 'volume_number': 12, 'start_page_uri': 'https://w3id.org/hto/Page/test_page_1', 'start_page_number': 1, 'georesolution': {'Edinburgh-rb1': {'lat': '55.95206', 'long': '-3.19648', 'pop': '435791', 'in-cc': 'GB', 'type': 'ppla', 'snippet': 'glands of the tongue . Edinburgh     '}}}, {'term_uri': 'https://w3id.org/hto/ArticleTermRecord/test_term_2', 'term_name': 'EDOM', 'edition_title': 'Test Edition Title', 'edition_uri': 'https://w3id.org/hto/Edition/7', 'edition_number': 7, 'source_file_uri': 'https://w3id.org/hto/InformationResource/1', 'source_file_path': 'source_path1.xml', 'volume_uri': 'https://w3id.org/hto/Volume/7_8', 'volume_number': 8, 'start_page_uri': 'https://w3id.org/hto/Page/test_page_2', 'start_page_number': 2, 'georesolution': {'district of Petraea-rb1': {'lat': '', 'long': '', 'pop': '', 'in-cc': '', 'type': '', 'snippet': 'in Ancient Geography , a district of Petraea . note   '}}}])]
        self.assertEqual(result, expect_result)