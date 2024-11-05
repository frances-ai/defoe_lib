import unittest

from SPARQLWrapper import SPARQLWrapper

from defoe.hto import sparql_service
import pandas as pd


class TestSparqlService(unittest.TestCase):
    def setUp(self):
        self.edition_1st_1771_mmsid = "992277653804341"
        self.edition_7th_1842_mmsid = "9910796273804340"
        self.hto_sparql_endpoint = "http://query.frances-ai.com/hto"
        self.hto_sparql_wrapper = SPARQLWrapper(self.hto_sparql_endpoint)

    def test_eb_terms_Ash_1st_1771(self):
        # This test will examine if the get_eb_terms_from_editions_with_source_provider function works
        # for high quality text for 1st edition in 1771 from Ash.
        edition_mmsids = [self.edition_1st_1771_mmsid]
        eb_terms_Ash_1st_1771 = sparql_service.get_eb_terms_from_editions_with_source_provider(self.hto_sparql_wrapper,
                                                                                               edition_mmsids,
                                                                                         sparql_service.Agent.ASH)
        df = pd.DataFrame(eb_terms_Ash_1st_1771)
        num_of_terms = 15955
        self.assertEqual(num_of_terms, len(df))
        terms_with_alter_names_df = df[df["alter_names"].apply(lambda alter_names: len(alter_names) > 0)]
        self.assertTrue(len(terms_with_alter_names_df) > 0)

    def test_eb_terms_NCKP_7th_1842(self):
        # This test will examine if the get_eb_terms_from_editions_with_source_provider function works
        # for high quality text for 7th edition in 1842 from NCKP.
        edition_mmsids = [self.edition_7th_1842_mmsid]
        eb_terms_NCKP_7th = sparql_service.get_eb_terms_from_editions_with_source_provider(self.hto_sparql_wrapper,
                                                                                               edition_mmsids,
                                                                                         sparql_service.Agent.NCKP)
        df = pd.DataFrame(eb_terms_NCKP_7th)
        num_of_terms = 23970
        self.assertEqual(num_of_terms, len(df))
        terms_with_alter_names_df = df[df["alter_names"].apply(lambda alter_names: len(alter_names) > 0)]
        self.assertTrue(len(terms_with_alter_names_df) > 0)
        terms_with_note_df = df[df["note"] == ""]
        self.assertTrue(len(terms_with_note_df) > 0)

    def test_eb_terms_NLS_7th_1842(self):
        # This test will examine if the get_eb_terms_from_editions_with_source_provider function works
        # for text for 7th edition in 1842 from NLS.
        edition_mmsids = [self.edition_7th_1842_mmsid]
        eb_terms_NLS_7th = sparql_service.get_eb_terms_from_editions_with_source_provider(self.hto_sparql_wrapper,
                                                                                               edition_mmsids,
                                                                                         sparql_service.Agent.NLS)
        df = pd.DataFrame(eb_terms_NLS_7th)
        num_of_terms = 13459
        self.assertEqual(num_of_terms, len(df))
        terms_with_alter_names_df = df[df["alter_names"].apply(lambda alter_names: len(alter_names) > 0)]
        self.assertTrue(len(terms_with_alter_names_df) == 0)
        terms_with_note_df = df[df["note"] != ""]
        self.assertTrue(len(terms_with_note_df) == 0)

    def test_eb_terms_Neuspell_7th_1842(self):
        # This test will examine if the get_eb_terms_from_editions_with_source_provider function works
        # for neuspell corrected text for 7th edition in 1842.
        edition_mmsids = [self.edition_7th_1842_mmsid]
        eb_terms_Neuspell_7th = sparql_service.get_neuspell_corrected_eb_terms_from_editions(self.hto_sparql_wrapper,
                                                                                               edition_mmsids)
        df = pd.DataFrame(eb_terms_Neuspell_7th)
        num_of_terms = 13459
        self.assertEqual(num_of_terms, len(df))
        terms_with_alter_names_df = df[df["alter_names"].apply(lambda alter_names: len(alter_names) > 0)]
        self.assertTrue(len(terms_with_alter_names_df) == 0)
        terms_with_note_df = df[df["note"] != ""]
        self.assertTrue(len(terms_with_note_df) == 0)

    def test_chapbooks_pages_NLS(self):
        # This test will examine if the get_nls_page_from_nls function works
        # for chapbooks text from NLS dataset.
        chapbooks_pages = sparql_service.get_nls_page_from_nls(self.hto_sparql_wrapper,
                                                               collection_name=sparql_service.NLSCollection.CHAPBOOKS.value)
        df = pd.DataFrame(chapbooks_pages)
        num_of_pages = 47329
        self.assertEqual(num_of_pages, len(df))



