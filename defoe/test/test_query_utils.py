import unittest

from defoe import get_root_path, get_geo_supported_os_type
from defoe.query_utils import (get_geoparser_xml, geoparser_coord_xml, SpacyMagic, xml_geo_entities, georesolve_cmd, georesolved_xml_tojson,
                               get_geoparser_xml_stanza, georesolved_xml_to_dict)


class TestQueryUtils(unittest.TestCase):
    def setUp(self):
        self.defoe_path = get_root_path() + "/"
        self.os_type = get_geo_supported_os_type()

    def test_geoparser_xml(self):
        print(self.defoe_path)
        gazetteer = "geonames"
        bounding_box = ""
        test_text = "The Duke of York is visiting Aberdeen and the University of Edinburgh this weekend. "
        result_xml = get_geoparser_xml(test_text, self.defoe_path, self.os_type, gazetteer, bounding_box)
        print(result_xml)

    def test_geoparser_xml_stanza(self):
        print(self.defoe_path)
        gazetteer = "geonames"
        bounding_box = ""
        test_text = "The Duke of York is visiting Aberdeen and the University of Edinburgh this weekend. "
        result_xml = get_geoparser_xml_stanza(test_text, self.defoe_path, gazetteer, bounding_box)
        print(result_xml)

    def test_georesolve_after_stanza(self):
        print(self.defoe_path)
        gazetteer = "geonames"
        bounding_box = ""
        test_text = "The Duke of York is visiting Aberdeen and the University of Edinburgh this weekend. "
        result_xml = get_geoparser_xml_stanza(test_text, self.defoe_path, gazetteer, bounding_box)
        resolved_result = georesolved_xml_to_dict(result_xml)
        print(resolved_result)

    def test_georesolve(self):
        print(self.defoe_path)
        gazetteer = "geonames"
        bounding_box = ""
        test_text = "I love China!"
        result_xml = get_geoparser_xml(test_text, self.defoe_path, self.os_type, gazetteer, bounding_box)
        resolved_result = geoparser_coord_xml(result_xml)
        print(resolved_result)

    def test_xml_geo_entities(self):
        gazetteer = "geonames"
        bounding_box = ""
        test_text = "I love China! I lives in Scotland"
        nlp = SpacyMagic.get('en_core_web_lg')
        doc = nlp(test_text)
        flag, in_xml = xml_geo_entities(doc)
        print(in_xml)
        resolved_xml = georesolve_cmd(in_xml, self.defoe_path, gazetteer, bounding_box)
        print(resolved_xml)
        geo_list = georesolved_xml_tojson(resolved_xml)
        print(geo_list)


if __name__ == '__main__':
    unittest.main()
