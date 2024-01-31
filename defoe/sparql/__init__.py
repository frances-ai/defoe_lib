"""
SPARQL
"""
from defoe.sparql.queries import frequency_keysearch_by_year, publication_normalized, fulltext_keysearch_by_year, \
    uris_keysearch, snippet_keysearch_by_year, geoparser_by_year
from pyspark.sql import SQLContext
from SPARQLWrapper import SPARQLWrapper, JSON


highest_quality_source_providers = {
    "992277653804341": "Ash Charlton",
    "9910796273804340": "Nineteenth-Century Knowledge Project"
}

default_source_provider = "National Library of Scotland"


def find_highest_quality_source_provider(mmsid):
    if mmsid in highest_quality_source_providers:
        return highest_quality_source_providers[mmsid]
    return default_source_provider


class Model:
    def get_queries(self):
        return {
            "frequency_keysearch_by_year": frequency_keysearch_by_year.do_query,
            "publication_normalized": publication_normalized.do_query,
            "fulltext_keysearch_by_year": fulltext_keysearch_by_year.do_query,
            "uris_keysearch": uris_keysearch.do_query,
            "snippet_keysearch_by_year": snippet_keysearch_by_year.do_query,
            "geoparser_by_year": geoparser_by_year.do_query
        }

    def get_eb_terms_by_mmsid_source_provider(self, sparql, mmsid, source_provider):
        sparql_data = []
        query = """
                    PREFIX hto: <https://w3id.org/hto#>
                    PREFIX prov: <http://www.w3.org/ns/prov#>
                    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
                    SELECT * WHERE {
                      ?term a hto:ArticleTermRecord;
                            hto:name ?term_name;
                            hto:startsAtPage ?page;
                            hto:hasOriginalDescription ?description_uri.
                      ?description_uri hto:wasExtractedFrom ?source;
                                       hto:text ?definition.
                      ?source prov:wasAttributedTo ?source_provider.
                      ?source_provider a ?agentType;
                             foaf:name %s.
                      FILTER (?agentType = hto:Person || ?agentType = hto:Organization)
                      ?page a hto:Page;
                            hto:number ?page_num.
                      OPTIONAL {
                        ?page hto:header ?header.
                      }
                      ?volume a hto:Volume;
                              hto:hadMember ?page;
                              hto:number ?volume_num;
                              hto:letters ?letters.
                      OPTIONAL {
                        ?volume hto:part ?part.
                      }
                      ?edition a hto:Edition;
                               hto:mmsid %s;
                               hto:hadMember ?volume;
                               hto:title ?edition_title;
                               hto:yearPublished ?year_published.
                       OPTIONAL {
                          ?edition hto:number ?edition_num.
                       }
                    }
                """ % (source_provider, mmsid)
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        for r in results["results"]["bindings"]:
            header = None
            if "header" in r:
                header = r["header"]["value"]
            part = None
            if "part" in r:
                part = r["part"]["value"]
            edition_num = None
            if "edition_num" in r:
                edition_num = r["edition_num"]["value"]
            sparql_data.append({"uri": r["term"]["value"], "year": r["year_published"]["value"], "title": r["edition_title"]["value"],
                                "edition": edition_num, "vuri": r["volume"]["value"],
                                "volume": r["volume_num"]["value"], "letters": r["letters"]["value"], "part": part,
                                "page": r["page_num"]["value"], "header": header, "term": r["term"]["value"],
                                "definition": r["definition"]["value"]})
        return sparql_data

    def get_eb_editions_info(self, sparql):
        # query the edition info
        edition_query = """
                    PREFIX hto: <https://w3id.org/hto#>
                    SELECT ?mmsid ?edition_num ?year_published WHERE {
                        ?edition a hto:Edition;
                            hto:mmsid ?mmsid;
                            hto:yearPublished ?year_published.
                            OPTIONAL {
                                ?edition hto:number ?edition_num.
                            }
                    }
                """
        sparql.setQuery(edition_query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        edition_info = []
        for r in results["results"]["bindings"]:
            edition_num = None
            if "edition_num" in r:
                edition_num = r["edition_num"]["value"]
            edition_info.append({"edition_num": edition_num, "year_published": r["year_published"]["value"],
                                 "mmsid": r["mmsid"]["value"]})
        return edition_info

    def get_eb_high_quality(self, sparql):
        # editions_info = self.get_eb_editions_info(sparql)
        sparql_data = []
        """
        for edition in editions_info:
            mmsid = edition["mmsid"]
            source_provider = find_highest_quality_source_provider(mmsid)
            terms = self.get_eb_terms_by_mmsid_source_provider(sparql, mmsid, source_provider)
            sparql_data.append(terms)
        """
        query = """
                    PREFIX hto: <https://w3id.org/hto#>
                    PREFIX prov: <http://www.w3.org/ns/prov#>
                    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
                    SELECT * WHERE {
                      ?term a hto:ArticleTermRecord;
                            hto:name ?term_name;
                            hto:startsAtPage ?page;
                            hto:hasOriginalDescription ?description_uri.
                      ?description_uri hto:wasExtractedFrom ?source;
                                       hto:text ?definition.
                      ?source prov:wasAttributedTo ?source_provider.
                      ?source_provider a ?agentType;
                             foaf:name ?source_provider_name.
                      FILTER (?agentType = hto:Person || ?agentType = hto:Organization)
                      ?page a hto:Page;
                            hto:number ?page_num.
                      OPTIONAL {
                        ?page hto:header ?header.
                      }
                      ?volume a hto:Volume;
                              hto:hadMember ?page;
                              hto:number ?volume_num;
                              hto:letters ?letters.
                      OPTIONAL {
                        ?volume hto:part ?part.
                      }
                      ?edition a hto:Edition;
                               hto:mmsid ?mmsid;
                               hto:hadMember ?volume;
                               hto:title ?edition_title;
                               hto:yearPublished ?year_published.
                       OPTIONAL {
                          ?edition hto:number ?edition_num.
                       }
                    }
                        """
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        terms = {}
        for r in results["results"]["bindings"]:
            if "term" not in r:
                print(r)
                continue
            term_uri = r["term"]["value"]

            term_name = r["term_name"]["value"]
            if term_uri in terms:
                terms[term_uri]["term_names"].append(term_name)
            else:
                header = None
                if "header" in r:
                    header = r["header"]["value"]
                part = None
                if "part" in r:
                    part = r["part"]["value"]
                edition_num = None
                if "edition_num" in r:
                    edition_num = r["edition_num"]["value"]
                definition = r["definition"]["value"]
                number_of_words = len(definition.split())
                terms[term_uri] = {"year": r["year_published"]["value"], "title": r["edition_title"]["value"],
                 "edition": edition_num, "vuri": r["volume"]["value"],
                 "volume": r["volume_num"]["value"], "letters": r["letters"]["value"], "part": part,
                 "page": r["page_num"]["value"], "header": header, "term_names": [term_name],
                 "definition": r["definition"]["value"], "numWords": number_of_words}

        for term in terms:
            sparql_data.append(
                {"uri": term, "year": terms[term]["year"], "title": terms[term]["title"],
                 "edition": terms[term]["edition"], "vuri": terms[term]["vuri"],
                 "volume": terms[term]["volume"], "letters": terms[term]["letters"], "part": terms[term]["part"],
                 "page": terms[term]["page"], "header": terms[term]["header"], "term_names": terms[term]["term_names"],
                 "definition": terms[term]["definition"], "numWords": terms[term]["numWords"]})

        return sparql_data

    def get_nls_info(self, sparql):
        query = """
        PREFIX nls: <https://w3id.org/nls#> 
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        SELECT ?uri ?year ?title ?snum ?vnum ?v ?part ?metsXML ?page ?text ?numberOfWords ?numberOfPages ?vtitle ?volumeId
            WHERE {
            ?uri a nls:Page .
            ?uri nls:text ?text .
            ?uri nls:number ?page .
            ?uri nls:numberOfWords ?numberOfWords .
            ?v nls:hasPart ?uri.
            ?v nls:number ?vnum.
            ?v nls:numberOfPages ?numberOfPages .
            ?v nls:metsXML ?metsXML.
            ?v nls:title ?vtitle.
            ?v nls:volumeId ?volumeId.
            ?s nls:hasPart ?v.
            ?s nls:publicationYear ?year.
            ?s nls:number ?snum.
            ?s nls:title ?title.
            OPTIONAL {?v nls:part ?part; }
            }   
        """
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()

        sparql_data = []
        for r in results["results"]["bindings"]:
            if "part" in r:
                v_part = r["part"]["value"]
            else:
                v_part = "None"
            sparql_data.append({"uri": r["uri"]["value"], "year": r["year"]["value"], "title": r["title"]["value"],
                                "serie": r["snum"]["value"], "vuri": r["v"]["value"], "volume": r["vnum"]["value"],
                                "numPages": r["numberOfPages"]["value"], "part": v_part,
                                "archive_filename": r["metsXML"]["value"], "page": r["page"]["value"],
                                "text": r["text"]["value"], "numWords": r["numberOfWords"]["value"],
                                "vtitle": r["vtitle"]["value"], "volumeId": r["volumeId"]["value"]})
        return sparql_data

    def get_eb_info(self, sparql):
        query = """
                  PREFIX eb: <https://w3id.org/eb#>
                  PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                  SELECT ?uri ?year ?title ?enum ?vnum ?v ?letters ?part ?metsXML ?page ?header ?term ?definition ?numberOfWords ?numberOfPages
                      WHERE {{
              	    ?uri a eb:Article .
              	    ?uri eb:name ?term .
                      ?uri eb:definition ?definition .
                      ?uri eb:numberOfWords ?numberOfWords . 
                      ?v eb:hasPart ?uri.
                      ?v eb:number ?vnum.
                      ?v eb:numberOfPages ?numberOfPages .
                      ?v eb:metsXML ?metsXML.
                      ?v eb:letters ?letters .
                      ?e eb:hasPart ?v.
                      ?e eb:publicationYear ?year.
                      ?e eb:number ?enum.
                      ?e eb:title ?title.
                      ?uri eb:startsAtPage ?sp.
                      ?sp eb:header ?header .
                      ?sp eb:number ?page .
                      OPTIONAL {?v eb:part ?part; }

                      }

                      UNION {
              	    ?uri a eb:Topic .
              	    ?uri eb:name ?term . 
                      ?uri eb:definition ?definition .
                      ?uri eb:numberOfWords ?numberOfWords . 
                      ?v eb:hasPart ?uri.
                      ?v eb:number ?vnum.
                      ?v eb:numberOfPages ?numberOfPages .
                      ?v eb:metsXML ?metsXML.
                      ?v eb:letters ?letters .
                      ?e eb:hasPart ?v.
                      ?e eb:publicationYear ?year.
                      ?e eb:number ?enum.
                      ?e eb:title ?title.
                      ?uri eb:startsAtPage ?sp.
                      ?sp eb:header ?header .
                      ?sp eb:number ?page .
                      OPTIONAL {?v eb:part ?part; }
                      }
                  } 
                  """
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()

        sparql_data = []
        for r in results["results"]["bindings"]:
            if "part" in r:
                v_part = r["part"]["value"]
            else:
                v_part = "None"
            sparql_data.append({"uri": r["uri"]["value"], "year": r["year"]["value"], "title": r["title"]["value"],
                                "edition": r["enum"]["value"], "vuri": r["v"]["value"],
                                "volume": r["vnum"]["value"], "numPages": r["numberOfPages"]["value"],
                                "letters": r["letters"]["value"], "part": v_part,
                                "archive_filename": r["metsXML"]["value"], "page": r["page"]["value"],
                                "header": r["header"]["value"], "term": r["term"]["value"],
                                "definition": r["definition"]["value"], "numWords": r["numberOfWords"]["value"]})
        return sparql_data

    def endpoint_to_object(self, sparql_endpoint, context):
        sparql = SPARQLWrapper(sparql_endpoint)
        if "total_eb" in sparql_endpoint:
            sparql_data = self.get_eb_info(sparql)
        else:
            sparql_data = self.get_nls_info(sparql)

        sqlContext = SQLContext.getOrCreate(context)
        df = sqlContext.createDataFrame(sparql_data)
        return df
