from enum import Enum

from SPARQLWrapper import SPARQLWrapper, JSON


class NLSCollection(Enum):
    EB = "Encyclopaedia Britannica"
    CHAPBOOKS = "Chapbooks printed in Scotland"


class Agent(Enum):
    ASH = "Ash Charlton"
    NLS = "National Library of Scotland"
    NCKP = "Nineteenth-Century Knowledge Project"


neuspell_uri = "https://github.com/neuspell/neuspell"


def get_nls_page_from_nls(hto_sparql_wrapper, collection_name):
    """
    This function will query hto kg, and get all eb terms with their descriptions and metadata extracted from NLS dataset.
    :param collection_name: name of nls collection
    :param hto_sparql_wrapper: sparql wrapper for hto sparql query
    :return: eb_terms_nls, a list of terms with their descriptions and metadata extracted from NLS dataset.
    """
    source_provider_name = Agent.NLS.value
    pages = []
    query = """
      PREFIX hto: <https://w3id.org/hto#>
      PREFIX prov: <http://www.w3.org/ns/prov#>
      PREFIX foaf: <http://xmlns.com/foaf/0.1/>
      SELECT * WHERE {
        ?page a hto:Page;
            hto:number ?page_number;
            hto:hasOriginalDescription ?description.
        ?description hto:text ?description_text;
            hto:wasExtractedFrom ?source_dataset.
        ?source_dataset prov:wasAttributedTo ?agent;
            prov:value ?source_filepath.
        ?agent foaf:name "%s".
        ?volume a hto:Volume;
            hto:number ?volume_number;
            hto:numberOfPages ?number_pages;
            hto:hadMember ?page.
        ?series a hto:Series;
            hto:title ?series_title;
            hto:number ?series_number;
            hto:yearPublished ?year;
            hto:hadMember ?volume.
        ?eb_collection a hto:WorkCollection;
            hto:name "%s";
            hto:hadMember ?series.
    }
    """ % (source_provider_name, collection_name)
    print(query)
    hto_sparql_wrapper.setQuery(query)
    hto_sparql_wrapper.setReturnFormat(JSON)
    results = hto_sparql_wrapper.query().convert()
    for r in results["results"]["bindings"]:
        pages.append(
            {"page_uri": r["page"]["value"],
             "page_number": int(r["page_number"]["value"]),
             "source_file_uri": r["source_dataset"]["value"],
             "source_file_path": r["source_filepath"]["value"],
             "description": r["description_text"]["value"],
             "series_uri": r["series"]["value"],
             "series_title": r["series_title"]["value"],
             "series_number": int(r["series_number"]["value"]),
             "volume_uri": r["volume"]["value"],
             "volume_number": int(r["volume_number"]["value"]),
             "num_pages": int(r["number_pages"]["value"]),
             "year": r["year"]["value"]
             }
        )
    return pages


def get_neuspell_corrected_eb_terms_from_editions(hto_sparql_wrapper, edition_mmsids):
    """
        This function queries neuspell corrected eb terms from a list of editions.
        :param hto_sparql_wrapper: sparql wrapper for hto sparql query
        :param edition_mmsids: A list of edition mmsids.
        :return: a list of terms
        """
    collection_name = NLSCollection.EB.value + " Collection"
    mmsid_filter_string = "FILTER (?mmsid = \"" + "\" || ?mmsid = \"".join(edition_mmsids) + "\")"
    terms = []
    query = """
          PREFIX hto: <https://w3id.org/hto#>
          PREFIX prov: <http://www.w3.org/ns/prov#>
          PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          SELECT * WHERE {
            ?term a ?term_type;
                hto:name ?term_name;
                hto:startsAtPage ?page;
                hto:hasOriginalDescription ?description.
            FILTER (?term_type = hto:ArticleTermRecord || ?term_type = hto:TopicTermRecord)
            ?description hto:text ?description_text;
                prov:wasDerivedFrom ?extracted_description_from_nls;
                prov:wasAttributedTo <%s>.
            ?extracted_description_from_nls hto:wasExtractedFrom ?nls_source.
            ?nls_source prov:value ?source_filepath.
            ?page hto:number ?page_number.
            OPTIONAL {?page hto:header ?header}
            ?volume a hto:Volume;
                hto:number ?volume_number;
                hto:numberOfPages ?number_pages;
                hto:hadMember ?page.
            OPTIONAL {?volume hto:letters ?letters;}
            OPTIONAL {?volume hto:part ?part}
            ?edition a hto:Edition;
                hto:title ?edition_title;
                hto:yearPublished ?year;
                hto:mmsid ?mmsid;
                hto:hadMember ?volume.
            OPTIONAL {?edition hto:number ?edition_number}
            %s
            ?eb_collection a hto:WorkCollection;
                hto:name "%s";
                hto:hadMember ?edition.
        }
        """ % (neuspell_uri, mmsid_filter_string, collection_name)
    print(query)
    hto_sparql_wrapper.setQuery(query)
    hto_sparql_wrapper.setReturnFormat(JSON)
    results = hto_sparql_wrapper.query().convert()
    for r in results["results"]["bindings"]:
        note = ""
        alter_names = []
        term_uri = r["term"]["value"]
        edition_number = -1
        if "edition_number" in r:
            edition_number = int(r["edition_number"]["value"])
        part = -1
        if "part" in r:
            part = int(r["part"]["value"])
        letters = ""
        if "letters" in r:
            letters = r["letters"]["value"]
        terms.append(
            {"term_uri": term_uri,
             "alter_names": alter_names,
             "term_name": r["term_name"]["value"],
             "note": note,
             "source_file_uri": r["nls_source"]["value"],
             "source_file_path": r["source_filepath"]["value"],
             "description": r["description_text"]["value"],
             "edition_uri": r["edition"]["value"],
             "edition_title": r["edition_title"]["value"],
             "edition_number": edition_number,
             "volume_uri": r["volume"]["value"],
             "volume_number": int(r["volume_number"]["value"]),
             "num_pages": int(r["number_pages"]["value"]),
             "letters": letters,
             "part": part,
             "year": int(r["year"]["value"]),
             "start_page_uri": r["page"]["value"],
             "start_page_number": int(r["page_number"]["value"])
             }
        )
    return terms


def create_alter_names_dicts(hto_sparql_wrapper):
    hto_sparql_wrapper.setQuery("""
    PREFIX hto: <https://w3id.org/hto#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    SELECT * WHERE {
        ?term_uri a ?term_type;
            rdfs:label ?alter_name.
        FILTER (?term_type = hto:ArticleTermRecord || ?term_type = hto:TopicTermRecord)
    }
    """
                                )
    hto_sparql_wrapper.setReturnFormat(JSON)
    ret = hto_sparql_wrapper.queryAndConvert()
    term_alter_names = {}
    for r in ret["results"]["bindings"]:
        term_uri = r["term_uri"]["value"]
        alter_name = r["alter_name"]["value"]
        if term_uri in term_alter_names:
            term_alter_names[term_uri].append(alter_name)
        else:
            term_alter_names[term_uri] = [alter_name]

    return term_alter_names


def get_eb_terms_from_editions_with_source_provider(hto_sparql_wrapper, edition_mmsids, source_provider):
    """
    This function queries eb terms from a list of editions with one specific source provider.
    :param hto_sparql_wrapper: sparql wrapper for hto sparql query
    :param edition_mmsids: A list of edition mmsids.
    :param source_provider: People or Organisations who transcribed the EB collection into digital text.
    :return: a list of terms, each term has term_uri, year, edition_uri, edition_title, edition number,
    vol_uri, vol_num, num_pages, letters, part, file_path, start_page, header, term_primary_name,
    term_alter_names, note, description
    """
    collection_name = NLSCollection.EB.value + " Collection"
    mmsid_filter_string = "FILTER (?mmsid = \"" + "\" || ?mmsid = \"".join(edition_mmsids) + "\")"
    terms = []
    terms_alter_names = {}
    if source_provider == Agent.ASH or source_provider == Agent.NCKP:
        terms_alter_names = create_alter_names_dicts(hto_sparql_wrapper)

    query = """
      PREFIX hto: <https://w3id.org/hto#>
      PREFIX prov: <http://www.w3.org/ns/prov#>
      PREFIX foaf: <http://xmlns.com/foaf/0.1/>
      SELECT * WHERE {
        ?term a ?term_type;
            hto:name ?term_name;
            hto:startsAtPage ?page;
            hto:hasOriginalDescription ?description.
        OPTIONAL {?term hto:note ?note}
        FILTER (?term_type = hto:ArticleTermRecord || ?term_type = hto:TopicTermRecord)
        ?description hto:text ?description_text;
            hto:wasExtractedFrom ?source_dataset.
        ?source_dataset prov:wasAttributedTo ?agent;
            prov:value ?source_filepath.
        ?agent foaf:name "%s".
        ?page hto:number ?page_number.
        OPTIONAL {?page hto:header ?header}
        ?volume a hto:Volume;
            hto:number ?volume_number;
            hto:numberOfPages ?number_pages;
            hto:hadMember ?page.
        OPTIONAL {?volume hto:letters ?letters;}
        OPTIONAL {?volume hto:part ?part}
        ?edition a hto:Edition;
            hto:title ?edition_title;
            hto:yearPublished ?year;
            hto:mmsid ?mmsid;
            hto:hadMember ?volume.
        OPTIONAL {?edition hto:number ?edition_number}
        %s
        ?eb_collection a hto:WorkCollection;
            hto:name "%s";
            hto:hadMember ?edition.
    }
    """ % (source_provider.value, mmsid_filter_string, collection_name)
    print(query)
    hto_sparql_wrapper.setQuery(query)
    hto_sparql_wrapper.setReturnFormat(JSON)
    results = hto_sparql_wrapper.query().convert()
    for r in results["results"]["bindings"]:
        note = ""
        if "note" in r and (source_provider == Agent.ASH or source_provider == Agent.NCKP):
            note = r["note"]["value"]
        alter_names = []
        term_uri = r["term"]["value"]
        if term_uri in terms_alter_names:
            alter_names = terms_alter_names[term_uri]
        edition_number = -1
        if "edition_number" in r:
            edition_number = int(r["edition_number"]["value"])
        part = -1
        if "part" in r:
            part = int(r["part"]["value"])
        letters = ""
        if "letters" in r:
            letters = r["letters"]["value"]
        terms.append(
            {"term_uri": term_uri,
             "alter_names": alter_names,
             "term_name": r["term_name"]["value"],
             "note": note,
             "source_file_uri": r["source_dataset"]["value"],
             "source_file_path": r["source_filepath"]["value"],
             "description": r["description_text"]["value"],
             "edition_uri": r["edition"]["value"],
             "edition_title": r["edition_title"]["value"],
             "edition_number": edition_number,
             "volume_uri": r["volume"]["value"],
             "volume_number": int(r["volume_number"]["value"]),
             "num_pages": int(r["number_pages"]["value"]),
             "letters": letters,
             "part": part,
             "year": r["year"]["value"],
             "start_page_uri": r["page"]["value"],
             "start_page_number": int(r["page_number"]["value"])
             }
        )
    return terms


def get_hto_object(sparql_endpoint, collection_name, source):
    """
    This function will query hto kg, and get a list of hto object based on the collection name and source. For EB collection, it will return terms from given source dataset, For other NLS collection, it will return pages from given source datase.
    :param sparql_endpoint: the sparql endpoint for querying hto kg
    :param collection_name: Name of digital collections in hto kg.
    :param source: Three types of sources: NLS, Neuspell and HQ.
    :return: a list of hto object based on the collection name and source.
    """
    hto_sparql_wrapper = SPARQLWrapper(sparql_endpoint)
    results = []
    if collection_name == NLSCollection.EB.value:
        if source == "NLS":
            edition_mmsids = ["992277653804341"  # 1st 1771
                , "9929192893804340"  # 1st 1773
                , "997902523804341"  # 2nd 1778
                , "997902543804341"  # 3rd 1797
                , "9910796343804340"  # 3rd 1801
                , "9910796233804340"  # 4th 1810
                , "9922270543804340"  # 5th 1815
                , "9910796253804340"  # 6th 1823
                , "9910796273804340"  # 7th 1842
                , "9929777383804340"  # 8th 1853
                              ]
            results = get_eb_terms_from_editions_with_source_provider(hto_sparql_wrapper, edition_mmsids,
                                                                      Agent.NLS)
        elif source == "NeuSpell":
            edition_mmsids = ["992277653804341"  # 1st 1771
                , "9929192893804340"  # 1st 1773
                , "997902523804341"  # 2nd 1778
                , "997902543804341"  # 3rd 1797
                , "9910796343804340"  # 3rd 1801
                , "9910796233804340"  # 4th 1810
                , "9922270543804340"  # 5th 1815
                , "9910796253804340"  # 6th 1823
                , "9910796273804340"  # 7th 1842
                , "9929777383804340"  # 8th 1853
                              ]
            results = get_neuspell_corrected_eb_terms_from_editions(hto_sparql_wrapper, edition_mmsids)
        elif source == "HQ":
            ash_edition_mmsid = ["992277653804341"]  # 1st 1771
            nckp_edition_mmsid = ["9910796273804340"]  # 7th 1842
            nls_edition_mmsids = ["9929192893804340"  # 1st 1773
                , "997902523804341"  # 2nd 1778
                , "997902543804341"  # 3rd 1797
                , "9910796343804340"  # 3rd 1801
                , "9910796233804340"  # 4th 1810
                , "9922270543804340"  # 5th 1815
                , "9910796253804340"  # 6th 1823
                , "9929777383804340"  # 8th 1853
                                  ]
            ash_terms = get_eb_terms_from_editions_with_source_provider(hto_sparql_wrapper, ash_edition_mmsid,
                                                                        Agent.ASH)
            nls_terms = get_eb_terms_from_editions_with_source_provider(hto_sparql_wrapper, nls_edition_mmsids,
                                                                        Agent.NLS)
            nckp_terms = get_eb_terms_from_editions_with_source_provider(hto_sparql_wrapper, nckp_edition_mmsid,
                                                                         Agent.NCKP)
            results.extend(ash_terms)
            results.extend(nls_terms)
            results.extend(nckp_terms)
    else:
        if source == "NLS":
            results = get_nls_page_from_nls(hto_sparql_wrapper, collection_name)
    return results
