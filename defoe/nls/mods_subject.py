class MODSSubject(object):
    """
    Object model representation of the subject element in MODS (Metadata Object Description Schema,
    see: https://www.loc.gov/standards/mods/userguide/subject.html#mappings)
    """

    def __init__(self, authority=None, displayLabel=None, topics=None, geographics=None, names=None, temporals=None,
                genres=None, hierarchical_geographic=None, occupations=None):
        """
        Initializes a MODSSubject instance.

        Args:
            authority (str, optional): The name of an authoritative list of the subject whose values are controlled, such as lcsh (Library of Congress Subject Headings).
            displayLabel (str, optional): Label of the subject for display purpose only.
            topics (list of str, optional): Topics mentioned in the subject. Defaults to an empty list.
            geographics (list of str, optional): Geographic locations related to the subject. Defaults to an empty list.
            names (list of str, optional): Names or entities associated with the subject. Defaults to an empty list.
            temporals (list of str, optional): Temporal descriptors for the subject. Defaults to an empty list.
            hierarchical_geographic (dict, optional): Hierarchical geographic data. Defaults to an empty dictionary.
        """
        self.authority = authority
        self.displayLabel = displayLabel
        self.topics = topics or []
        self.geographics = geographics or []
        self.names = names or []
        self.temporals = temporals or []
        self.genres = genres or []
        self.hierarchical_geographic = hierarchical_geographic or {}
        self.occupations = occupations or []

    def __repr__(self):
        """
        Returns a string representation of the MODSSubject instance.

        Returns:
            str: A string that describes the MODSSubject instance, including its attributes.
        """
        return (f"MODSSubject(authority={self.authority}, "
                f"displayLabel={self.displayLabel}, "
                f"topics={self.topics}, "
                f"geographics={self.geographics}, "
                f"names={self.names}, "
                f"temporals={self.temporals}, "
                f"genres={self.genres}, "
                f"hierarchical_geographic={self.hierarchical_geographic}, "
                f"occupations={self.occupations})")

    def to_dict(self):
        """
        Converts the MODSSubject instance to a dictionary.

        Returns:
            dict: A dictionary representation of the MODSSubject instance.
        """
        return {
            "authority": self.authority,
            "displayLabel": self.displayLabel,
            "topics": self.topics,
            "geographics": self.geographics,
            "names": self.names,
            "temporals": self.temporals,
            "genres": self.genres,
            "hierarchical_geographic": self.hierarchical_geographic,
            "occupations": self.occupations
        }

    @staticmethod
    def from_xml_element(subject_element, namespaces):
        """
        Factory method to create a MODSSubject instance from an XML element.

        Args:
            subject_element (etree.Element): An XML element representing a MODS subject.
            namespaces (dict): A dictionary mapping namespace prefixes to URIs for XML parsing.

        Returns:
            MODSSubject: An instance of MODSSubject populated with data from the XML element.
        """
        authority = subject_element.get('authority')
        displayLabel = subject_element.get('displayLabel')
        topics = [str(topic) for topic in subject_element.xpath('mods:topic/text()', namespaces=namespaces)]
        geographics = [str(geographic) for geographic in subject_element.xpath('mods:geographic/text()', namespaces=namespaces)]
        genres = [str(genre) for genre in subject_element.xpath('mods:genre/text()', namespaces=namespaces)]
        names = [str(name) for name in subject_element.xpath('mods:name/mods:namePart/text()', namespaces=namespaces)]
        temporals = [str(temporal) for temporal in subject_element.xpath('mods:temporal/text()', namespaces=namespaces)]
        occupations = [str(occupation) for occupation in subject_element.xpath('mods:occupation/text()', namespaces=namespaces)]

        # Parse hierarchical geographic data
        hierarchical_geographic_element = subject_element.xpath('mods:hierarchicalGeographic', namespaces=namespaces)
        hierarchical_geographic = {}
        if hierarchical_geographic_element:
            geo_fields = ['continent', 'country', 'region', 'state', 'city', 'territory', 'county', 'island', 'area']
            for field in geo_fields:
                values = hierarchical_geographic_element[0].xpath(f'mods:{field}/text()', namespaces=namespaces)
                if values:
                    hierarchical_geographic[field] = [str(value) for value in values]

        return MODSSubject(authority, displayLabel, topics, geographics, names, temporals, genres, hierarchical_geographic, occupations)



