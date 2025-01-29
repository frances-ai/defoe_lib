# "Defoe" - analysis of historical books and newspapers data

This repository contains code to analyse historical books and newspapers datasets using Apache Spark.
It is an improved version of [defoe](https://github.com/defoe-code/defoe)

---

[📖 Defoe Tutorial & Hands-On Installation](https://eur02.safelinks.protection.outlook.com/?url=https%3A%2F%2Fcolab.research.google.com%2Fdrive%2F1v244q6Y024EKjPFbC3tEMLPrmnOLtdq4%3Fusp%3Dsharing&data=05%7C02%7C%7Cf765665efcd348614e5a08dd409c7a7a%7C2e9f06b016694589878910a06934dc61%7C0%7C0%7C638737762981075444%7CUnknown%7CTWFpbGZsb3d8eyJFbXB0eU1hcGkiOnRydWUsIlYiOiIwLjAuMDAwMCIsIlAiOiJXaW4zMiIsIkFOIjoiTWFpbCIsIldUIjoyfQ%3D%3D%7C0%7C%7C%7C&sdata=P00abW5cb92Z8wNSvrl%2BHpaOD%2FWq0hhzcRDQKi7z%2Flo%3D&reserved=0)

## Supported datasets

Defoe supports already several datasets. In order to query a daset, defoe needs a list of files and/or directories that conform the dataset. Many of those files (used so far), can be found under the [others](./others) directory. Those files would need to be modifed, in order to update them with the corresponding paths. 
  

### British Library Books

This dataset consists of ~1TB of digitised versions of ~68,000 books from the 16th to the 19th centuries. The books have been scanned into a collection of XML documents. Each book has one XML document one per page plus one XML document for metadata about the book as a whole. The XML documents for each book are held within a compressed, ZIP, file. Each ZIP file holds the XML documents for a single book (the exception is 1880-1889's 000000037_0_1-42pgs__944211_dat.zip which wholds the XML documents for 2 books). These ZIP files occupy ~224GB.

This dataset is available under an open, public domain, licence. See [Datasets for content mining](https://www.bl.uk/collection-guides/datasets-for-content-mining) and [BL Labs Flickr Data: Book data and tag history (Dec 2013 - Dec 2014)](https://figshare.com/articles/BL_Labs_Flickr_Data/1269249). For links to the data itself, see [Digitised Books largely from the 19th Century](https://data.bl.uk/digbks/). The data is provided by [Gale](https://www.gale.com), a division of [CENGAGE](https://www.cengage.com/).

### British Library Newspapers

This dataset consists of ~1TB of digitised versions of newspapers from the 18th to the early 20th century. Each newspaper has an associated folder of XML documents where each XML document corresponds to a single issue of the newspaper. Each XML document conforms to a British Library-specific XML schema.

This dataset is available, under licence, from [Gale](https://www.gale.com), a division of [CENGAGE](https://www.cengage.com/). The dataset is in 5 parts e.g. [Part I: 1800-1900](https://www.gale.com/uk/c/british-library-newspapers-part-i). For links to all 5 parts, see [British Library Newspapers](https://www.gale.com/uk/s?query=british+library+newspapers). 

### Times Digital Archive

The code can also handle the [Times Digital Archive](https://www.gale.com/uk/c/the-times-digital-archive) (TDA). 

This dataset is available, under licence, from [Gale](https://www.gale.com), a division of [CENGAGE](https://www.cengage.com/).

The code was used with papers from 1785-2009.

### Find My Past Newspapers

This dataset is available, under licence, from [Find My Past](https://www.findmypast.co.uk/). To run queries with this dataset we can chose either to use:
* ALTO model: for running queries at page level. These are the same queries for the BL books.
* FMP model: for running queries at article level.  

### Papers Past New Zealand and Pacific newspapers

[Papers Past](http://paperspast.natlib.govt.nz/) provide digitised [New Zealand and Pacific newspapers](http://paperspast.natlib.govt.nz/newspapers) from the 19th and 20th centuries.

Data can be accessed via API calls which return search results in the form of XML documents. Each XML document holds one or more articles.

This dataset is available, under licence, from [Papers Past](http://paperspast.natlib.govt.nz).


### National Library of Scotland (NLS) digital collections 

[National Library of Scotland](https://data.nls.uk/data/digitised-collections/) provide several digitised collections, such as:
- [Encyclopaedia Britanica](https://data.nls.uk/data/digitised-collections/encyclopaedia-britannica/) from the 18th and 20th centuries.
- [ChapBooks](https://data.nls.uk/data/digitised-collections/chapbooks-printed-in-scotland/)
- [Ladies' Edinburgh Debating Society](https://data.nls.uk/data/digitised-collections/edinburgh-ladies-debating-society/)
- [Scottish Gazetteers](https://data.nls.uk/data/digitised-collections/gazetteers-of-scotland/)

**Note, that ALL collections offered by NLS use the same XML and METS format. Therefore, we can use the defoe NLS model to query any of those collections.**


See [copyrights restrictions](https://www.nls.uk/copyright)



---

# Get started

Set up (local):

* [Set up local environment](./docs/setup-local.md)

Set up (Urika):

* [Set up Urika environment](./docs/setup-urika.md)
* [Import data into Urika](./docs/import-data-urika.md)
* [Import British Library Books and Newspapers data into Urika](./docs/import-data-urika-ati-se-humanities-uoe.md) (Alan Turing Institute-Scottish Enterprise Data Engineering Program University of Edinburgh project members only)

Set up (Cirrus - HPC Cluster):
* [Installing defoe, Spark, and transferring data](https://github.com/defoe-code/CDCS_Text_Mining_Lab/blob/master/README.md)

Set up (VM):

* [Set up a VM, with defoe, Spark, Hadoop, Edinburgh Geoparser](./docs/setup-VM.md)

Run queries:

* [Specify data to query](./docs/specify-data-to-query.md)
* [Specify Azure data to query](./docs/specify-data-to-query-azure.md)
* [Run individual queries](./docs/run-queries.md)
* [Run multiple queries at once - just one ingestion](./docs/run-list-of-queries.md)
* [Extracting, Transforming and Saving RDD objects to HDFS as a dataframe](./docs/nls_demo_examples/nls_demo_individual_queries.md#writing-and-reading-data-fromto-hdfs)
* [Loading dataframe from HDFS and performing a query](./docs/nls_demo_examples/nls_demo_individual_queries.md#writing-and-reading-data-fromto-hdfs)
* [Extracting, Transforming and Saving RDD objects to PostgreSQL database](./docs/nls_demo_examples/nls_demo_individual_queries.md#writing-and-reading-data-fromto-postgresql-database)
* [Loading dataframe from PostgreSQL database and performing a query](./docs/nls_demo_examples/nls_demo_individual_queries.md#writing-and-reading-data-fromto-postgresql-database)
* [Extracting, Transforming and Saving RDD objects to ElastiSearch](./docs/nls_demo_examples/nls_demo_individual_queries.md#writing-and-reading-data-tofrom-elasticsearch-es)
* [Loading dataframe from ElasticSearch and performing a query](./docs/nls_demo_examples/nls_demo_individual_queries.md#writing-and-reading-data-tofrom-elasticsearch-es)

Available queries:

* [ALTO documents](./docs/alto/index.md) (British Library Books and Find My Past Newspapers (at page level))
* [British Library Newspapers](./docs/papers/index.md) (these can also be run on the Times Digital Archive)
* [FMP newspapers](./docs/fmp/index.md) (Find My Past Newspapers datasets at article level)
* [Papers Past New Zealand and Pacific newspapers](./docs/nzpp/index.md)
* [Generic XML document queries](./docs/generic_xml/index.md) (these can be run on arbitrary XML documents)
* [NLS queries](./docs/nls/index.md) (these can be run on the Encyclopaedia Britannica, Scottish Gazetteers or ChapBooks datasets)
* [HDFS queries](./docs/hdfs/index.md) (running queries against HDFS files - for interoperability across models)
* [ES queries](./docs/es/index.md) (running queries against ES - for interoperability across models)
* [PostgreSQL queries](/docs/psql/index.md) (running queries against PostgreSQL database - for interoperability across models)
* [NLSArticles query](./docs/nlsArticles/index.md) (just for extracting automatically articles from the Encyclopaedia Britannica dataset)


Developers:

* [Run unit tests](./docs/tests.md)
* [Design and implementation notes](./docs/design-implementation.md)

---

## Name

The code is called "defoe" after [Daniel Defoe](https://en.wikipedia.org/wiki/Daniel_Defoe), writer, journalist and pamphleteer of the 17-18 century.
