# Stores each Page as string to HDFS (as a CSV file) with some metadata associated with each document

* Documents are cleaned (long-S and hyphen fixes) and also preprocessed using all the treatments that we have.
* Pages are saved as cleaned, as well as the other preprocessed method, in different columns, along with some metadata
* Query module: `defoe.nls.queries.write_pages_df_es`
* Configuration file:
  - defoe path (defoe_path)
  - operating system (os)
  - Examples:
      - defoe_path: /home/rosa_filgueira_vicente/defoe/
      - os_type: linux
* Result format:

```
Row("title",  "edition", "year", "place", "archive_filename",  "source_text_filename", 
"text_unit", "text_unit_id", "num_text_unit", "type_archive", "model", "source_text_raw", 
"source_text_clean", "source_text_norm", "source_text_lemmatize", "source_text_stem","num_words")
```
