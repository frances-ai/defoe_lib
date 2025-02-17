# Get preprocessed concordance (also called details) of articles in which we have keywords or keysentences, filtering those by target words and group by year 

* Both keywords/keysentences and words in documents are cleaned (long-S and hyphen fixes) and preprocessed according to the configuration file
* Query module: `defoe.papers.queries.target_keysearch_by_year_preprocessed_details`
* Configuration file:
  - File with keywords or keysentece (data)
  - number of target words from the file with keywords/keysentences. **Note: It asumes, that the X first words/sentences of that file are the target words**
  - in which position the lexicon start. If we use 0, it means that the target words are also including in the lexicon.
  - defoe path (defoe_path)
  - operating system (os) 
  - preprocessing treatment (preprocess)
  - Examples:
     - preprocess: normalize
     - data: music.txt
     - num_target: 1
     - lexicon_start: 0
     - defoe_path: /lustre/home/sc048/rosaf4/defoe/
     - os_type: linux
* Result format:

```
          <YEAR>:
          [
            [- article_id: 
             - authors:
             - filename:
             - issue_id:
             - page_ids:
             - preprocessed text:
             - term
             - title ]
            ...
          ],
          <YEAR>:
          ...
```

**Note:** Use this query if you have target word(s) and you want to store the preprocessed text. Otherwise, you could use `defoe.papers.queries.keysearch_by_year_details` or `defoe.papers.queries.target_keysearch_by_year_details`. 
