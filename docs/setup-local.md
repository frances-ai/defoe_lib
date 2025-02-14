# Set up local environment

## Requirements
* Java 8
* Python3.9

---

## Get source code repository

Run:

```bash
git clone https://github.com/frances-ai/defoe_lib
```

This will clone the source code repository into a `defoe_lib` directory.

---

## Setup environment

### Install Java

See instructions here: [Java install](https://www.java.com/en/download/help/download_options.html)

### Install Python3.9

See instruction here: [python3.9 install](https://www.python.org/downloads/)

For Apple M1 chips machine, export a property:
```bash
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
```

### Install dependencies
In the `defoe_lib` directory, run
```bash
pip install -r requirements.txt
```

## Running defoe queries

There are three ways to run defoe query based on the use scenario:
1. **run defoe query in command line**: as the original way to run defoe, this works for most of the defoe models (such as `nls`, `alto`, `sparql`). 
You should always use this method unless the model you are using can be accessed through other methods below. 

2. **direct use of defoe_service module**: when you only need to run queries inside this repository, then you just need the `defoe_service.py`. Note that, this only works for `hto` and `sparql` model. Here is an example:
    ```python
    from services.defoe_service import submit_job
    
    if __name__ == "__main__":
        model_name = 'hto'
        endpoint = 'http://query.frances-ai.com/hto'
        query_name = 'frequency_distribution'
        query_config = {'collection': 'Encyclopaedia Britannica','level': "edition",'source': 'NLS'}
        result_file_path = "./hto_eb_edition_nls_freqDist.yml"
        job_id = 'hto_eb_nls_freq_dist'
        submit_job(job_id, model_name, query_name, endpoint, query_config, result_file_path)
    ```
3. **use defoe grpc server across applications or platform**: if you want to run defoe query from another local application 
(especially, when using different programming language, or in different local or remote machines), then you need to start defoe grpc 
server to run defoe query request through network. Note that, this only works for hto and sparql model. using the following command in the `defoe_lib` directory
    ```bash
    python start_defoe_grpc_server.py
    ```

### Run defoe query in command line

In your terminal, navigate to this folder: `defoe_lib`. Zip the defoe folder using the following command:
```bash
zip -r defoe.zip defoe
```

If you need to run any geoparsing related query, please download an extra lib into `defoe/geoparser-1.3` with following command:
```bash
cd defoe/geoparser-1.3
wget https://storage.googleapis.com/damon_public_files/dataframes/
unzip lib.zip
```

Check the query file you want to run, see if you need data from configuration file. If you do, create `config.yml` file.

Run the query in command line in the following format:
```bash
spark-submit --py-files defoe.zip defoe/run_query.py <data_file_path> <model_name> <query_name>q -r <result_filepath> -n <num_cores> -e <error_filepath>
```

**Example: extract [Chapbooks printed in Scotland](https://data.nls.uk/data/digitised-collections/chapbooks-printed-in-scotland/) dataset**

This example shows how to extract the textual content of each page of chapbooks printed in scotland along with their metadata.
1. Download the full dataset of this collection [here](https://data.nls.uk/data/digitised-collections/chapbooks-printed-in-scotland/)
2. Create a text file `chapbooks_filepaths.txt` which list the filepaths of all downloaded chapbooks.
This can be done using provided `defoe_lib/examples/nls_data_file_helper.py`. Edit this python file:
```python
....
    data_mapping_file_path = str(current_path) + '/chapbooks_filepaths.txt'
    data_file_path = '<path where you have chapbooks collection downloaded>/nls-data-chapbooks'
....
```
3. Start defoe query `write_metadata_pages_yml` from `nls` model:
```bash
spark-submit --py-files defoe.zip defoe/run_query.py examples/chapbooks_filepaths.txt nls defoe.nls.queries.write_metadata_pages_yml -r results_chapbooks.yml -n 16
```


### Direct use of defoe_service module

To use `defoe_service`, you simply import it in your python file inside this `defoe_lib` folder. 
The following example can be found in the `defoe_lib/examples/defoe_service_example`:
```python
from defoe_service import submit_job

if __name__ == "__main__":
    model_name = 'hto'
    endpoint = 'http://query.frances-ai.com/hto'
    query_name = 'frequency_distribution'
    query_config = {'collection': 'Encyclopaedia Britannica', 'level': "edition", 'source': 'NLS'}
    result_file_path = "hto_eb_edition_nls_freqDist.yml"
    job_id = 'hto_eb_nls_freq_dist'
    submit_job(job_id, model_name, query_name, endpoint, query_config, result_file_path)
```
Run this example using the following command in `defoe_lib`:
```bash
python -m examples.defoe_service_example
```



### Use defoe grpc server across applications or platform

First, you need to start the grpc server using the following command:
```bash
python start_defoe_grpc_server.py
```

Based on the programming language the client application uses, generation of required code files for client app differs.
Check the [grpc official documentation](https://grpc.io/docs/languages/). For python, you can just copy `defoe_pb2.py`, 
`defoe_pb2.pyi` and `defoe_pb2_grpc.py` in your client application. You can find how defoe grpc server is used in 
[frances-api](https://github.com/frances-ai/frances-api/blob/main/web_app/query_app/service/defoe_service/local_defoe_service.py)





### Common issues
1. If you get 'no buffer storage' error, try to increase the memory using the following command:
    ```bash
    ulimit -n 65535
    ```
