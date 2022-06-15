touch queries/config_file.yml

# Install miniconda latest version, for Python 3
bash requirements.sh

docker run -d --name fuseki -p 3030:3030 -e JVM_ARGS=-Xmx2g stain/jena-fusek
# get randomly generated admin password from log
# download total_eb.ttl
# manage datasets -> upload eb_total.ttl

zip -r defoe.zip defoe
spark-submit --driver-memory 12g --py-files defoe.zip defoe/run_query.py sparql_data.txt sparql defoe.sparql.queries.publication_normalized   -r  results.yml  -n 34
