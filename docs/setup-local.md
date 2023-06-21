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


## Start the grpc server
In the `defoe_lib` directory, run
```bash
python start_defoe_grpc_server.py
```