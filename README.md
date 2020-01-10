# centos_airflow

- CentOS環境でairflowをテストするためのリポジトリ

## structure

```
sasakikensuke@MBP-N5UME4JR $ tree
.
├── README.md
├── airflow
│   ├── airflow-webserver.pid
│   ├── airflow.cfg
│   ├── airflow.db
│   ├── dags
│   │   ├── bash_tutorial.py
│   │   ├── cleaning_bycity_sample.py
│   │   ├── tutorial.py
│   │   └── tutorial_python.py
│   ├── data
│   │   └── input.csv
│   ├── logs
│   │   ├── bash_tutorial
│   │   ├── csv-etl-sample
│   │   ├── dag_processor_manager
│   │   ├── scheduler
│   │   ├── tutorial_print_task
│   │   └── tutorial_python
│   ├── output
│   └── unittests.cfg
└── other
```

## Usage

0. setup databases

```
airflow initdb
```

1. setup webserver and scheduler

```
airflow webserver -p 8080
```

```
airflow scheduler
```

2. run DAGs from browser


## easy sample

- cityごとに抽出 → columnの値を合計 → dbに格納するサンプル
`./airflow/dags/cleaning_bycity_sample.py`