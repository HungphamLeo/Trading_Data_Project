├── .dockerignore

├── .github

│   └── workflows

├── .gitignore

├── Makefile

├── README.md

├── infra

│   ├── docker

│   │   ├── ingestion

│   │   │   └── Dockerfile

│   │   └── processing

│   │       └── dbt_processing

│   │           └── Dockerfile

│   └── docker_compose

│       ├── core.yml

│       └── processing.yaml

├── logger_storage

├── platforms

│   ├── api_gateway

│   ├── ingestion

│   │   └── crypto

│   │       └── binance

│   │           ├── extract

│   │           │   ├── __pycache__

│   │           │   │   └── fetch_binance_klines.cpython-312.pyc

│   │           │   └── fetch_binance_klines.py

│   │           ├── parse

│   │           └── producer

│   ├── processing

│   │   └── dbt

│   │       ├── config

│   │       │   ├── dbt_project.yaml

│   │       │   └── profiles.yaml

│   │       └── models

│   │           └── crypto

│   │               ├── dto

│   │               │   └── binance_dto.py

│   │               ├── int

│   │               │   ├── int_kyc_history.sql

│   │               │   └── int_transactions_enriched.sql

│   │               ├── marts

│   │               │   ├── marts_core__dim_user.sql

│   │               │   └── marts_core__fact_transactions.sql

│   │               ├── snapshots

│   │               │   └── user_kyc_snapshots.sql

│   │               ├── staging

│   │               │   ├── schema.yml

│   │               │   ├── stg_rates.sql

│   │               │   ├── stg_transactions.sql

│   │               │   └── stg_users.sql

│   │               └── tests

│   ├── storage

│   │   ├── cache

│   │   │   └── redis

│   │   └── warehouse

│   │       ├── base_storage.py

│   │       └── postgresql

│   │           ├── _init_postgresql.py

│   │           └── crypto

│   │               ├── bronze_crypto

│   │               │   └── _init_tables.sql

│   │               ├── gold_crypto

│   │               └── silver_crypto

│   └── streaming

├── project_tree.txt

├── requirements_dbt.txt

├── requirements_ingestion.txt

├── scripts

│   ├── README.md

│   ├── run_binance_ingestion.py

│   ├── sample_bi_queries.sql

│   └── test.ipynb

├── services

│   ├── test

│   └── trading

├── shared

│   ├── common_models

│   │   └── crypto

│   ├── config

│   │   ├── config_loader.py

│   │   ├── envs

│   │   ├── schema

│   │   └── services

│   │       └── binance_services.yaml

│   ├── logger

│   │   └── python_logger.py

│   └── utils

│       ├── files

│       │   └── binance_setup.py

│       └── networks

└── temp_data_storage

    ├── transactions.xlsx

    └── users.xlsx
```

Phần này gửi tôi bản copy vào file README mà ko làm vỡ

Trước mắt thì do thời gian công việc chính bận rộn, không đủ thời gian để làm công tác test hết và fix hẵn,  nên trước mắt bản thân em sẽ trình bày các ý chính cũng như những gì mình muốn làm với các câu hỏi trong để tuyển dụng:

Đầu tiên là việc lựa chọn kiến trúc thì em muốn tách các tầng layer khi mà triển khai 1 project không khi là trong hiện tại mà còn trong tương lai, do đó thì phần 
infra sẽ là nơi chứa các folder và Dockerfile; docker compose dùng để tiến hành chạy build docker để có thể tiến hành set up môi trường, lấy các module cần cài đặt để trong các file requirements.
sau đó đến phần platforms thì sẽ chứa các modulelớn như phần api_gateway nếu sau này muốn xây dựng các function để đặt lệnh, get account balance, check lãi lỗi của lệnh, lượng volumns buy/sell, toàn sàn thông qua API từ trên giao diện/postman; ingresion thì sẽ bao gồm các module business scope như crypto thì trong crypto thì sẽ có binance, bybit, kucoin, okx,... hay tiến hành craw web để lấy dữ liệu công ty kinh doanh, dữ liệu ngành, hoặc lấy dữ liệu vĩ mô trên FED,.. tựu chung tầng này sẽ là nơi xác định business scope ; phần porcessing thì sẽ là phần để triển khai các data plaform để chạy etl, có thể là prefect, pysppark,  dbt,... bên trong các data platform thì sẽ gồm các business scope gồm phần models và phần config; storage sẽ là phần tập trung cácfucntion triển khai, các chức năng tiến hành các database như mongodb (nếu làm datalake, nếu trong trường hợp cần schema linh hoạt), postgresql nếu là dw (mình nghĩ nên dùng postgres vì nó hiện tại rất ổn khi có đồng thời cả phần sql lẫn nosql, còn không thì dùng oracle vì db này không gây ra hiện tường leo thang lock), redis nếu trong trường hợp xác định cache để set và get dữ liệu real -time được cập nhật từ cây nến gần nhất thông qua socket.
Phần services sẽ là phần buil với mục đích sữ dụng source dữ liệu để làm gì ? Làm trading, làm backtest, Làm mm hay chỉ là làm powerbi report => phần sử dụng dữ liệu cho mục đích của doanh nghiệp
Phần shared gồm các model xài hcung, uitls, confdig, logger, phần load loger, ....
Mô hình mình làm db sẽ là start schema => đơn giản nhất thay vì dùng mô hình như snowflake, phức tạp,.

*Note: các thông tin trên db, password, username thì bình thường sẽ dc lưu trong các file encrypt, yaml, or json,.. và được điền vào file gitignore để không push lên trên git, nhưng do tính chất dự án là bài tập nên ko cần thiết.




