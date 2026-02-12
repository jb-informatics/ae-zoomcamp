# Data Engineering Zoomcamp
This is module 4 of the Data Engineering Zoomcamp focused on analytics engineering.

### Install dependencies
```
pip install -r requirements.txt
```

### Ingest NYC Taxi Dataset
This module focuses on yellow and green taxi data (2019-2020).

Available arguments:
--years → One or more years (e.g., 2019 2020)
--months → One or more months (space-separated or comma-separated)
--taxi → Taxi type: yellow or green

To ingest all data from 2019-2020:
```
python pipeline/ingest-data.py
```

To ingest a single month
```
python pipeline/ingest-data.py --years 2019 --months 5 --months 6 --months 7 --taxi yellow
```

To ingest multiple months
```
python pipeline/ingest-data.py --years 2019 --months 5,6,7,8 --taxi yellow
```
