<img src="logo.png" alt="drawing" width="500"/>

## Python Data Engineering Challenge

Our system ingests search term data from Google Ads API into a PostgreSQL database, via an AWS S3 Data Lake.

Once ingested we score each search term with its Return On Ad Spend (ROAS).

```text
ROAS = conversion value / cost
```

### Task

Three CSVs have been given - campaigns.csv, adgroups.csv and search_terms.csv. 

First ingest these 3 CSVs into a database, ensure the data ingestion is idempotent. 

Secondly, the adgroup alias is in the format:

`Shift - Shopping - <country> - <campaign structure value> - <priority> - <random string> - <hash>`

We sometimes need to know the ROAS aggregated by `country` and/or by `priority`. 

Build something to allow for those aggregations to be queried easily.

### Submission

We really value neatness and things being put in place to aid local development and continuous integration.

Please fork this repo to complete the challenge.

Good luck we are rooting for you, show us what you can do!
