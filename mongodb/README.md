# Run
`docker-compose up -d`

# Access
Open browser, navigate to localhost:8081

# Import
`mongoimport --uri "mongodb://root:example@mongo:27017" --authenticationDatabase admin -d testdb -c covid-country-latest --type CSV --file country_wise_latest.csv --headerline`
run above to import csv file from covid dataset

Run the complete set:
```
root@16fbe0561aae:/data/covid# mongoimport --uri "mongodb://root:example@mongo:27017" --authenticationDatabase admin -d testdb -c covid_19_clean_complete --type CSV --file covid_19_clean_complete.csv --headerline
2021-05-26T14:25:11.586+0000	connected to: mongodb://[**REDACTED**]@mongo:27017
2021-05-26T14:25:12.349+0000	49068 document(s) imported successfully. 0 document(s) failed to import.
root@16fbe0561aae:/data/covid# mongoimport --uri "mongodb://root:example@mongo:27017" --authenticationDatabase admin -d testdb -c day_wise --type CSV --file day_wise.csv --headerline
2021-05-26T14:25:23.350+0000	connected to: mongodb://[**REDACTED**]@mongo:27017
2021-05-26T14:25:23.382+0000	188 document(s) imported successfully. 0 document(s) failed to import.
root@16fbe0561aae:/data/covid# mongoimport --uri "mongodb://root:example@mongo:27017" --authenticationDatabase admin -d testdb -c full_grouped --type CSV --file full_grouped.csv --headerline
2021-05-26T14:25:34.907+0000	connected to: mongodb://[**REDACTED**]@mongo:27017
2021-05-26T14:25:35.440+0000	35156 document(s) imported successfully. 0 document(s) failed to import.
root@16fbe0561aae:/data/covid# mongoimport --uri "mongodb://root:example@mongo:27017" --authenticationDatabase admin -d testdb -c usa_county_wise --type CSV --file usa_county_wise.csv --headerline
2021-05-26T14:25:45.582+0000	connected to: mongodb://[**REDACTED**]@mongo:27017
2021-05-26T14:25:48.582+0000	[######..................] testdb.usa_county_wise	17.6MB/66.6MB (26.4%)
2021-05-26T14:25:51.582+0000	[############............] testdb.usa_county_wise	35.2MB/66.6MB (52.8%)
2021-05-26T14:25:54.582+0000	[###################.....] testdb.usa_county_wise	53.1MB/66.6MB (79.7%)
2021-05-26T14:25:56.836+0000	[########################] testdb.usa_county_wise	66.6MB/66.6MB (100.0%)
2021-05-26T14:25:56.836+0000	627920 document(s) imported successfully. 0 document(s) failed to import.
root@16fbe0561aae:/data/covid# mongoimport --uri "mongodb://root:example@mongo:27017" --authenticationDatabase admin -d testdb -c worldometer_data --type CSV --file worldometer_data.csv --headerline
2021-05-26T14:26:05.707+0000	connected to: mongodb://[**REDACTED**]@mongo:27017
2021-05-26T14:26:05.732+0000	209 document(s) imported successfully. 0 document(s) failed to import.
```
It are about 700.000 records
