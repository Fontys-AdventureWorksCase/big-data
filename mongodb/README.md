# Run
`docker-compose up -d`

# Access
Open browser, navigate to localhost:8081

# Import
`mongoimport --uri "mongodb://root:example@mongo:27017" --authenticationDatabase admin -d testdb -c covid-country-latest --type CSV --file country_wise_latest.csv --headerline`
run above to import csv file from covid dataset
