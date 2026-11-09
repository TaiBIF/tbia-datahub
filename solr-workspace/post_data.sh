cd /var/solr/csvs/export
post -c tbia_records -commit no *.csv
post -c tbia_records -d '<commit/>'