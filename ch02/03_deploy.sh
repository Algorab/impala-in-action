#!/bin/bash

export DB=${DB-tpcds}
IMPALA_HOST=${IMPALA_HOST-$(hostname)}
TPCDS_OUT="data"
HDFS_DIR="/user/$USER/tpcds"
TABLES="store store_sales inventory time_dim household_demographics item customer_address customer_demographics date_dim warehouse customer store_returns"

# Copy Tables
for table in $TABLES
do
	echo "Copying $table..."
	hadoop fs -mkdir -p $HDFS_DIR/$table
	hadoop fs -put $TPCDS_OUT/$table*.dat $HDFS_DIR/$table
done

# Create DDLs
for table in $TABLES
do
	export LOCATION="$HDFS_DIR/$table"
	sed -e 's/${env:DB}/tpcds/' -e "s#\${env:LOCATION}#$LOCATION#" -e 's/${DB}/tpcds/' -e "s#\${LOCATION}#$LOCATION#"  ddls/$table.sql > "ddls/static_$table.sql"
	impala-shell -i $IMPALA_HOST -f ddls/static_$table.sql
done
