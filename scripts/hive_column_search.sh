#!/bin/bash

set -u #check that all the shell parameters/variables are initialized.

SchemaName=$1
ColumnName=$2

echo '' > column_result.txt

prodhive -S -e 'show tables in '${SchemaName} > table_list.txt

while read TableName; do
  echo '---- '${TableName}' ----' | tee -a column_result.txt
  prodhive -S -e 'use '${SchemaName}'; describe '${TableName}';' > column_list.txt

  while read ColumnList; do
    echo '**** '${ColumnList} | grep ${ColumnName} | tee -a column_result.txt
  done < column_list.txt
  
done < table_list.txt

rm table_list.txt
rm column_list.txt

