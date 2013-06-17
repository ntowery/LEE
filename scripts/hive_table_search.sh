#!/bin/bash

set -u #check that all the shell parameters/variables are initialized.

TableName=$1

echo '' > table_result_${TableName}.txt

prodhive -S -e 'show schemas' > schema_list_${TableName}.txt

while read SchemaName; do
  echo '---- '${SchemaName}' ----' | tee -a table_result_${TableName}.txt
  prodhive -S -e 'use '${SchemaName}'; show tables;' > table_list_${TableName}.txt

  while read TableList; do
    echo '**** '${TableList} | grep ${TableName} | tee -a table_result_${TableName}.txt
  done < table_list_${TableName}.txt
  
done < schema_list_${TableName}.txt

rm schema_list_${TableName}.txt
rm table_list_${TableName}.txt

