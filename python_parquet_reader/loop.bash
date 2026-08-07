#!/bin bash


list="mp dc ib pb is ta cb ha of ie if up ci d ra t e ch"


for t in $list; 
do
printf "#table = ${t} \n"
echo curl -X POST "http://localhost:10000/storage_service/retrain_dict?keyspace=moloco_zdic&cf=table_${t}"
done

for t in $list; 
do
echo truncate moloco_zdic.table_${t}\;
done
