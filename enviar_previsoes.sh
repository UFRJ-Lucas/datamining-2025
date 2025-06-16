#!/bin/bash

URL="https://barra.cos.ufrj.br:443/datamining/rpc/avalia"

echo "==========\nRESPOSTAS:\n==========\n" > log_previsao.txt

for file in ./previsoes/*.json; do
	echo "Enviando arquivo: $file" >> log_previsao.txt
	curl -s -X POST "$URL" -H 'accept: application/json' -H 'Content-Type: application/json' -d @"$file" >> log_previsao.txt
	echo "\n" >> log_previsao.txt
done

echo "============\nFIM DO ENVIO\n============" >> log_previsao.txt
