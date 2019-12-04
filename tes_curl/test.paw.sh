## assignment 测试location
curl -X GET "http://localhost:9200/legal_idx/cases/_search?pretty&q=location:New%20South%20Walse"

## assignment 测试person
curl -X GET "http://localhost:9200/legal_idx/cases/_search?pretty&q=person:John"

## assignment 普通测试
curl -X GET "http://localhost:9200/legal_idx/cases/_search?pretty&q=(criminal%20AND%20law)"

curl -X GET "http://localhost:9200/legal_idx/cases/_search?pretty&q=organization:Arts"
