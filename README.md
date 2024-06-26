# my_assign

## SDK Used
[NDC Typescript SDK](https://github.com/hasura/ndc-sdk-typescript)

## DBMS used
MySQL

## Getting Started

```sh
npm i
npm run build
npx tsc
```

Run the connector:

```sh
node dist/index.js serve --configuration .
```

Queries supported as of now:
(Please refer to `snapshots` folder for sample queries and to understand syntax.)

1. Aggregates:
    (GROUP BY not supported yet, hence kindly do not make combined request for fields and aggregate functions.)
    1. sum
    2. min
    3. max
    4. stddev_pop
    5. stddev_samp
    6. var_pop
    7. var_samp
    8. csv (GROUP_CONCAT)
    9. avg
    10. COUNT(*)
    11. COUNT(\<column_name\>) (DISTINCT or not)
2. Limit
3. Offset
4. Where:
    1. "and":   AND
    2. "or" :   OR
    3. Comparison Operators (Only for Column `compared to` Scalar):
        1. "_lte" : <=
        2. "_lt" : <
        3. "_gte" : >=
        4. "_gt" : >
        5. "_eq" : =
        6. "_like" : LIKE
        6. "_contains" : CONTAINS (LIKE %\<expr\>%)
        7. "_in" : IN
5. Order By (Multilevel Order By supported)
6. You can connect two or more tables by using the 'relationship' type in fields.