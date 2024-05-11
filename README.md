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

1. Limit
2. Offset
3. Where:
    1. "and" : AND
    2. Comparison Operators (Only for Column `compared to` Scalar):
        1. "_lte" : <=
        2. "_eq" : =
        3. "_like" : LIKE
        4. "_in" : IN
4. Order By (Multilevel Order By supported)
5. 