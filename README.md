# PerfiDB

PerfiDB is a SQL database engineered specifically to store and manage personal finance data. The main features include:

- A simple yet powerful labelling system. (Think about Gmail labels for your bank transactions)
- Intuitive and concise SQL statements to manage your money


# Examples
```sql
-- Import transactions to account 'amex' from a csv file
COPY amex FROM 'bank-exports/2022-03.csv';

SELECT * FROM amex;

-- Show all transactions labelled with 'grocery'.
SELECT * FROM db WHERE label = 'grocery';

-- Show transactions from amex account labelled with 'grocery'.
SELECT * FROM amex WHERE label = 'grocery';

-- Show transactions from the month July.
SELECT * FROM db WHERE date = 7;
```

# How to use PerfiDB
A common use case is to export transactions from your banks and run SQL `COPY` statement to load them into PerfiDB.

### Import transactions
To import transactions from a csv file into account _amex_gold_
```sql
COPY amex_gold FROM 'bank-exports/2022-03.csv';
```

Note: Because hyphen `-` is interpreted as 'minus' in SQL, if you want to use `-` in account name you need to surround account name by single quotation marks, e.g. `'amex-gold'`.

To print out records from csv file without actually saving to database, specify dry-run:
```sql
COPY amex_gold FROM 'bank-exports/2022-03.csv' WITH (FORMAT dryrun)
```