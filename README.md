# PerfiDB

PerfiDB is a SQL database engineered specifically to store and manage personal finance data. The main features include:

- A simple yet powerful labelling system. (Think about Gmail labels for your bank transactions)
- Intuitive and concise SQL statements to manage your money


# Examples
```sql
-- Import transactions to account 'amex' from a csv file
COPY amex FROM 'bank-exports/2022-03.csv';

-- List all transactions
SELECT * FROM db;

-- List transactions from account 'amex'
SELECT * FROM amex;

-- Add two labels (grocery, bread) to all transactions in July containing description text 'bakehouse'
UPDATE db SET label = 'grocery, bread' WHERE date = '2022-07' AND description LIKE 'bakehouse';

-- List all transactions labelled with 'grocery'.
SELECT * FROM db WHERE label = 'grocery';
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

### Query

#### From all accounts
```sql
SELECT * FROM db;
```

#### From specific account
```sql
SELECT * FROM bank_1;
```

#### Filters
##### Dates
```sql
-- Filter by month, i.e. 7 means July. If current date has passed July it means July of current year,
-- if current date is before end of July it means July of previous year.
SELECT * FROM db WHERE date = 7;

-- Filter by month
SELECT * FROM db WHERE date = '2022-07';

-- Filter by date
SELECT * FROM db WHERE date = '2022-07-31';
```

##### Labels
```sql
SELECT * FROM db WHERE label = 'grocery';
```

##### Transaction ID
```sql
SELECT * FROM db WHERE id = 1234;

-- or simply
SELECT 1234 FROM db
```