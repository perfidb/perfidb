# PerfiDB

PerfiDB is a SQL database engineered specifically to store and manage personal finance data. The main features include:

- A simple yet powerful labelling system. (Think about Gmail labels for your bank transactions)
- Intuitive and concise SQL statements to manage your money


# Examples
```sql
-- Import transactions to account 'amex' from a csv file
IMPORT amex FROM 'bank-exports/2022-03.csv';

-- List all transactions
SELECT * ;

-- List transactions from account 'amex'
SELECT spending FROM amex;

-- Add two labels (grocery, bread) to all transactions in July containing description text 'bakehouse'
LABEL WHERE date = 2022-07 AND description LIKE 'bakehouse'  grocery bread;

-- List all transactions labelled with 'grocery'.
SELECT * WHERE label = 'grocery';
```

# How to use PerfiDB
A common use case is to export transactions from your banks and run SQL `COPY` statement to load them into PerfiDB.

### Launch
Run `perfidb` command and specify a new database file:
```
perfidb myfinance.db
```

### Running a query
A query should end with a semicolon `;`. A query can extend to multiple lines, the last line has to end with a semicolon.

### Import transactions
To import transactions from a csv file into account _amex_gold_
```sql
IMPORT amex_gold FROM 'bank-exports/2022-03.csv';
```

To print out records from csv file without actually saving to database, specify dry-run:
```sql
IMPORT amex_gold FROM 'bank-exports/2022-03.csv' (dryrun);
```

If you are wondering how are CSV files parsed, see _How are CSV files parsed_ section below.

### Export transactions
To export all transactions to a CSV file
```sql
EXPORT TO '/home/ren/all_trans.csv';
```

To export transactions from a specific account to a CSV file

(Note: note implemented at the moment)
```sql
EXPORT amex TO './amex.csv';
```

### Query

#### From all accounts
```sql
SELECT *;
```

#### Show only spending or income
```sql
SELECT spending;

SELECT income;
```

#### From specific account
```sql
SELECT * FROM amex;
```

#### Filters
##### Dates
```sql
-- Filter by month, i.e. 7 means July. If current date has passed July it means July of current year,
-- if current date is before end of July it means July of previous year.
SELECT * WHERE date = 7;

-- Filter by month
SELECT * WHERE date = 2022-07;

-- Filter by date
SELECT * WHERE date = 2022-07-31;
```

##### Labels
```sql
SELECT * WHERE label = 'grocery';

-- Preview auto labelling results
SELECT auto() WHERE date = 2022-07;

-- Label by transaction id. Apply 'food' and 'dining' to transaction 100 and 201.
LABEL 100 101 food dining;

-- Apply auto labelling 
LABEL 100, 101 auto();
```

##### Transaction ID
```sql
SELECT * WHERE id = 1234;

-- or simply
SELECT 1234;
```

### INSERT
```sql
INSERT INTO amex VALUES
  ('2023-02-21', 'food', -45.0),
  ('2023-02-23', 'salary', 500);
```

## Live mode
Sometimes you might want to label transactions directly as if operating a spreadsheet, without using SQL. The **live** mode allows you to do exactly that. To switch to live mode, type command `live`, without semicolon.

The live mode loads transactions of your very last `SELECT` query. Use `j` and `k` to move up and down. To edit label of the highlighed transaction, press `l` and start typing labels. If you want to apply multiple labels use comma as a separator. Press `Enter` once finished editing.

As you start typing the new label, not all characters from the old label are overridden, that is fine, new labels will be applied to the transaction.

Once new labels are applied to a transaction all existing labels of that transaction will be removed. At the moment PerfiDB does not support partial editing in live mode.

To get out of live mode, press `q`.

## How are CSV files parsed

PerfiDB first tries to detect if the first line in CSV is the header. It checks the presence of some common patterns, e.g. date, description, amount, etc. 

If header line is detected, it will also try to detect those columns and then parse each row in CSV using the detected column.

It also tries to parse the transaction date with a few common date formats.

If no header line is detected in CSV it assumes the column in following order: date, amount, description.
