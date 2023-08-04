# What is PerfiDB

PerfiDB is a simple database engineered specifically to store and manage personal finance data. The main features include:
- Using intuitive and elegant SQL-like language to query your transactions
- Full text search in your transaction description
- Intelligently parsing Internet banking statements, it understands the date, amount, and description columns
- A simple yet powerful labelling system, think about Gmail labels for your bank transactions.
- Keeping your sensitive personal finance data locally on your computer

## How to get help
- Ask questions in the Discord channel: https://discord.gg/Yg2cStNC
- Create issues in Github

# Quick tour
### Launch
```bash
perfidb
```
Database file will be created under `$HOME/.perfidb/finance.db`

### Import transactions
```sql
-- Import transactions to account 'amex' from a csv file
IMPORT amex FROM 'bank-exports/2022-03.csv';
```

### Query
```sql    
-- List all transactions
SELECT *;

-- List all spending from account 'amex'
SELECT spending FROM amex;

-- List all spending with the word 'paypal' in description
SELECT spending WHERE description = 'paypal';

-- Add two labels (grocery, bread) to transaction 128
LABEL 128 grocery bread;

-- List all transactions labelled with 'grocery'.
SELECT * WHERE label = 'grocery';
```

# User guide
## Install
### Install on macOS
```
brew install perfidb/tap/perfidb
```

### Linux & Windows
Install script will be published in near future. Please build form source for now.

## Launch
```
perfidb
```
By default the database file will be created under `$HOME/.perfidb/finance.db`. To specify a different location
you can run:
```
perfidb -f myfinance.db
```

### Exit
To exit PerfiDB you can either press `Ctrl + C` or type in the command `exit` 

## Running a query
A query should end with a semicolon `;`. A query can extend to multiple lines, the last line has to end with a semicolon.

## Import transactions
To import transactions from a csv file into account _amex-gold_
```sql
IMPORT amex-gold FROM 'bank-exports/2022-03.csv';
```

To print out records from csv file without actually saving to database, specify dry-run:
```sql
IMPORT amex-gold FROM 'bank-exports/2022-03.csv' (dryrun);
```

If you are wondering how are CSV files parsed, see _How are CSV files parsed_ section below.

## Spending & Income
By default transactions with negative amount (e.g. -35.7) is considered as _spending_ and transactions with 
positive amount _income_. Some bank statements are the opposite, e.g. American Express. When important statements
with positive amount (e.g. 50.95) as spending you need to specify the `inverse` flag, e.g.
```sql
IMPORT amex FROM 'bank-exports/2022-03.csv' (inverse);

-- You can also add dryrun option to check the amount before importing
IMPORT amex FROM 'bank-exports/2022-03.csv' (inverse dryrun);    
```

## Export transactions
To export all transactions to a CSV file
```sql
EXPORT TO '/home/ren/all_trans.csv';
```

To export transactions from a specific account to a CSV file

(Note: not implemented at the moment)
```sql
EXPORT amex TO './amex.csv';
```

## Query

### From all accounts
```sql
SELECT *;
```

### Show only spending or income
```sql
SELECT spending;

SELECT income;
```

### From specific account
```sql
SELECT * FROM amex;
```

### Filters
#### Dates
```sql
-- Filter by month, i.e. 7 means July. If current date has passed July it means July of current year,
-- if current date is before end of July it means July of previous year.
SELECT * WHERE date = 7;

-- Filter by month
SELECT * WHERE date = 2022-07;

-- Filter by date
SELECT * WHERE date = 2022-07-31;
```

#### Labels
```sql
SELECT * WHERE label = 'grocery';

-- Preview auto labelling results
SELECT auto() WHERE date = 2022-07;

-- Label by transaction id. Apply 'food' and 'dining' to transaction 100 and 201.
LABEL 100 101 food dining;

-- Apply auto labelling 
LABEL 100, 101 auto();
```

#### Amount
```sql
SELECT * WHERE spending > 100;

SELECT * WHERE income > 100;

SELECT * WHERE amount < -50;
```

#### Transaction ID
```sql
SELECT * WHERE id = 1234;

-- or simply
SELECT 1234;
```

#### Logical operator AND, OR
```sql
SELECT * WHERE spending > 100 AND label = 'grocery';
```

### SUM, COUNT
Get subtotal of spending or income
```sql
SELECT SUM(spending) WHERE date = 2023-03;

SELECT SUM(income) WHERE date = 2023-03;
```

Get subtotal of both spending and income in March
```sql
SELECT SUM(*) WHERE date = 2023-03;
```

Count number of transactions
```sql
SELECT COUNT(spending) WHERE date = 2023-03;
SELECT COUNT(income) WHERE date = 2023-03;
SELECT COUNT(*) WHERE date = 2023-03;
```

## Insert transactions manually
```sql
INSERT INTO amex VALUES
  ('2023-02-21', 'food', -45.0),
  ('2023-02-23', 'salary', 500);
```

## Delete transaction
```sql
-- delete by transaction ids
DELETE 345 346;
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
