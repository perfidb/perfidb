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
SELECT * FROM db WHERE tags = 'grocery';

-- Show transactions from amex account labelled with 'grocery'.
SELECT * FROM amex WHERE tags = 'grocery';
```

A common use case is to export transactions from your banks and run SQL `COPY` statement to load them into PerfiDB.