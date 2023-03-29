use std::path::Path;
use csv::StringRecord;
use lazy_static::lazy_static;
use regex::Regex;
use crate::csv_reader::CsvError;

/// Contains column index of a CSV transaction file.
/// Once a CSV is parsed, we need to know which column stores date, which column stores amount, etc.
/// The column number uses 0-based index.
pub(crate) struct ColumnInfo {
    /// Does this CSV file has a header row
    pub(crate) has_header: bool,
    pub(crate) perfidb_transaction_id_column: Option<usize>,
    pub(crate) perfidb_account_column: Option<usize>,
    pub(crate) perfidb_label_column: Option<usize>,
    pub(crate) date_column: usize,
    pub(crate) description_column: usize,
    pub(crate) amount_column: usize,
    pub(crate) credit_amount_column: Option<usize>,
}

pub(crate) fn parse_csv_column_with_header(headers: &StringRecord) -> Result<ColumnInfo, CsvError> {
    let mut perfidb_account_column :Option<usize> = None;
    let mut perfidb_transaction_id_column :Option<usize> = None;
    let mut perfidb_label_column :Option<usize> = None;
    let mut date_index :Option<usize> = None;
    let mut description_index :Option<usize> = None;
    let mut debit_amount_index :Option<usize> = None;
    let mut credit_amount_index :Option<usize> = None;

    for (i, s) in headers.iter().enumerate() {
        match s.to_ascii_lowercase().as_str() {
            "_perfidb_account" => perfidb_account_column = Some(i),
            "_perfidb_transaction_id" => perfidb_transaction_id_column = Some(i),
            "_perfidb_label" => perfidb_label_column = Some(i),
            _ => {}
        }
    }

    let date_regex = Regex::new(r"(?i)date|time").unwrap();
    for (i, s) in headers.iter().enumerate() {
        if date_regex.is_match(s) {
            date_index = Some(i);
            break;
        }
    }
    if let None = date_index {
        return Err(CsvError::InvalidFileError("Unable to locate 'date' column".to_string()));
    }

    let description_regex = Regex::new(r"(?i)description|narrative").unwrap();
    for (i, s) in headers.iter().enumerate() {
        if description_regex.is_match(s) {
            description_index = Some(i);
            break;
        }
    }
    if let None = description_index {
        return Err(CsvError::InvalidFileError("Unable to locate 'description' column".to_string()));
    }

    // Detecting 'debit amount' and 'credit amount', in Westpac statements
    let debit_amount_regex = Regex::new(r"(?i)debit amount").unwrap();
    let credit_amount_regex = Regex::new(r"(?i)credit amount").unwrap();
    for (i, s) in headers.iter().enumerate() {
        if debit_amount_regex.is_match(s) {
            debit_amount_index = Some(i);
        }
        if credit_amount_regex.is_match(s) {
            credit_amount_index = Some(i);
        }
    }

    // if we found only debit amount or only credit amount, report error
    if (debit_amount_index.is_none() && credit_amount_index.is_some()) ||
        (debit_amount_index.is_some() && credit_amount_index.is_none()) {
        return Err(CsvError::InvalidFileError("Unable to locate debit and credit amount column".to_string()));
    }

    if let None = debit_amount_index {
        let amount_regex = Regex::new(r"(?i)amount|subtotal").unwrap();
        for (i, s) in headers.iter().enumerate() {
            if amount_regex.is_match(s) {
                debit_amount_index = Some(i);
                break;
            }
        }
        if let None = debit_amount_index {
            return Err(CsvError::InvalidFileError("Unable to locate amount column".to_string()));
        }
    }

    Ok(ColumnInfo {
        has_header: true,
        perfidb_transaction_id_column,
        perfidb_account_column,
        perfidb_label_column,
        date_column: date_index.unwrap(),
        description_column: description_index.unwrap(),
        amount_column: debit_amount_index.unwrap(),
        credit_amount_column: credit_amount_index,
    })
}



pub(crate) fn parse_csv_column_no_header(csv_path: &Path) -> ColumnInfo {
    let mut reader = csv::ReaderBuilder::new().has_headers(false).from_path(csv_path).unwrap();
    let mut rows :Vec<StringRecord> = vec![];

    // Read up to first 5 rows
    for (i, row) in reader.records().enumerate() {
        if i >= 5 {
            break;
        }
        rows.push(row.unwrap());
    }

    let num_columns = rows[0].len();

    let mut date_column_index = None;
    let mut amount_column_index = None;
    let mut description_column_index = None;

    // Try finding date column
    for i in 0..num_columns {
        if date_column_index.is_none() && column_match_date(i, &rows) {
            date_column_index = Some(i);
        } else if amount_column_index.is_none() && column_match_amount(i, &rows) {
            amount_column_index = Some(i);
        } else if description_column_index.is_none() && column_match_description(i, &rows) {
            description_column_index = Some(i);
        }
    }

    ColumnInfo {
        has_header: false,
        perfidb_transaction_id_column: None,
        perfidb_account_column: None,
        perfidb_label_column: None,
        date_column: date_column_index.unwrap(),
        amount_column: amount_column_index.unwrap(),
        description_column: description_column_index.unwrap(),
        credit_amount_column: None,
    }
}

lazy_static! {
    static ref ALL_DIGITS_DATE: Regex = Regex::new(r"\d{6,}").unwrap();

    static ref ALL_DIGITS_AMOUNT: Regex = Regex::new(r"\d+").unwrap();
}

fn column_match_date(column: usize, rows: &[StringRecord]) -> bool {
    for row in rows {
        let value = row[column].trim().to_uppercase().to_string();
        // If length less than 6 it can't be date
        if value.len() < 6 {
            return false;
        }

        if !ALL_DIGITS_DATE.is_match(&value.replace([' ', 'T', '/', '-', ':', 'Z', '+'], "")) {
            return false;
        }
    }

    true
}

fn column_match_amount(column: usize, rows: &[StringRecord]) -> bool {
    for row in rows {
        let value = row[column].trim().to_uppercase().to_string();

        if !ALL_DIGITS_AMOUNT.is_match(&value.replace([' ', '.', ',', '$', '-', '+'], "")) {
            println!("{}", value.replace([' ', '.', ',', '$', '-', '+'], ""));
            return false;
        }
    }

    true
}

fn column_match_description(column: usize, rows: &[StringRecord]) -> bool {
    let previous_length = rows[0].len();
    for row in rows {
        if row[column].len() != previous_length {
            return true;
        }
    }

    false
}
