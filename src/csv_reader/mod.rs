use std::{fmt};
use std::ops::Index;
use std::path::Path;
use chrono::{NaiveDate, NaiveDateTime};
use csv::StringRecord;
use log::{info};
use regex::Regex;
use crate::csv_reader::column::ColumnInfo;

mod column;

/// A transaction record in csv file
pub(crate) struct Record {
    pub(crate) id: Option<u32>,
    pub(crate) account: String,
    pub(crate) date: NaiveDateTime,
    pub(crate) description: String,
    pub(crate) amount: f32,
    pub(crate) labels: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CsvError {
    FileNotFoundError(String),
    InvalidFileError(String),
}

impl fmt::Display for CsvError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "csv reading error: {}",
            match self {
                CsvError::FileNotFoundError(s) => s,
                CsvError::InvalidFileError(s) => s,
            }
        )
    }
}

impl std::error::Error for CsvError {}

pub(crate) fn read_transactions(table_name :&str, file_path: &Path, inverse_amount: bool) -> Result<Vec<Record>, CsvError> {
    if !file_path.exists() {
        return Err(CsvError::FileNotFoundError("File not found".into()));
    }

    let header_row = detect_header_row(file_path);

    let column_info = match &header_row {
        Some(header_row) => {
            info!("Header row detected");
            column::parse_csv_column_with_header(header_row)?
        },
        None => {
            column::parse_csv_column_no_header(file_path)
        }
    };

    let mut rdr = csv::ReaderBuilder::new().has_headers(column_info.has_header).from_path(file_path).unwrap();
    let mut records :Vec<Record> = vec![];
    let inverse_amount :f32 = if inverse_amount { -1.0 } else { 1.0 };
    for record in rdr.records() {
        let row = record.unwrap();
        let date = parse_date(row.get(column_info.date_column).unwrap());
        let description = row.get(column_info.description_column).unwrap().to_string();
        let amount = parse_amount(&row, &column_info) * inverse_amount;

        let id = column_info.perfidb_transaction_id_column.map(|i| row.index(i).parse::<u32>().unwrap());

        let account = match column_info.perfidb_account_column {
            Some(i) => row.index(i).to_string(),
            None => table_name.to_string()
        };

        let labels: Option<Vec<String>> = match column_info.perfidb_label_column {
            Some(i) => {
                match row.index(i) {
                    "" => None,
                    _ => Some(row.index(i).split('|').map(str::to_string).collect())
                }
            },
            None => None
        };

        records.push(Record {
            id,
            account,
            date,
            description,
            amount,
            labels
        });
    }

    Ok(records)
}

/// Try detecting if the first row of csv file is a 'header' row.
/// Most bank statements should include a header row, e.g. "date | amount | description". Some banks' statement does not
/// include a header row, the first row is the first transaction data.
fn detect_header_row(csv_path: &Path) -> Option<StringRecord> {
    let mut csv_reader = csv::ReaderBuilder::new().has_headers(false).from_path(csv_path).unwrap();
    let mut first_row = StringRecord::new();
    csv_reader.read_record(&mut first_row).unwrap();

    let mut match_header_pattern = false;
    let header_pattern = Regex::new(r"(?i)_perfidb_account|date|time|amount|total|description").unwrap();
    for column in first_row.iter() {
        if header_pattern.is_match(column) {
            match_header_pattern = true;
            break;
        }
    }

    let mut second_row = StringRecord::new();
    let has_second_row = csv_reader.read_record(&mut second_row).unwrap();

    let has_header = has_second_row
        && match_header_pattern
        && first_row.get(0).unwrap().len() != second_row.get(0).unwrap().len();

    if has_header { Some(first_row) } else { None }
}

fn parse_date(s :&str) -> NaiveDateTime {
    let yyyymmdd_t_hhmmss = Regex::new(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$").unwrap();
    let yyyymmdd_t_hhmmss_zone = Regex::new(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+.+$").unwrap();
    let ddmmyyyy = Regex::new(r"^\d{2}/\d{2}/\d{4}$").unwrap();
    let ddmmmyyyy = Regex::new(r"^\d{1,2} [a-zA-Z]{3} \d{4}$").unwrap();

    if yyyymmdd_t_hhmmss.is_match(s) {
        NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S").unwrap()
    } else if yyyymmdd_t_hhmmss_zone.is_match(s) {
        NaiveDateTime::parse_from_str(&s[0..19], "%Y-%m-%dT%H:%M:%S").unwrap()
    } else if ddmmyyyy.is_match(s) {
        NaiveDate::parse_from_str(s, "%d/%m/%Y").unwrap().and_hms_opt(0, 0, 0).unwrap()
    } else if ddmmmyyyy.is_match(s) {
        NaiveDate::parse_from_str(s, "%d %b %Y").unwrap().and_hms_opt(0, 0, 0).unwrap()
    } else {
        NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap().and_hms_opt(0, 0, 0).unwrap()
    }
}

fn parse_amount(row: &StringRecord, header_index: &ColumnInfo) -> f32 {
    if header_index.credit_amount_column.is_none() {
        let amount_str = row.get(header_index.amount_column).unwrap().replace(['$', ','], "");
        return amount_str.trim().parse::<f32>().unwrap();
    }

    // if we get here it means there is a 'credit amount' column.

    // first check if debit amount is empty
    let amount_str = row.get(header_index.amount_column).unwrap().replace(['$', ','], "");
    if !amount_str.is_empty() {
        -amount_str.parse::<f32>().unwrap()
    } else {
        row.get(header_index.credit_amount_column.unwrap()).unwrap().replace(['$', ','], "").parse::<f32>().unwrap()
    }
}

#[cfg(test)]
mod tests;
