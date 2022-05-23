use std::{fmt};
use std::collections::HashSet;
use std::path::Path;
use chrono::{NaiveDate, NaiveDateTime};
use csv::StringRecord;
use crate::transaction::Transaction;
use log::{info};
use regex::Regex;

#[derive(Debug, Clone, PartialEq)]
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

struct CsvHeaderIndex {
    date: usize,
    description: usize,
    amount: usize,
    credit_amount: Option<usize>,
}

pub(crate) fn read_transactions(account :&str, file_path: &Path, inverse_amount: bool) -> Result<Vec<Transaction>, CsvError> {
    if !file_path.exists() {
        return Err(CsvError::FileNotFoundError("File not found".to_string()));
    }

    info!("Scanning CSV headers from {:?}", file_path);
    // Checking if first row is header
    let mut rdr = csv::ReaderBuilder::new().has_headers(false).from_path(file_path).unwrap();
    let mut first_row = StringRecord::new();
    rdr.read_record(&mut first_row);
    let mut first_row_joined = String::new();
    for column in first_row.iter() {
        first_row_joined.push_str(column);
        first_row_joined.push('|');
    }
    let mut second_row = StringRecord::new();
    let has_second_row = rdr.read_record(&mut second_row).unwrap();

    println!("{}", first_row_joined.as_str());

    let header_pattern = Regex::new(r"(?i)date|time|amount|total|description").unwrap();
    let has_header = has_second_row
        && header_pattern.is_match(first_row_joined.as_str())
        && first_row.get(0).unwrap().len() != second_row.get(0).unwrap().len();


    let mut rdr = csv::ReaderBuilder::new().has_headers(has_header).from_path(file_path).unwrap();
    let header_index :CsvHeaderIndex;
    if has_header {
        info!("Header row detected");
        header_index = parse_header_index(rdr.headers().unwrap())?;
    } else {
        info!("No header row detected");

        // TODO: ensure robust handling of header index when no header is detected
        header_index = CsvHeaderIndex {
            date: 0,
            amount: 1,
            credit_amount: None,
            description: 2
        }
    }

    let mut transactions :Vec<Transaction> = vec![];
    let inverse_amount :f32 = if inverse_amount { -1.0 } else { 1.0 };
    for record in rdr.records() {
        let row = record.unwrap();
        let date = parse_date(row.get(header_index.date).unwrap());
        let description = row.get(header_index.description).unwrap().to_string();
        let amount = parse_amount(&row, &header_index) * inverse_amount;

        transactions.push(Transaction {
            account: account.to_string(),
            date,
            description,
            amount,
            tags: HashSet::new()
        });
    }

    Ok(transactions)
}

fn parse_header_index(headers: &StringRecord) -> Result<CsvHeaderIndex, CsvError> {
    let mut date_index :i32 = -1;
    let mut description_index :i32 = -1;
    let mut debit_amount_index :i32 = -1;
    let mut credit_amount_index :i32 = -1;

    let date_regex = Regex::new(r"(?i)date|time").unwrap();
    for (i, s) in headers.iter().enumerate() {
        if date_regex.is_match(s) {
            date_index = i as i32;
            break;
        }
    }
    if date_index == -1 {
        return Err(CsvError::InvalidFileError("Unable to locate 'date' column".to_string()));
    }

    let description_regex = Regex::new(r"(?i)description|narrative").unwrap();
    for (i, s) in headers.iter().enumerate() {
        if description_regex.is_match(s) {
            description_index = i as i32;
            break;
        }
    }
    if description_index == -1 {
        return Err(CsvError::InvalidFileError("Unable to locate 'description' column".to_string()));
    }

    // Detecting 'debit amount' and 'credit amount', in Westpac statements
    let debit_amount_regex = Regex::new(r"(?i)debit amount").unwrap();
    let credit_amount_regex = Regex::new(r"(?i)credit amount").unwrap();
    for (i, s) in headers.iter().enumerate() {
        if debit_amount_regex.is_match(s) {
            debit_amount_index = i as i32;
        }
        if credit_amount_regex.is_match(s) {
            credit_amount_index = i as i32;
        }
    }

    // if we found only debit amount or only credit amount, report error
    if debit_amount_index * credit_amount_index < 0 {
        return Err(CsvError::InvalidFileError("Unable to locate debit and credit amount column".to_string()));
    }

    if debit_amount_index < 0 {
        let amount_regex = Regex::new(r"(?i)amount|subtotal").unwrap();
        for (i, s) in headers.iter().enumerate() {
            if amount_regex.is_match(s) {
                debit_amount_index = i as i32;
                break;
            }
        }
        if debit_amount_index == -1 {
            return Err(CsvError::InvalidFileError("Unable to locate amount column".to_string()));
        }
    }


    Ok(CsvHeaderIndex {
        date: date_index as usize,
        description: description_index as usize,
        amount: debit_amount_index as usize,
        credit_amount: if credit_amount_index < 0 { None } else { Some(credit_amount_index as usize) },
    })
}

fn parse_date(s :&str) -> NaiveDateTime {
    let yyyymmdd_t_hhmmss = Regex::new(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$").unwrap();
    let yyyymmdd_t_hhmmss_zone = Regex::new(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+.+$").unwrap();
    let ddmmyyyy = Regex::new(r"^\d{2}/\d{2}/\d{4}$").unwrap();
    let ddmmmyyyy = Regex::new(r"^\d{2} [a-zA-Z]{3} \d{4}$").unwrap();

    if yyyymmdd_t_hhmmss.is_match(s) {
        NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S").unwrap()
    } else if yyyymmdd_t_hhmmss_zone.is_match(s) {
        NaiveDateTime::parse_from_str(&s[0..19], "%Y-%m-%dT%H:%M:%S").unwrap()
    } else if ddmmyyyy.is_match(s) {
        NaiveDate::parse_from_str(s, "%d/%m/%Y").unwrap().and_hms(0, 0, 0)
    } else if ddmmmyyyy.is_match(s) {
        NaiveDate::parse_from_str(s, "%d %b %Y").unwrap().and_hms(0, 0, 0)
    } else {
        NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap().and_hms(0, 0, 0)
    }
}

fn parse_amount(row: &StringRecord, header_index: &CsvHeaderIndex) -> f32 {
    if header_index.credit_amount.is_none() {
        return row.get(header_index.amount).unwrap().parse::<f32>().unwrap();
    }

    let amount_str = row.get(header_index.amount).unwrap();
    if !amount_str.is_empty() {
        return -amount_str.parse::<f32>().unwrap();
    } else {
        return row.get(header_index.credit_amount.unwrap()).unwrap().parse::<f32>().unwrap();
    }
}