use std::{fmt};
use std::collections::HashSet;
use std::path::Path;
use chrono::NaiveDateTime;
use csv::StringRecord;
use serde::Deserialize;
use crate::transaction::Transaction;

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
}

pub(crate) fn read_transactions(account :&str, file_path: &Path) -> Result<Vec<Transaction>, CsvError> {
    if !file_path.exists() {
        return Err(CsvError::FileNotFoundError("File not found".to_string()));
    }

    println!("Importing transactions FROM {:?}", file_path);
    let mut transactions :Vec<Transaction> = vec![];
    let mut rdr = csv::Reader::from_path(file_path).unwrap();
    let headers = rdr.headers().unwrap();
    let header_index = parse_header_index(headers)?;

    for record in rdr.records() {
        let row = record.unwrap();
        let date = parse_date(row.get(header_index.date).unwrap());
        let description = row.get(header_index.description).unwrap().to_string();
        let amount = parse_amount(row.get(header_index.amount).unwrap());

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
    let mut amount_index :i32 = -1;

    for (i, s) in headers.iter().enumerate() {
        let cleaned = s.trim().to_lowercase();
        if cleaned == "date" {
            date_index = i as i32;
            break;
        }
    }
    if date_index == -1 {
        return Err(CsvError::InvalidFileError("Unable to locate 'date' column".to_string()));
    }

    for (i, s) in headers.iter().enumerate() {
        let cleaned = s.trim().to_lowercase();
        if cleaned == "description" {
            description_index = i as i32;
            break;
        }
    }
    if description_index == -1 {
        return Err(CsvError::InvalidFileError("Unable to locate 'description' column".to_string()));
    }

    for (i, s) in headers.iter().enumerate() {
        let cleaned = s.trim().to_lowercase();
        if cleaned == "amount" {
            amount_index = i as i32;
            break;
        }
    }
    if amount_index == -1 {
        return Err(CsvError::InvalidFileError("Unable to locate amount column".to_string()));
    }

    Ok(CsvHeaderIndex {
        date: date_index as usize,
        description: description_index as usize,
        amount: amount_index as usize,
    })
}

fn parse_date(s :&str) -> NaiveDateTime {
    NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S").unwrap()
}

fn parse_amount(s :&str) -> f32 {
    s.parse::<f32>().unwrap()
}