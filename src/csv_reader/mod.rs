use std::{fmt};
use std::ops::Index;
use std::path::Path;
use chrono::{NaiveDate, NaiveDateTime};
use csv::StringRecord;
use log::{info};
use regex::Regex;

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

struct CsvHeaderIndex {
    perfidb_transaction_id_index: Option<usize>,
    perfidb_account_index: Option<usize>,
    perfidb_label_index: Option<usize>,
    date: usize,
    description: usize,
    amount: usize,
    credit_amount: Option<usize>,
}

pub(crate) fn read_transactions(table_name :&str, file_path: &Path, inverse_amount: bool) -> Result<Vec<Record>, CsvError> {
    if !file_path.exists() {
        return Err(CsvError::FileNotFoundError("File not found".to_string()));
    }

    info!("Scanning CSV headers from {:?}", file_path);
    // Checking if first row is header
    let mut rdr = csv::ReaderBuilder::new().has_headers(false).from_path(file_path).unwrap();
    let mut first_row = StringRecord::new();
    rdr.read_record(&mut first_row).unwrap();
    let mut first_row_joined = String::new();
    for column in first_row.iter() {
        first_row_joined.push_str(column);
        first_row_joined.push('|');
    }
    let mut second_row = StringRecord::new();
    let has_second_row = rdr.read_record(&mut second_row).unwrap();

    info!("Analysing first row: {}", first_row_joined.as_str());

    let header_pattern = Regex::new(r"(?i)_perfidb_account|date|time|amount|total|description").unwrap();
    let has_header = has_second_row
        && header_pattern.is_match(first_row_joined.as_str())
        && first_row.get(0).unwrap().len() != second_row.get(0).unwrap().len();


    let mut rdr = csv::ReaderBuilder::new().has_headers(has_header).from_path(file_path).unwrap();
    let header_index :CsvHeaderIndex = if has_header {
        info!("Header row detected");
        parse_header_index(rdr.headers().unwrap())?
    } else {
        info!("No header row detected");

        // TODO: ensure robust handling of header index when no header is detected
        CsvHeaderIndex {
            perfidb_transaction_id_index: None,
            perfidb_account_index: None,
            perfidb_label_index: None,
            date: 0,
            amount: 1,
            credit_amount: None,
            description: 2
        }
    };

    let mut records :Vec<Record> = vec![];
    let inverse_amount :f32 = if inverse_amount { -1.0 } else { 1.0 };
    for record in rdr.records() {
        let row = record.unwrap();
        let date = parse_date(row.get(header_index.date).unwrap());
        let description = row.get(header_index.description).unwrap().to_string();
        let amount = parse_amount(&row, &header_index) * inverse_amount;

        let id = match header_index.perfidb_transaction_id_index {
            Some(i) => Some(row.index(i).parse::<u32>().unwrap()),
            None => None
        };

        let account = match header_index.perfidb_account_index {
            Some(i) => row.index(i).to_string(),
            None => table_name.to_string()
        };

        let labels: Option<Vec<String>> = match header_index.perfidb_label_index {
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

fn parse_header_index(headers: &StringRecord) -> Result<CsvHeaderIndex, CsvError> {
    let mut perfidb_account_index :Option<usize> = None;
    let mut perfidb_transaction_id_index :Option<usize> = None;
    let mut perfidb_label_index :Option<usize> = None;
    let mut date_index :Option<usize> = None;
    let mut description_index :Option<usize> = None;
    let mut debit_amount_index :Option<usize> = None;
    let mut credit_amount_index :Option<usize> = None;

    for (i, s) in headers.iter().enumerate() {
        match s.to_ascii_lowercase().as_str() {
            "_perfidb_transaction_id" => perfidb_transaction_id_index = Some(i),
            "_perfidb_account" => perfidb_account_index = Some(i),
            "_perfidb_label" => perfidb_label_index = Some(i),
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

    Ok(CsvHeaderIndex {
        perfidb_transaction_id_index,
        perfidb_account_index,
        perfidb_label_index,
        date: date_index.unwrap(),
        description: description_index.unwrap(),
        amount: debit_amount_index.unwrap(),
        credit_amount: credit_amount_index,
    })
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
        NaiveDate::parse_from_str(s, "%d/%m/%Y").unwrap().and_hms(0, 0, 0)
    } else if ddmmmyyyy.is_match(s) {
        NaiveDate::parse_from_str(s, "%d %b %Y").unwrap().and_hms(0, 0, 0)
    } else {
        NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap().and_hms(0, 0, 0)
    }
}

fn parse_amount(row: &StringRecord, header_index: &CsvHeaderIndex) -> f32 {
    if header_index.credit_amount.is_none() {
        let amount_str = row.get(header_index.amount).unwrap().replace(['$', ','], "");
        return amount_str.trim().parse::<f32>().unwrap();
    }

    // if we get here it means there is a 'credit amount' column.

    // first check if debit amount is empty
    let amount_str = row.get(header_index.amount).unwrap().replace(['$', ','], "");
    if !amount_str.is_empty() {
        -amount_str.parse::<f32>().unwrap()
    } else {
        row.get(header_index.credit_amount.unwrap()).unwrap().replace(['$', ','], "").parse::<f32>().unwrap()
    }
}