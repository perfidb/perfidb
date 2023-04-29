mod import;
mod export;
mod select;
mod label;
mod condition;
mod insert;
mod delete;

use std::ops::Range;
use chrono::NaiveDate;
use log::warn;

use nom::{AsChar, InputTakeAtPosition, IResult};
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::character::complete::{char, digit1, multispace0};
use nom::error::{Error, ErrorKind};
use crate::csv_reader::Record;
use crate::db::label_op::{LabelCommand};

#[derive(Debug, PartialEq)]
pub(crate) enum Statement {
    /// SELECT statement (projection, account, where clause, group by)
    Select(Projection, Option<String>, Option<Condition>, Option<GroupBy>),

    /// LABEL 100 200 : food -grocery
    Label(Vec<u32>, LabelCommand),

    /// EXPORT TO file_path
    Export(String),

    /// IMPORT account FROM file_path
    Import(String, String, bool, bool),

    /// INSERT INTO account VALUES (2022-05-20, 'description', -30.0, 'label1, label2'), (2022-05-21, 'description', -32.0)
    Insert(Option<String>, Vec<Record>),

    /// DELETE trans_id
    Delete(Vec<u32>),
}

#[derive(Debug, PartialEq)]
pub(crate) enum Projection {
    Star,
    Sum(GroupBy),
    Count(GroupBy),
    Auto,
    Id(u32),
}

#[derive(Debug, PartialEq)]
pub(crate) enum GroupBy {
    None,
    Label,
}

#[derive(Debug, PartialEq)]
pub(crate) enum Condition {
    Id(u32),
    Spending(Operator, f32),
    Income(Operator, f32),
    Amount(Operator, f32),
    Description(Operator, String),
    /// Start date(inclusive) and end date(exclusive) for the period
    Date(Operator, Range<NaiveDate>),
    Label(Operator, String),
    And(Box<(Condition, Condition)>),
    Or(Box<(Condition, Condition)>),
}

impl Condition {
    pub(crate) fn from_logical(op: &LogicalOperator, cond1: Condition, cond2: Condition) -> Condition {
        match op {
            LogicalOperator::And => Condition::And(Box::new((cond1, cond2))),
            LogicalOperator::Or => Condition::Or(Box::new((cond1, cond2)))
        }
    }
}

#[derive(Debug, PartialEq)]
pub(crate) enum Operator {
    Eq,
    Gt,
    GtEq,
    Lt,
    LtEq,
    Match,
    IsNull,
    IsNotNull,
}

#[derive(Debug, PartialEq)]
pub(crate) enum LogicalOperator {
    And,
    Or
}

impl From<&str> for Operator {
    fn from(value: &str) -> Self {
        let lower_case = value.to_ascii_lowercase();
        match lower_case.as_str() {
            "=" => Operator::Eq,
            ">" => Operator::Gt,
            "<" => Operator::Lt,
            ">=" => Operator::GtEq,
            "<=" => Operator::LtEq,
            "match" | "like" => Operator::Match,
            _ => panic!("Unable to parse operator {}", lower_case)
        }
    }
}

pub(crate) fn parse(query: &str) -> IResult<&str, Statement> {
    alt((
        select::select,
        label::parse_label,
        export::export,
        import::import,
        insert::parse_insert,
        delete::parse_delete,
    ))(query)
}

pub(crate) fn non_space(input: &str) -> IResult<&str, &str> {
    input.split_at_position_complete(char::is_whitespace)
}

pub(crate) fn non_space1(input: &str) -> IResult<&str, &str> {
    input.split_at_position1_complete(char::is_whitespace, ErrorKind::Fail)
}

pub(crate) fn space_comma1(input: &str) -> IResult<&str, &str> {
    input.split_at_position1_complete(|c| { !c.is_whitespace() && c != ',' }, ErrorKind::Fail)
}

fn yyyy_mm_dd_date(input: &str) -> IResult<&str, NaiveDate> {
    let original_input = input;
    let (input, year) = digit1(input)?;
    let (input, _) = tag("-")(input)?;
    let (input, month) = digit1(input)?;
    let (input, _) = tag("-")(input)?;
    let (input, day) = digit1(input)?;

    let date_str = format!("{year}-{month}-{day}");
    let date = NaiveDate::parse_from_str(date_str.as_str(), "%Y-%m-%d");
    match date {
        Ok(date) => Ok((input, date)),
        Err(e) => {
            warn!("{e:?}");
            Err(nom::Err::Error(Error::new(original_input, ErrorKind::Fail)))
        }
    }
}

/// Eat a comma with optional leading and trailing whitespace
fn comma(input: &str) -> IResult<&str, ()> {
    let (input, _) = multispace0(input)?;
    let (input, _) = char(',')(input)?;
    let (input, _) = multispace0(input)?;
    Ok((input, ()))
}

fn floating_point_num(input: &str) -> IResult<&str, f32> {
    let original_input = input;
    let (input, value) = input.split_at_position_complete(|c| {
        let c = c.as_char();
        !(c.is_dec_digit() || c == '.' || c == '-')
    })?;

    match value.parse::<f32>() {
        Ok(value) => Ok((input, value)),
        Err(e) => {
            warn!("{e:?}");
            Err(nom::Err::Error(nom::error::Error::new(original_input, ErrorKind::Fail)))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::sql::parser::{parse, Statement};

    #[test]
    fn test() {
        let query = "EXPORT  to './finance/export.csv'";
        let result = parse(query);
        println!("{:?}", result);

        let query = "IMPORT amex-explorer FROM './finance/export.csv'";
        let (_, result) = parse(query).unwrap();
        assert_eq!(result, Statement::Import("amex-explorer".to_string(), "./finance/export.csv".to_string(), false, false));

        let query = "IMPORT amex-explorer FROM './finance/export.csv' (i, dryrun)";
        let (_, result) = parse(query).unwrap();
        assert_eq!(result, Statement::Import("amex-explorer".to_string(), "./finance/export.csv".to_string(), true, true));
    }
}