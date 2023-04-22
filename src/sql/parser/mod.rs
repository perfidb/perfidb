mod import;
mod export;
mod select;
mod update;
mod condition;

use std::ops::Range;
use chrono::NaiveDate;

use nom::{InputTakeAtPosition, IResult};
use nom::branch::alt;
use crate::common::Error;

#[derive(Debug, PartialEq)]
pub(crate) enum Statement {
    /// EXPORT TO file_path
    Export(String),
    /// IMPORT account FROM file_path
    Import(String, String, bool, bool),
    /// SELECT statement (projection, account, where clause)
    Select(Projection, Option<String>, Option<Condition>),
    UpdateLabel(String, Option<Condition>),
}

#[derive(Debug, PartialEq)]
pub(crate) enum Projection {
    Star,
    Sum(GroupBy),
    Count(GroupBy),
    Auto,
    Id(usize),
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

pub(crate) fn parse(query: &str) -> Result<Statement, Error> {
    let result = alt((export::export, import::import, select::select, update::update))(query);
    match result {
        Ok((_, statement)) => Ok(statement),
        Err(e) => Err(Error::new(e.to_string()))
    }
}

pub(crate) fn non_space(input: &str) -> IResult<&str, &str> {
    input.split_at_position_complete(char::is_whitespace)
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
        let result = parse(query);
        assert_eq!(result, Ok(Statement::Import("amex-explorer".to_string(), "./finance/export.csv".to_string(), false, false)));

        let query = "IMPORT amex-explorer FROM './finance/export.csv' (i, dryrun)";
        let result = parse(query);
        assert_eq!(result, Ok(Statement::Import("amex-explorer".to_string(), "./finance/export.csv".to_string(), true, true)));
    }
}