mod import;
mod export;
mod select;

use nom::bytes::complete::{is_not, tag_no_case};
use nom::character::complete::{char, multispace0, multispace1};
use nom::{InputTakeAtPosition, IResult};
use nom::branch::alt;
use nom::combinator::opt;
use nom::sequence::delimited;
use crate::common::Error;

#[derive(Debug, PartialEq)]
pub(crate) enum Statement {
    /// EXPORT TO file_path
    Export(String),
    /// IMPORT account FROM file_path
    Import(String, String, bool, bool),
    /// SELECT statement
    Select
}

pub(crate) enum Projection {
    Star,
    Sum,
    Count,
    Auto,
    Id(usize),
}

pub(crate) fn parse(query: &str) -> Result<Statement, Error> {
    let result = alt((export::export, import::import))(query);
    match result {
        Ok((_, statement)) => Ok(statement),
        Err(e) => Err(Error::new(e.to_string()))
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
        let result = parse(query);
        assert_eq!(result, Ok(Statement::IMPORT("amex-explorer".to_string(), "./finance/export.csv".to_string(), false, false)));

        let query = "IMPORT amex-explorer FROM './finance/export.csv' (i, dryrun)";
        let result = parse(query);
        assert_eq!(result, Ok(Statement::IMPORT("amex-explorer".to_string(), "./finance/export.csv".to_string(), true, true)));

    }
}