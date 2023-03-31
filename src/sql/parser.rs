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
}

pub(crate) fn parse(query: &str) -> Result<Statement, Error> {
    let result = alt((export, import))(query);
    match result {
        Ok((_, statement)) => Ok(statement),
        Err(e) => Err(Error::new(e.to_string()))
    }
}

/// Parse `EXPORT TO file_path` pattern.
fn export(input: &str) -> IResult<&str, Statement> {
    let (input, _) = tag_no_case("EXPORT")(input)?;
    let (input, _) =  multispace1(input)?;
    let (input, _) = tag_no_case("TO")(input)?;
    let (file_path, _) =  multispace1(input)?;
    let quotation_marks :&[_] = &['\'', '"'];
    Ok((file_path, Statement::Export(file_path.trim_matches(quotation_marks).to_string())))
}

/// Parse `IMPORT amex-explorer FROM ./file/path (inverse dryrun)
/// TODO: handle file path with whitespace
fn import(input: &str) -> IResult<&str, Statement> {
    let (input, _) = tag_no_case("IMPORT")(input)?;
    let (input, _) =  multispace1(input)?;
    let (input, account) =  non_space(input)?;
    let (input, _) =  multispace1(input)?;
    let (input, _) = tag_no_case("FROM")(input)?;
    let (input, _) =  multispace1(input)?;
    let (input, file_path) =  non_space(input)?;
    let (input, _) =  multispace0(input)?;
    let (_, import_options) =  parse_import_options(input)?;

    let mut inverse_flag = false;
    let mut dryrun_flag = false;
    if let Some(import_options) = import_options {
        for import_option in import_options.split(&[' ', ',']) {
            if import_option == "i" || import_option == "inverse" {
                inverse_flag = true;
            } else if import_option == "dryrun" {
                dryrun_flag = true;
            }
        }
    }

    let quotation_marks :&[_] = &['\'', '"'];
    Ok((file_path, Statement::Import(
        account.to_string(),
        file_path.trim_matches(quotation_marks).to_string(),
        inverse_flag,
        dryrun_flag
    )))
}

fn parse_import_options(input: &str) -> IResult<&str, Option<&str>> {
    opt(parentheses)(input)
}

fn non_space(input: &str) -> IResult<&str, &str> {
    input.split_at_position_complete(char::is_whitespace)
}

fn parentheses(input: &str) -> IResult<&str, &str> {
    delimited(char('('), is_not(")"), char(')'))(input)
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