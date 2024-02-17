use nom::bytes::complete::{is_not, tag_no_case};
use nom::character::complete::{char, multispace0};
use nom::combinator::opt;
use nom::{IResult};
use nom::sequence::delimited;
use crate::parser::{Statement};

/// Parse `IMPORT (inverse dryrun)
pub(crate) fn import(input: &str) -> IResult<&str, Statement> {
    let (input, _) = tag_no_case("IMPORT")(input)?;
    let (input, _) =  multispace0(input)?;
    let (input, import_options) =  parse_import_options(input)?;

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

    Ok((input, Statement::Import(inverse_flag, dryrun_flag)))
}

fn parse_import_options(input: &str) -> IResult<&str, Option<&str>> {
    opt(parentheses)(input)
}

fn parentheses(input: &str) -> IResult<&str, &str> {
    delimited(char('('), is_not(")"), char(')'))(input)
}
