use nom::bytes::complete::{is_not, tag_no_case};
use nom::character::complete::{char, multispace0, multispace1};
use nom::combinator::opt;
use nom::{InputTakeAtPosition, IResult};
use nom::sequence::delimited;
use crate::sql::parser::{non_space, Statement};

/// Parse `IMPORT amex-explorer FROM ./file/path (inverse dryrun)
/// TODO: handle file path with whitespace
pub(crate) fn import(input: &str) -> IResult<&str, Statement> {
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

fn parentheses(input: &str) -> IResult<&str, &str> {
    delimited(char('('), is_not(")"), char(')'))(input)
}
