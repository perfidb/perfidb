use nom::bytes::complete::tag_no_case;
use nom::character::complete::multispace1;
use nom::IResult;
use crate::sql::parser::Statement;

/// Match `SELECT` statements. This is still working-in-progress. We are trying to migrate
/// all `SELECT` syntax into this parser.

/// Parse `SELECT *` pattern.
pub(crate) fn select(input: &str) -> IResult<&str, Statement> {
    let (input, _) = tag_no_case("SELECT")(input)?;
    let (input, _) =  multispace1(input)?;
    let (input, _) = tag_no_case("*")(input)?;
    Ok((input, Statement::Select))
}