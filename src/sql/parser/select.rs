use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case};
use nom::character::complete::{alpha1, multispace0, multispace1, u32};
use nom::combinator::opt;
use nom::{IResult};
use nom::Err::Error;
use nom::error::ErrorKind;

use crate::sql::parser::{GroupBy, non_space, Projection, Statement};
use crate::sql::parser::condition::where_parser;

/// Match `SELECT` statements. This is still working-in-progress. We are trying to migrate
/// all `SELECT` syntax into this parser.

/// Parse `SELECT *` pattern.
pub(crate) fn select(input: &str) -> IResult<&str, Statement> {
    let (input, _) = tag_no_case("SELECT")(input)?;
    let (input, _) =  multispace1(input)?;
    let (input, projection) = alt((proj_star, proj_sum, proj_count, proj_auto, proj_trans_id))(input)?;
    let (input, account) = opt(from_account)(input)?;
    let (input, condition) = opt(where_parser)(input)?;
    let (input, group_by) = opt(group_by)(input)?;
    Ok((input, Statement::Select(projection, account, condition, group_by)))
}

/// FROM account
pub(crate) fn from_account(input: &str) -> IResult<&str, String> {
    let (input, _) = tag_no_case("FROM")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, account) = non_space(input)?;
    let (input, _) = multispace0(input)?;
    Ok((input, account.into()))
}

/// Normal projection, i.e. SELECT *
fn proj_star(input: &str) -> IResult<&str, Projection> {
    let (input, _) = tag("*")(input)?;
    let (input, _) = multispace0(input)?;
    Ok((input, Projection::Star))
}

/// SUM(*)
fn proj_sum(input: &str) -> IResult<&str, Projection> {
    let (input, _) = tag_no_case("SUM(*)")(input)?;
    let (input, _) = multispace0(input)?;
    Ok((input, Projection::Sum(GroupBy::None)))
}

/// SUM(*)
fn proj_count(input: &str) -> IResult<&str, Projection> {
    let (input, _) = tag_no_case("COUNT(*)")(input)?;
    let (input, _) = multispace0(input)?;
    Ok((input, Projection::Count(GroupBy::None)))
}

/// AUTO(*)
fn proj_auto(input: &str) -> IResult<&str, Projection> {
    let (input, _) = tag_no_case("AUTO()")(input)?;
    let (input, _) = multispace0(input)?;
    Ok((input, Projection::Auto))
}


/// SELECT 123
fn proj_trans_id(input: &str) -> IResult<&str, Projection> {
    let (input, trans_id) = u32(input)?;
    let (input, _) = multispace0(input)?;
    Ok((input, Projection::Id(trans_id)))
}

fn group_by(input: &str) -> IResult<&str, GroupBy> {
    let (input, _) = tag_no_case("group by")(input)?;
    let (input, _) =  multispace1(input)?;
    let (input, group_by_value) = alpha1(input)?;
    match group_by_value {
        "label" => Ok((input, GroupBy::Label)),
        // TODO fix the error handling
        _ => Err(Error(nom::error::Error { input, code: ErrorKind::Fail }))
    }
}

#[cfg(test)]
mod tests {
    use crate::sql::parser::select::{select};
    use crate::sql::parser::{Condition, GroupBy, Operator, Projection, Statement};

    #[test]
    fn test() {
        let query = "select  * ";
        let result = select(query);
        assert_eq!(result, Ok(("", Statement::Select(Projection::Star, None, None, None))));

        let query = "SELECT * FROM amex-plat";
        let result = select(query);
        assert_eq!(result, Ok(("", Statement::Select(Projection::Star, Some("amex-plat".into()), None, None))));


        let query = "select  count(*)";
        let result = select(query);
        assert_eq!(result, Ok(("", Statement::Select(Projection::Count(GroupBy::None), None, None, None))));

        let query = "select * from cba where spending > 100.0";
        let result = select(query);
        assert_eq!(result, Ok(("", Statement::Select(Projection::Star, Some("cba".into()), Some(Condition::Spending(Operator::Gt, 100.0)), None))));
    }
}