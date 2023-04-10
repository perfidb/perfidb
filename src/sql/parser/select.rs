use nom::branch::alt;
use nom::bytes::complete::{tag_no_case};
use nom::character::complete::{multispace0, multispace1};
use nom::combinator::opt;
use nom::{IResult};

use crate::sql::parser::{non_space, Projection, Statement};
use crate::sql::parser::condition::where_parser;

/// Match `SELECT` statements. This is still working-in-progress. We are trying to migrate
/// all `SELECT` syntax into this parser.

/// Parse `SELECT *` pattern.
pub(crate) fn select(input: &str) -> IResult<&str, Statement> {
    let (input, _) = tag_no_case("SELECT")(input)?;
    let (input, _) =  multispace1(input)?;
    alt((select_star, select_count))(input)
}

/// SELECT *
pub(crate) fn select_star(input: &str) -> IResult<&str, Statement> {
    let (input, _) = tag_no_case("*")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, account) = opt(from_account)(input)?;
    let (input, condition) = opt(where_parser)(input)?;
    Ok((input, Statement::Select(Projection::Star, account, condition)))
}

/// FROM account
pub(crate) fn from_account(input: &str) -> IResult<&str, String> {
    let (input, _) = tag_no_case("FROM")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, account) = non_space(input)?;
    let (input, _) = multispace0(input)?;
    Ok((input, account.into()))
}

/// SELECT COUNT(*)
pub(crate) fn select_count(input: &str) -> IResult<&str, Statement> {
    let (input, _) = tag_no_case("COUNT(*)")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, account) = opt(from_account)(input)?;
    let (input, condition) = opt(where_parser)(input)?;
    Ok((input, Statement::Select(Projection::Count, account, condition)))
}


#[cfg(test)]
mod tests {
    use crate::sql::parser::select::{select};
    use crate::sql::parser::{Condition, Operator, Projection, Statement};

    #[test]
    fn test() {
        let query = "select  * ";
        let result = select(query);
        assert_eq!(result, Ok(("", Statement::Select(Projection::Star, None, None))));

        let query = "SELECT * FROM amex-plat";
        let result = select(query);
        assert_eq!(result, Ok(("", Statement::Select(Projection::Star, Some("amex-plat".into()), None))));


        let query = "select  count(*)";
        let result = select(query);
        assert_eq!(result, Ok(("", Statement::Select(Projection::Count, None, None))));

        let query = "select * from cba where spending > 100.0";
        let result = select(query);
        assert_eq!(result, Ok(("", Statement::Select(Projection::Star, Some("cba".into()), Some(Condition::Spending(Operator::Gt, 100.0))))));
    }
}