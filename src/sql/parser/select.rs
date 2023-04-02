use nom::branch::alt;
use nom::bytes::complete::{tag_no_case, take, take_till};
use nom::character::complete::{multispace0, multispace1};
use nom::combinator::opt;
use nom::{AsChar, InputTakeAtPosition, IResult};
use nom::sequence::preceded;
use crate::common::Error;
use crate::sql::parser::{Condition, Projection, Statement};

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
    let (input, condition) = opt(where_parser)(input)?;
    Ok((input, Statement::Select(Projection::Star, condition)))
}

/// SELECT COUNT(*)
pub(crate) fn select_count(input: &str) -> IResult<&str, Statement> {
    let (input, _) = tag_no_case("COUNT(*)")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, condition) = opt(where_parser)(input)?;
    Ok((input, Statement::Select(Projection::Count, None)))
}

/// WHERE ...
fn where_parser(input: &str) -> IResult<&str, Condition> {
    let (input, _) = tag_no_case("WHERE")(input)?;
    let (input, _) = multispace1(input)?;
    alt((where_spending, where_spending))(input)
}

fn where_spending(input: &str) -> IResult<&str, Condition> {
    let (input, _) = tag_no_case("spending")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, compare_operator) = take_till(|c| c != '<' && c != '>' && c != '=')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, value) = floating_point_num(input)?;
    Ok((input, Condition::Spending(value.parse::<f32>().unwrap())))
}

fn floating_point_num(input: &str) -> IResult<&str, &str> {
    input.split_at_position_complete(|c| {
        let c = c.as_char();
        !(c.is_dec_digit() || c == '.' || c == '-')
    })
}

#[cfg(test)]
mod tests {
    use crate::sql::parser::select::select;
    use crate::sql::parser::{Condition, Projection, Statement};

    #[test]
    fn test() {
        let query = "select  * ";
        let result = select(query);
        assert_eq!(result, Ok(("", Statement::Select(Projection::Star, None))));

        let query = "select  count(*)";
        let result = select(query);
        assert_eq!(result, Ok(("", Statement::Select(Projection::Count, None))));

        let query = "select * where spending > 100.0";
        let result = select(query);
        assert_eq!(result, Ok(("", Statement::Select(Projection::Star, Some(Condition::Spending(100.0))))));
    }
}