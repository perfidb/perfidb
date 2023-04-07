use nom::branch::alt;
use nom::bytes::complete::{tag_no_case, take, take_till};
use nom::character::complete::{multispace0, multispace1};
use nom::combinator::opt;
use nom::{AsChar, InputTakeAtPosition, IResult};
use nom::error::Error;
use nom::sequence::preceded;
use crate::sql::parser::{Condition, non_space, Operator, Projection, Statement};

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
    alt((where_spending, where_income, where_amount, where_description))(input)
}

/// spending > 100.0
fn where_spending(input: &str) -> IResult<&str, Condition> {
    let (input, _) = tag_no_case("spending")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, compare_operator) = take_till(|c| c != '<' && c != '>' && c != '=')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, value) = floating_point_num(input)?;
    Ok((input, Condition::Spending(compare_operator.into(), value.parse::<f32>().unwrap())))
}

/// income > 100.0
fn where_income(input: &str) -> IResult<&str, Condition> {
    let (input, _) = tag_no_case("income")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, compare_operator) = take_till(|c| c != '<' && c != '>' && c != '=')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, value) = floating_point_num(input)?;
    Ok((input, Condition::Income(compare_operator.into(), value.parse::<f32>().unwrap())))
}

/// amount < -100.0
fn where_amount(input: &str) -> IResult<&str, Condition> {
    let (input, _) = tag_no_case("amount")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, compare_operator) = take_till(|c| c != '<' && c != '>' && c != '=')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, value) = floating_point_num(input)?;
    Ok((input, Condition::Amount(compare_operator.into(), value.parse::<f32>().unwrap())))
}

/// description|desc =|like|match '...'
fn where_description(input: &str) -> IResult<&str, Condition> {
    let (input, _) = alt((tag_description_multispace1, tag_desc_multispace1))(input)?;
    let (input, operator) = alt((tag_eq_operator, tag_like_operator, tag_match_operator))(input)?;
    let (input, text) = non_space(input)?;
    Ok((input, Condition::Description(operator, text.into())))
}


/// 'description '
fn tag_description_multispace1(input: &str) -> IResult<&str, ()> {
    let (input, _) = tag_no_case("description")(input)?;
    let (input, _) = multispace1(input)?;
    Ok((input, ()))
}

/// 'desc '
fn tag_desc_multispace1(input: &str) -> IResult<&str, ()> {
    let (input, _) = tag_no_case("desc")(input)?;
    let (input, _) = multispace1(input)?;
    Ok((input, ()))
}

/// '='
fn tag_eq_operator(input: &str) -> IResult<&str, Operator> {
    let (input, _) = tag_no_case("=")(input)?;
    let (input, _) = multispace0(input)?;
    Ok((input, Operator::Eq))
}

/// 'like'
fn tag_like_operator(input: &str) -> IResult<&str, Operator> {
    let (input, _) = tag_no_case("like")(input)?;
    let (input, _) = multispace0(input)?;
    Ok((input, Operator::Match))
}

/// 'match'
fn tag_match_operator(input: &str) -> IResult<&str, Operator> {
    let (input, _) = tag_no_case("match")(input)?;
    let (input, _) = multispace0(input)?;
    Ok((input, Operator::Match))
}



fn floating_point_num(input: &str) -> IResult<&str, &str> {
    input.split_at_position_complete(|c| {
        let c = c.as_char();
        !(c.is_dec_digit() || c == '.' || c == '-')
    })
}

#[cfg(test)]
mod tests {
    use crate::sql::parser::select::{select, where_parser};
    use crate::sql::parser::{Condition, Operator, Projection, Statement};

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
        assert_eq!(result, Ok(("", Statement::Select(Projection::Star, Some(Condition::Spending(Operator::Gt, 100.0))))));

        let query = "WHERE income >= 1000";
        let result = where_parser(query);
        assert_eq!(result, Ok(("", Condition::Income(Operator::GtEq, 1000.0))));

        let query = "where desc  match 'abc'";
        let result = where_parser(query);
        assert_eq!(result, Ok(("", Condition::Description(Operator::Match, "'abc'".into()))));

        let query = "where description like abc";
        let result = where_parser(query);
        assert_eq!(result, Ok(("", Condition::Description(Operator::Match, "abc".into()))));
    }
}