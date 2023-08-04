use nom::branch::alt;
use nom::bytes::complete::{is_not, tag_no_case};
use nom::character::complete::{alpha1, char, multispace0, multispace1, u32};
use nom::combinator::opt;
use nom::{IResult};
use nom::Err::Error;
use nom::error::ErrorKind;
use nom::sequence::delimited;

use crate::sql::parser::{Condition, GroupBy, LogicalOperator, non_space, Operator, OrderBy, OrderByField, Projection, Statement};
use crate::sql::parser::condition::where_parser;

/// Match `SELECT` statements. This is still working-in-progress. We are trying to migrate
/// all `SELECT` syntax into this parser.

/// Parse `SELECT *` pattern.
pub(crate) fn select(input: &str) -> IResult<&str, Statement> {
    let (input, _) = tag_no_case("SELECT")(input)?;
    let (input, _) =  multispace1(input)?;

    // Check if there are special 'where condition' specified here as a projection.
    // E.g. user can do 'SELECT spending WHERE date = 7', it is a shortcut syntax for 'SELECT * WHERE date = 7 AND spending >= 0'
    // let (input, projection_condition) = opt(alt((parse_implied_where_spending, parse_implied_where_income)))(input)?;
    let (input, (projection, implied_condition)) = alt((
        parse_star,
        parse_sum,
        parse_count,
        parse_implied_where_spending,
        parse_implied_where_income,
        parse_auto,
        parse_trans_id
    ))(input)?;

    let (input, account) = opt(from_account)(input)?;
    let (input, condition) = opt(where_parser)(input)?;
    let condition = match condition {
        None => implied_condition,
        Some(where_condition) => match implied_condition {
            None => Some(where_condition),
            Some(implied_condition) => Some(Condition::from_logical(&LogicalOperator::And, where_condition, implied_condition))
        }
    };

    let (input, _) =  multispace0(input)?;
    let (input, order_by) = parse_order_by(input)?;
    let (input, _) =  multispace0(input)?;
    let (input, limit) = parse_limit(input)?;
    let (input, _) =  multispace0(input)?;
    let (input, group_by) = opt(group_by)(input)?;
    Ok((input, Statement::Select(projection, account, condition, order_by, limit, group_by)))
}

/// SUM(*), SUM(spending), SUM(income)
fn parse_sum(input: &str) -> IResult<&str, (Projection, Option<Condition>)> {
    let (input, _) = tag_no_case("SUM")(input)?;
    let (input, sum_arg) = delimited(char('('), is_not(")"), char(')'))(input)?;
    let (input, _) =  multispace0(input)?;
    match sum_arg.to_lowercase().as_str() {
        "spending" => Ok((input, (Projection::Sum, Some(Condition::Spending(Operator::GtEq, 0.0))))),
        "income" => Ok((input, (Projection::Sum, Some(Condition::Income(Operator::GtEq, 0.0))))),
        _ => Ok((input, (Projection::Sum, None)))
    }
}

/// COUNT(*), COUNT(spending), COUNT(income)
fn parse_count(input: &str) -> IResult<&str, (Projection, Option<Condition>)> {
    let (input, _) = tag_no_case("COUNT")(input)?;
    let (input, count_arg) = delimited(char('('), is_not(")"), char(')'))(input)?;
    let (input, _) =  multispace0(input)?;
    match count_arg.to_lowercase().as_str() {
        "spending" => Ok((input, (Projection::Count, Some(Condition::Spending(Operator::GtEq, 0.0))))),
        "income" => Ok((input, (Projection::Count, Some(Condition::Income(Operator::GtEq, 0.0))))),
        _ => Ok((input, (Projection::Count, None)))
    }
}

/// Normal projection, SELECT * ...
fn parse_star(input: &str) -> IResult<&str, (Projection, Option<Condition>)> {
    let (input, _) = tag_no_case("*")(input)?;
    let (input, _) = multispace0(input)?;
    Ok((input, (Projection::Star, None)))
}

/// If we see 'SELECT spending ...' it is an implied where clause, need to add to other where clauses later.
fn parse_implied_where_spending(input: &str) -> IResult<&str, (Projection, Option<Condition>)> {
    let (input, _) = tag_no_case("spending")(input)?;
    let (input, _) =  multispace0(input)?;
    Ok((input, (Projection::Star, Some(Condition::Spending(Operator::GtEq, 0.0)))))
}

/// If we see 'SELECT income ...' it is an implied where clause, need to add to other where clauses later.
fn parse_implied_where_income(input: &str) -> IResult<&str, (Projection, Option<Condition>)> {
    let (input, _) = tag_no_case("income")(input)?;
    let (input, _) =  multispace0(input)?;
    Ok((input, (Projection::Star, Some(Condition::Income(Operator::GtEq, 0.0)))))
}

/// AUTO(*)
fn parse_auto(input: &str) -> IResult<&str, (Projection, Option<Condition>)> {
    let (input, _) = tag_no_case("AUTO()")(input)?;
    let (input, _) = multispace0(input)?;
    Ok((input, (Projection::Auto, None)))
}

/// SELECT 123
fn parse_trans_id(input: &str) -> IResult<&str, (Projection, Option<Condition>)> {
    let (input, trans_id) = u32(input)?;
    let (input, _) = multispace0(input)?;
    Ok((input, (Projection::Id(trans_id), None)))
}

/// FROM account
pub(crate) fn from_account(input: &str) -> IResult<&str, String> {
    let (input, _) = tag_no_case("FROM")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, account) = non_space(input)?;
    let (input, _) = multispace0(input)?;
    Ok((input, account.into()))
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

fn parse_order_by(input: &str) -> IResult<&str, OrderBy> {
    let (input, order_by_clause) = opt(tag_no_case("order by"))(input)?;
    match order_by_clause {
        None => Ok((input, OrderBy::date())),
        Some(_) => {
            let (input, _) =  multispace1(input)?;
            let (input, field) = alt((order_by_date, order_by_amount))(input)?;
            let (input, desc) = opt(tag_no_case("desc"))(input)?;
            Ok((input, OrderBy { field, desc: desc.is_some() }))
        }
    }
}

fn order_by_date(input: &str) -> IResult<&str, OrderByField> {
    let (input, _) = tag_no_case("date")(input)?;
    let (input, _) =  multispace0(input)?;
    Ok((input, OrderByField::Date))
}

fn order_by_amount(input: &str) -> IResult<&str, OrderByField> {
    let (input, _) = tag_no_case("amount")(input)?;
    let (input, _) =  multispace0(input)?;
    Ok((input, OrderByField::Amount))
}

fn parse_limit(input: &str) -> IResult<&str, Option<usize>> {
    let (input, limit) = opt(tag_no_case("limit"))(input)?;
    match limit {
        Some(_) => {
            let (input, _) =  multispace1(input)?;
            let (input, result) = nom::character::complete::u64(input)?;
            Ok((input, Some(result as usize)))
        },
        None => Ok((input, None))
    }
}

#[cfg(test)]
mod tests {
    use crate::sql::parser::select::{select};
    use crate::sql::parser::{Condition, GroupBy, Operator, OrderBy, Projection, Statement};

    #[test]
    fn test() {
        let query = "select  * ";
        let result = select(query);
        assert_eq!(result, Ok(("", Statement::Select(Projection::Star, None, None, OrderBy::date(), None, None))));

        let query = "select income order by amount DESC";
        let result = select(query);
        assert_eq!(result, Ok(("", Statement::Select(Projection::Star, None, Some(Condition::Income(Operator::GtEq, 0.0)), OrderBy::amount_desc(), None, None))));

        let query = "SELECT * FROM amex-plat LIMIT 5";
        let result = select(query);
        assert_eq!(result, Ok(("", Statement::Select(Projection::Star, Some("amex-plat".into()), None, OrderBy::date(), Some(5), None))));


        let query = "SELECT SUM(spending) from cba";
        let result = select(query);
        assert_eq!(result, Ok(("", Statement::Select(Projection::Sum, Some("cba".into()), Some(Condition::Spending(Operator::GtEq, 0.0)), OrderBy::date(), None, None))));

        let query = "SELECT sum(income)";
        let result = select(query);
        assert_eq!(result, Ok(("", Statement::Select(Projection::Sum, None, Some(Condition::Income(Operator::GtEq, 0.0)), OrderBy::date(), None, None))));

        let query = "select  count(*)";
        let result = select(query);
        assert_eq!(result, Ok(("", Statement::Select(Projection::Count, None, None, OrderBy::date(), None, None))));

        let query = "select count(spending) from cba where spending < 100.0 limit 4 group by label";
        let result = select(query);
        assert_eq!(result, Ok(("", Statement::Select(
            Projection::Count,
            Some("cba".into()),
            Some(Condition::And(Box::new((Condition::Spending(Operator::Lt, 100.0), Condition::Spending(Operator::GtEq, 0.0))))),
            OrderBy::date(), Some(4), Some(GroupBy::Label)))));

        let query = "select * from cba where spending > 100.0 order by amount desc group by label";
        let result = select(query);
        assert_eq!(result, Ok(("", Statement::Select(Projection::Star, Some("cba".into()), Some(Condition::Spending(Operator::Gt, 100.0)), OrderBy::amount_desc(), None, Some(GroupBy::Label)))));
    }
}