use std::ops::{Add, Range};
use chrono::{Datelike, Duration, NaiveDate, Utc};
use nom::branch::alt;
use nom::bytes::complete::{is_not, tag_no_case, take, take_till};
use nom::character::complete::{char, multispace0, multispace1};
use nom::combinator::opt;
use nom::{AsChar, InputTakeAtPosition, IResult};
use nom::error::{Error, ErrorKind};
use nom::sequence::{delimited, preceded};
use regex::Regex;
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
    alt((where_spending,
         where_income,
         where_amount,
         where_description,
         where_date,
         where_label))(input)
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


/// date = ...
fn where_date(input: &str) -> IResult<&str, Condition> {
    let (input, _) = tag_no_case("date")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, operator) = tag_eq_operator(input)?;
    let (input, date_range) = parse_date_range(input)?;

    Ok((input, Condition::Date(operator, date_range)))
}

fn parse_date_range(date_str: &str) -> IResult<&str, Range<NaiveDate>> {
    // if month
    if let Ok(month) = date_str.parse::<u32>() {
        if (1..=12).contains(&month) {
            let today = Utc::now().naive_utc().date();
            let mut year = today.year();
            if month >= today.month() {
                year -= 1;
            }

            let first_day = NaiveDate::from_ymd_opt(year, month, 1).unwrap();
            let next_month = if month == 12 { 1 } else { month + 1 };
            let next_month_year = if month == 12 { year + 1 } else { year };
            let first_day_next_month = NaiveDate::from_ymd_opt(next_month_year, next_month, 1).unwrap();

            return Ok(("", first_day..first_day_next_month));
        }
    } else {
        // Handle date format '2022-09-03'
        let yyyy_mm_dd = Regex::new(r"^\d{4}-\d{2}-\d{2}$").unwrap();
        if yyyy_mm_dd.is_match(date_str) {
            let date = NaiveDate::parse_from_str(date_str, "%Y-%m-%d").unwrap();
            return Ok(("", date..date.add(Duration::days(1))));
        }

        let yyyy_mm = Regex::new(r"^\d{4}-\d{2}$").unwrap();
        if yyyy_mm.is_match(date_str) {
            let splitted: Vec<&str> = date_str.split('-').collect();
            let year = splitted[0].to_string().parse::<i32>().unwrap();
            let month = splitted[1].to_string().parse::<u32>().unwrap();

            let first_day = NaiveDate::from_ymd_opt(year, month, 1).unwrap();
            let next_month = if month == 12 { 1 } else { month + 1 };
            let next_month_year = if month == 12 { year + 1 } else { year };
            let first_day_next_month = NaiveDate::from_ymd_opt(next_month_year, next_month, 1).unwrap();

            return Ok(("", first_day..first_day_next_month));
        }
    }

    return Err(nom::Err::Failure(Error::new(date_str, ErrorKind::Fail)));
}



/// label = ...
fn where_label(input: &str) -> IResult<&str, Condition> {
    let (input, _) = tag_no_case("label")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, operator) = tag_eq_operator(input)?;
    let (input, labels) = delimited(char('\''), is_not("'"), char('\''))(input)?;
    Ok((input, Condition::Label(operator, labels.into())))
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
    use chrono::{Datelike, NaiveDate};
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

        let query = "where date = 12";
        let result = where_parser(query).unwrap().1;
        assert!(matches!(result, Condition::Date { .. }));
        if let Condition::Date(_, date_range) = result {
            assert_eq!(date_range.start.month(), 12);
        }

        let query = "where date = 2023-04";
        let result = where_parser(query).unwrap().1;
        assert!(matches!(result, Condition::Date { .. }));
        if let Condition::Date(_, date_range) = result {
            assert_eq!(date_range.start, NaiveDate::from_ymd_opt(2023, 4, 1).unwrap());
            assert_eq!(date_range.end, NaiveDate::from_ymd_opt(2023, 5, 1).unwrap());
        }

        let query = "where label = 'abc, def'";
        let result = where_parser(query);
        assert_eq!(result, Ok(("", Condition::Label(Operator::Eq, "abc, def".into()))));
    }
}