use std::ops::{Add, Range};
use chrono::{Datelike, Duration, NaiveDate, Utc};
use log::warn;
use nom::branch::alt;
use nom::bytes::complete::{is_not, tag, tag_no_case, take_till};
use nom::character::complete::{char, digit1, multispace0, multispace1, u32};
use nom::{IResult};
use nom::multi::many0;
use nom::sequence::delimited;
use crate::sql::parser::{Condition, floating_point_num, LogicalOperator, Operator, yyyy_mm_dd_date};

/// WHERE ...
pub(crate) fn where_parser(input: &str) -> IResult<&str, Condition> {
    let (input, _) = tag_no_case("WHERE")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, first_condition) = single_condition(input)?;

    // Followed by 0 or more AND/OR conditions
    match many0(alt((and_condition, or_condition)))(input) {
        Ok((input, more_conditions)) => {
            if more_conditions.is_empty() {
                Ok((input, first_condition))
            } else {
                Ok((input, combine_logical_conditions(first_condition, more_conditions)))
            }
        },
        Err(_) => {
            warn!("Unable to parse additional where condition {}", input);
            Ok((input, first_condition))
        }
    }
}

fn combine_logical_conditions(first: Condition, logical_conditions: Vec<(LogicalOperator, Condition)>) -> Condition {
    let mut current = first;
    for (logical_op, next_cond) in logical_conditions {
        current = Condition::from_logical(&logical_op, current, next_cond);
    }

    current
}

fn single_condition(input: &str) -> IResult<&str, Condition> {
    let (input, condition) = alt((
        where_id,
        where_spending,
        where_income,
        where_amount,
        where_description,
        where_date,
        where_label))(input)?;
    let (input, _) = multispace0(input)?;
    Ok((input, condition))
}

/// AND single_condition
fn and_condition(input: &str) -> IResult<&str, (LogicalOperator, Condition)> {
    let (input, _) = tag_no_case("AND")(input)?;
    let (input, _) = multispace1(input)?;
    single_condition(input).map(|(input, c)|(input, (LogicalOperator::And, c)))
}

/// OR single_condition
fn or_condition(input: &str) -> IResult<&str, (LogicalOperator, Condition)> {
    let (input, _) = tag_no_case("OR")(input)?;
    let (input, _) = multispace1(input)?;
    single_condition(input).map(|(input, c)|(input, (LogicalOperator::Or, c)))
}

/// id = 123
fn where_id(input: &str) -> IResult<&str, Condition> {
    let (input, _) = tag_no_case("id")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = tag("=")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, id) = u32(input)?;
    Ok((input, Condition::Id(id)))
}

/// spending > 100.0
fn where_spending(input: &str) -> IResult<&str, Condition> {
    let (input, _) = tag_no_case("spending")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, compare_operator) = take_till(|c| c != '<' && c != '>' && c != '=')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, value) = floating_point_num(input)?;
    Ok((input, Condition::Spending(compare_operator.into(), value)))
}

/// income > 100.0
fn where_income(input: &str) -> IResult<&str, Condition> {
    let (input, _) = tag_no_case("income")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, compare_operator) = take_till(|c| c != '<' && c != '>' && c != '=')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, value) = floating_point_num(input)?;
    Ok((input, Condition::Income(compare_operator.into(), value)))
}

/// amount < -100.0
fn where_amount(input: &str) -> IResult<&str, Condition> {
    let (input, _) = tag_no_case("amount")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, compare_operator) = take_till(|c| c != '<' && c != '>' && c != '=')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, value) = floating_point_num(input)?;
    Ok((input, Condition::Amount(compare_operator.into(), value)))
}

/// description|desc =|like|match '...'
fn where_description(input: &str) -> IResult<&str, Condition> {
    let (input, _) = alt((tag_description_multispace1, tag_desc_multispace1))(input)?;
    let (input, operator) = alt((tag_eq_operator, tag_like_operator, tag_match_operator))(input)?;
    let (input, text) = delimited(char('\''), is_not("'"), char('\''))(input)?;
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
    let (input, date_range) = alt((yyyy_mm_dd, yyyy_mm, single_month_int))(input)?;
    let (input, _) = multispace0(input)?;
    Ok((input, Condition::Date(operator, date_range)))
}

fn yyyy_mm_dd(input: &str) -> IResult<&str, Range<NaiveDate>> {
    let (input, date) = yyyy_mm_dd_date(input)?;
    Ok((input, date..date.add(Duration::days(1))))
}

fn yyyy_mm(input: &str) -> IResult<&str, Range<NaiveDate>> {
    let (input, year) = digit1(input)?;
    let (input, _) = tag("-")(input)?;
    let (input, month) = digit1(input)?;

    let year = year.to_string().parse::<i32>().unwrap();
    let month = month.to_string().parse::<u32>().unwrap();

    let first_day = NaiveDate::from_ymd_opt(year, month, 1).unwrap();
    let next_month = if month == 12 { 1 } else { month + 1 };
    let next_month_year = if month == 12 { year + 1 } else { year };
    let first_day_next_month = NaiveDate::from_ymd_opt(next_month_year, next_month, 1).unwrap();

    Ok((input, first_day..first_day_next_month))
}

fn single_month_int(input: &str) -> IResult<&str, Range<NaiveDate>> {
    let (input, month) = u32(input)?;
    let mut month = month % 12;
    if month == 0 {
        month = 12;
    }

    let today = Utc::now().naive_utc().date();
    let mut year = today.year();
    if month >= today.month() {
        year -= 1;
    }

    let first_day = NaiveDate::from_ymd_opt(year, month, 1).unwrap();
    let next_month = if month == 12 { 1 } else { month + 1 };
    let next_month_year = if month == 12 { year + 1 } else { year };
    let first_day_next_month = NaiveDate::from_ymd_opt(next_month_year, next_month, 1).unwrap();

    Ok((input, first_day..first_day_next_month))
}

/// label = ...   label IS NULL    label IS NOT NULL
fn where_label(input: &str) -> IResult<&str, Condition> {
    let (input, _) = tag_no_case("label")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, op) = alt((tag_eq_operator, tag_is_null_operator, tag_is_not_null_operator))(input)?;

    // If we see 'IS NULL' or 'IS NOT NULL' there is no need to parse the labels, we just return empty string label here
    match op {
        Operator::IsNull | Operator::IsNotNull => Ok((input, Condition::Label(op, "".into()))),
        _ => {
            let (input, labels) = delimited(char('\''), is_not("'"), char('\''))(input)?;
            Ok((input, Condition::Label(op, labels.into())))
        }
    }
}


/// '='
fn tag_eq_operator(input: &str) -> IResult<&str, Operator> {
    let (input, _) = tag_no_case("=")(input)?;
    let (input, _) = multispace0(input)?;
    Ok((input, Operator::Eq))
}

/// IS NULL
fn tag_is_null_operator(input: &str) -> IResult<&str, Operator> {
    let (input, _) = tag_no_case("IS NULL")(input)?;
    let (input, _) = multispace0(input)?;
    Ok((input, Operator::IsNull))
}

/// IS NOT NULL
fn tag_is_not_null_operator(input: &str) -> IResult<&str, Operator> {
    let (input, _) = tag_no_case("IS NOT NULL")(input)?;
    let (input, _) = multispace0(input)?;
    Ok((input, Operator::IsNotNull))
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


#[cfg(test)]
mod tests {
    use chrono::{Datelike, NaiveDate};
    use crate::sql::parser::{Condition, Operator};
    use crate::sql::parser::condition::where_parser;

    #[test]
    fn test() {
        let query = "where spending > 100.0";
        let result = where_parser(query);
        assert_eq!(result, Ok(("", Condition::Spending(Operator::Gt, 100.0))));

        let query = "WHERE income >= 1000";
        let result = where_parser(query);
        assert_eq!(result, Ok(("", Condition::Income(Operator::GtEq, 1000.0))));

        let query = "where desc  match 'abc'";
        let result = where_parser(query);
        assert_eq!(result, Ok(("", Condition::Description(Operator::Match, "abc".into()))));

        let query = "where description like 'abc'";
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


        let query = "WHERE desc like 'abc' AND spending > 1000";
        let result = where_parser(query).unwrap().1;
        assert_eq!(result, Condition::And(Box::new((
            Condition::Description(Operator::Match, "abc".into()),
            Condition::Spending(Operator::Gt, 1000.0)
        ))));

        let query = "WHERE desc like 'abc' AND spending > 1000 OR income < 30";
        let result = where_parser(query).unwrap().1;
        assert_eq!(result, Condition::Or(
            Box::new((
                Condition::And(Box::new((
                    Condition::Description(Operator::Match, "abc".into()),
                    Condition::Spending(Operator::Gt, 1000.0)
                ))),
                Condition::Income(Operator::Lt, 30.0))
            ))
        );
    }
}