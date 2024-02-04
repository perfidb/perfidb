use std::ops::{Add, Range};
use chrono::{Datelike, Duration, NaiveDate, Utc};
use log::warn;
use nom::branch::alt;
use nom::bytes::complete::{is_not, tag, tag_no_case, take_till};
use nom::character::complete::{char, digit1, i32, multispace0, multispace1, u32};
use nom::{IResult};
use nom::error::ErrorKind;
use nom::multi::many0;
use nom::sequence::delimited;
use crate::sql::parser::{Condition, floating_point_num, LogicalOperator, Operator, yyyy_mm_dd_date};
use crate::util::{month_of, year_of};

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
        where_month,
        where_year,
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
    let (input, operator) = alt((label_eq_operator, tag_like_operator, tag_match_operator))(input)?;
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
    let (input, _) = multispace0(input)?;
    let (input, operator) = label_eq_operator(input)?;
    let (input, _) = multispace0(input)?;
    let (input, date) = yyyy_mm_dd_date(input)?;
    let (input, _) = multispace0(input)?;
    Ok((input, Condition::Date(operator, date..date + Duration::days(1))))
}

/// month = ...
fn where_month(input: &str) -> IResult<&str, Condition> {
    let (input, _) = tag_no_case("month")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, operator) = alt((label_eq_operator, between_operator))(input)?;
    let (input, date_range) = match operator {
        Operator::Between => month_range(input)?,
        Operator::Eq => month(input)?,
        _ => {
            return Err(nom::Err::Error(nom::error::Error::new(input, ErrorKind::Fail)));
        }
    };

    let (input, _) = multispace0(input)?;
    Ok((input, Condition::Date(operator, date_range)))
}

/// year = ...
fn where_year(input: &str) -> IResult<&str, Condition> {
    let (input, _) = tag_no_case("year")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, operator) = label_eq_operator(input)?;
    let (input, _) = multispace0(input)?;
    let (input, year) = i32(input)?;
    let (input, _) = multispace0(input)?;
    Ok((input, Condition::Date(operator, year_of(year))))
}

/// month can be in format 'yyyy-mm' or just a single int, e.g. 12.
fn month(input: &str) -> IResult<&str, Range<NaiveDate>> {
    alt((month_yyyy_mm, month_int))(input)
}

fn month_yyyy_mm(input: &str) -> IResult<&str, Range<NaiveDate>> {
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

fn month_int(input: &str) -> IResult<&str, Range<NaiveDate>> {
    let (input, month) = u32(input)?;
    let single_month_range = month_of(month);

    Ok((input, single_month_range))
}

fn month_range(input: &str) -> IResult<&str, Range<NaiveDate>> {
    let (input, month_from) = month(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("and")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, month_to) = month(input)?;
    Ok((input, month_from.start..month_to.end))
}

/// label = ...   label IS NULL    label IS NOT NULL
fn where_label(input: &str) -> IResult<&str, Condition> {
    let (input, _) = tag_no_case("label")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, op) = alt((label_eq_operator, label_not_eq_operator, label_is_null_operator, label_is_not_null_operator))(input)?;

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
fn label_eq_operator(input: &str) -> IResult<&str, Operator> {
    let (input, _) = tag("=")(input)?;
    let (input, _) = multispace0(input)?;
    Ok((input, Operator::Eq))
}

/// '!='
fn label_not_eq_operator(input: &str) -> IResult<&str, Operator> {
    let (input, _) = tag("!=")(input)?;
    let (input, _) = multispace0(input)?;
    Ok((input, Operator::NotEq))
}

/// IS NULL
fn label_is_null_operator(input: &str) -> IResult<&str, Operator> {
    let (input, _) = tag_no_case("IS NULL")(input)?;
    let (input, _) = multispace0(input)?;
    Ok((input, Operator::IsNull))
}

/// IS NOT NULL
fn label_is_not_null_operator(input: &str) -> IResult<&str, Operator> {
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

/// 'between'
fn between_operator(input: &str) -> IResult<&str, Operator> {
    let (input, _) = tag_no_case("between")(input)?;
    let (input, _) = multispace0(input)?;
    Ok((input, Operator::Between))
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

        let query = "where month = 12";
        let result = where_parser(query).unwrap().1;
        assert!(matches!(result, Condition::Date { .. }));
        if let Condition::Date(_, date_range) = result {
            assert_eq!(date_range.start.month(), 12);
        }

        let query = "where month = 2023-04";
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