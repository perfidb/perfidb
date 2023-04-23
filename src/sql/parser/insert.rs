use nom::bytes::complete::{is_not, tag_no_case};
use nom::character::complete::{char, multispace0, multispace1};
use nom::combinator::opt;
use nom::IResult;
use nom::multi::many1;
use nom::sequence::delimited;
use crate::csv_reader::Record;
use crate::sql::parser::{comma, floating_point_num, non_space, Statement, yyyy_mm_dd_date};

pub(crate) fn parse_insert(input: &str) -> IResult<&str, Statement> {
    let (input, _) = tag_no_case("INSERT")(input)?;
    let (input, _) =  multispace1(input)?;
    let (input, account) = opt(parse_account)(input)?;
    let (input, _) = tag_no_case("VALUES")(input)?;
    let (input, _) =  multispace0(input)?;
    let (input, records) = many1(parse_record)(input)?;
    Ok((input, Statement::Insert(account, records)))
}

fn parse_account(input: &str) -> IResult<&str, String> {
    let (input, _) = tag_no_case("INTO")(input)?;
    let (input, _) =  multispace1(input)?;
    let (input, account) = non_space(input)?;
    let (input, _) =  multispace0(input)?;
    Ok((input, account.into()))
}

fn parse_record(input: &str) -> IResult<&str, Record> {
    let (input, _) = opt(comma)(input)?;
    let (input, _) = multispace0(input)?;
    let (input, record) = delimited(char('('), parse_record_inner, char(')'))(input)?;
    Ok((input, record))
}

fn parse_record_inner(input: &str) -> IResult<&str, Record> {
    let (input, _) = multispace0(input)?;
    let (input, date) = yyyy_mm_dd_date(input)?;
    let (input, _) = comma(input)?;
    let (input, desc) = delimited(char('\''), is_not("'"), char('\''))(input)?;
    let (input, _) = comma(input)?;
    let (input, amount) = floating_point_num(input)?;
    let (input, labels) = opt(parse_record_labels)(input)?;

    Ok((input, Record {
        id: None,
        account: "".to_string(),
        date: date.and_hms_opt(0, 0, 0).unwrap(),
        description: desc.into(),
        amount,
        labels,
    }))
}

/// Parse additional labels argument in VALUES ( ... )
fn parse_record_labels(input: &str) -> IResult<&str, Vec<String>> {
    let (input, _) = comma(input)?;
    let (input, labels) = delimited(char('\''), is_not("'"), char('\''))(input)?;
    let labels = labels.split(&[',', ' ']).filter(|s| !s.is_empty()).map(str::to_string).collect::<Vec<String>>();
    Ok((input, labels))
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;
    use crate::sql::parser::insert::parse_insert;
    use crate::sql::parser::Statement;

    #[test]
    fn test() {
        let statement = "INSERT VALUES (2020-11-03, 'food', -30.45, 'dining, lunch'), (2022-01-20, 'computer', -2000)";
        let result = parse_insert(statement).unwrap().1;
        assert!(matches!(result, Statement::Insert(..)));
        if let Statement::Insert(account, records) = result {
            assert!(account.is_none());
            assert_eq!(records[0].labels, Some(vec!["dining".to_string(), "lunch".to_string()]));
            assert_eq!(records[1].date.date(), NaiveDate::from_ymd_opt(2022, 1, 20).unwrap());
        }
    }
}