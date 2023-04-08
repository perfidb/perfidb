use nom::bytes::complete::{is_not, tag, tag_no_case};
use nom::character::complete::{char, multispace0, multispace1};
use nom::combinator::opt;
use nom::IResult;
use nom::sequence::delimited;
use crate::sql::parser::condition::where_parser;
use crate::sql::parser::Statement;

/// Parse `UPDATE SET label = ...` pattern.
pub(crate) fn update(input: &str) -> IResult<&str, Statement> {
    let (input, _) = tag_no_case("UPDATE")(input)?;
    let (input, _) =  multispace1(input)?;
    let (input, _) = tag_no_case("SET")(input)?;
    let (input, _) =  multispace1(input)?;
    let (input, _) = tag_no_case("label")(input)?;
    let (input, _) =  multispace0(input)?;
    let (input, _) = tag("=")(input)?;
    let (input, _) =  multispace0(input)?;
    let (input, labels) = delimited(char('\''), is_not("'"), char('\''))(input)?;
    let (input, _) =  multispace0(input)?;
    let (input, condition) = opt(where_parser)(input)?;
    Ok((input, Statement::UpdateLabel(labels.into(), condition)))
}

#[cfg(test)]
mod tests {
    use crate::sql::parser::{Condition, Operator, Statement};
    use crate::sql::parser::update::update;

    #[test]
    fn test() {
        let query = "update set label = 'a,b,c'";
        let (_, update_statement) = update(query).unwrap();
        assert_eq!(update_statement, Statement::UpdateLabel("a,b,c".into(), None));

        let query = "update set label = 'grocery' where desc like 'woolworths'";
        let (_, update_statement) = update(query).unwrap();
        assert_eq!(update_statement, Statement::UpdateLabel("grocery".into(), Some(Condition::Description(Operator::Match, "woolworths".into()))));
    }
}