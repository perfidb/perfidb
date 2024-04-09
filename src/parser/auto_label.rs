use nom::bytes::complete::tag_no_case;
use nom::character::complete::{multispace0, multispace1};
use nom::combinator::opt;
use nom::IResult;
use crate::parser::condition::where_parser;
use crate::parser::Statement;

pub(crate) fn auto_label(input: &str) -> IResult<&str, Statement> {
    let (input, _) = tag_no_case("AUTO_LABEL")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, run) = opt(tag_no_case("RUN"))(input)?;
    let (input, _) = multispace0(input)?;
    let (input, condition) = where_parser(input)?;

    Ok((input, Statement::AutoLabel(condition, run.is_some())))
}

#[cfg(test)]
mod tests {
    use crate::parser::{Condition, Statement};
    use crate::parser::auto_label::auto_label;

    #[test]
    fn test() {
        let query = "auto_label run where id = 3";
        let result = auto_label(query);
        assert_eq!(result, Ok(("", Statement::AutoLabel(Condition::Id(3), true))));

        let query = "auto_label where id = 3";
        let result = auto_label(query);
        assert_eq!(result, Ok(("", Statement::AutoLabel(Condition::Id(3), false))));
    }
}
