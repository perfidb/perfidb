use nom::branch::alt;
use nom::bytes::complete::{is_not, tag};
use nom::character::complete::{char, space0};
use nom::combinator::opt;
use nom::IResult;
use nom::multi::many1;
use nom::sequence::delimited;
use crate::sql::parser::non_space1;

/// Represent a labelling operation, i.e. add a label, remove a label
#[derive(PartialEq, Debug)]
pub(crate) struct LabelOp {
    pub(crate) label: String,
    pub(crate) op: Operation,
}

#[derive(PartialEq, Debug)]
pub(crate) enum Operation {
    Add,
    Remove,
}

pub(crate) fn parse_label_ops(input: &str) -> IResult<&str, Vec<LabelOp>> {
    many1(parse_single_label_op)(input)
}

fn parse_single_label_op(input: &str) -> IResult<&str, LabelOp> {
    let (input, _) = space0(input)?;
    let (input, minus) = opt(tag("-"))(input)?;
    let (input, _) = space0(input)?;
    let (input, label) = alt((
        delimited(char('\''), is_not("'"), char('\'')),
        non_space1)
    )(input)?;

    let op = match minus {
        None => Operation::Add,
        Some(_) => Operation::Remove
    };
    // Calling trim() here in case there are leading or trailing whitespace between single quotes, e.g. ' food '
    Ok((input, LabelOp { label: label.trim().to_string(), op }))
}

#[cfg(test)]
mod tests {
    use crate::db::label_op::{LabelOp, Operation, parse_label_ops};

    #[test]
    fn test() {
        let (input, label_ops) = parse_label_ops(" abc -def - ' xyz mmm'  ").unwrap();
        assert_eq!(input, "  ");
        assert_eq!(label_ops[0], LabelOp { label: String::from("abc"), op: Operation::Add });
    }
}
