use nom::branch::alt;
use nom::bytes::complete::{is_not, tag, tag_no_case};
use nom::character::complete::{char, space0};
use nom::combinator::opt;
use nom::IResult;
use nom::multi::many1;
use nom::sequence::delimited;
use crate::parser::non_space1;

/// Represent a labelling command. Currently supporting two types of command.
/// Manual - manually specifying a list of add / remove labels.
/// Auto - use auto labelling rule defined in config file.
#[derive(PartialEq, Debug, Clone)]
pub(crate) enum LabelCommand {
    Manual(Vec<LabelOp>),

    /// When auto labelling is specified, all existing labels are wiped out
    Auto,
}

/// Represent a labelling operation, i.e. add a label, remove a label
#[derive(PartialEq, Debug, Clone)]
pub(crate) struct LabelOp {
    pub(crate) label: String,
    pub(crate) op: Operation,
}

impl LabelOp {
    pub(crate) fn new_add(label: &str) -> LabelOp {
        LabelOp {
            label: label.into(),
            op: Operation::Add
        }
    }

    pub(crate) fn new_remove(label: &str) -> LabelOp {
        LabelOp {
            label: label.into(),
            op: Operation::Remove
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub(crate) enum Operation {
    Add,
    Remove,
}

pub(crate) fn parse_label_command(input: &str) -> IResult<&str, LabelCommand> {
    let (input, _) = space0(input)?;
    let (input, auto) = opt(tag_no_case("auto()"))(input)?;
    if auto.is_some() {
        return Ok((input, LabelCommand::Auto));
    }

    let (input, label_ops) = parse_label_ops(input)?;
    Ok((input, LabelCommand::Manual(label_ops)))
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
