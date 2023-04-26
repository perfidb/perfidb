use nom::bytes::complete::{tag_no_case};
use nom::character::complete::{multispace0};
use nom::IResult;
use nom::multi::many1;
use crate::db::label_op::{parse_label_command, parse_label_ops};
use crate::sql::parser::condition::where_parser;
use crate::sql::parser::{space_comma1, Statement};

/// Parse `UPDATE SET label = ...` pattern.
pub(crate) fn parse_update(input: &str) -> IResult<&str, Statement> {
    let (input, _) = tag_no_case("LABEL")(input)?;
    let (input, trans_ids) =  parse_trans_ids(input)?;
    let (input, label_cmd) =  parse_label_command(input)?;
    Ok((input, Statement::UpdateLabel(trans_ids, label_cmd)))
}

fn parse_trans_ids(input: &str) -> IResult<&str, Vec<u32>> {
    many1(parse_trans_id)(input)
}

fn parse_trans_id(input: &str) -> IResult<&str, u32> {
    let (input, _) = space_comma1(input)?;
    let (input, trans_id) = nom::character::complete::u32(input)?;
    Ok((input, trans_id))
}

#[cfg(test)]
mod tests {
    use crate::db::label_op::{LabelCommand, LabelOp};
    use crate::sql::parser::{Condition, Operator, Statement};
    use crate::sql::parser::update::parse_update;

    #[test]
    fn test() {
        let query = "label 100 101 a b -c";
        let (_, update_statement) = parse_update(query).unwrap();
        assert_eq!(update_statement, Statement::UpdateLabel(vec![100, 101], LabelCommand::Manual(vec![
            LabelOp::new_add("a"), LabelOp::new_add("b"), LabelOp::new_remove("c")
        ])));
    }
}