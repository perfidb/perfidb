use nom::bytes::complete::tag_no_case;
use nom::character::complete::{multispace0, multispace1};
use nom::IResult;
use nom::multi::many1;
use crate::sql::parser::Statement;

pub(crate) fn parse_delete(input: &str) -> IResult<&str, Statement> {
    let (input, _) = tag_no_case("DELETE")(input)?;
    let (input, trans_ids) = many1(parse_transaction_id)(input)?;
    Ok((input, Statement::Delete(trans_ids)))
}

fn parse_transaction_id(input: &str) -> IResult<&str, u32> {
    let (input, _) = multispace0(input)?;
    let (input, trans_id) = nom::character::complete::u32(input)?;
    Ok((input, trans_id))
}
