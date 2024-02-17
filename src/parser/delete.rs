use log::warn;
use nom::bytes::complete::tag_no_case;
use nom::IResult;
use nom::multi::many1;
use crate::parser::{space_comma1, Statement};

pub(crate) fn parse_delete(input: &str) -> IResult<&str, Statement> {
    let (input, _) = tag_no_case("DELETE")(input)?;
    let parse_result = many1(parse_transaction_id)(input);
    match parse_result {
        Ok((input, trans_ids)) => Ok((input, Statement::Delete(Some(trans_ids)))),
        Err(e) => {
            warn!("{e:?}");
            Ok((input, Statement::Delete(None)))
        }
    }

}

fn parse_transaction_id(input: &str) -> IResult<&str, u32> {
    let (input, _) = space_comma1(input)?;
    let (input, trans_id) = nom::character::complete::u32(input)?;
    Ok((input, trans_id))
}
