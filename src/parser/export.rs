use nom::bytes::complete::tag_no_case;
use nom::character::complete::multispace1;
use nom::IResult;
use crate::parser::Statement;

/// Parse `EXPORT TO file_path` pattern.
pub(crate) fn export(input: &str) -> IResult<&str, Statement> {
    let (input, _) = tag_no_case("EXPORT")(input)?;
    let (input, _) =  multispace1(input)?;
    let (input, _) = tag_no_case("TO")(input)?;
    let (file_path, _) =  multispace1(input)?;
    let quotation_marks :&[_] = &['\'', '"'];
    Ok((file_path, Statement::Export(file_path.trim_matches(quotation_marks).to_string())))
}
