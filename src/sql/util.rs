use sqlparser::ast::{Expr, UnaryOperator, Value};
use crate::common::Error;

pub(crate) fn expr_to_s(expr: &Expr) -> Result<String, Error> {
    // Clone the expr as it's very hard to do pattern matching with reference
    let expr: Expr = expr.clone();

    match expr {
        Expr::Value(Value::SingleQuotedString(s)) => { Ok(s) },
        Expr::Value(Value::DoubleQuotedString(s)) => { Ok(s) },
        _ => {
            Err(Error::new(format!("Unable to parse {expr:?}")))
        }
    }
}

pub(crate) fn expr_to_float(expr: &Expr) -> Result<f32, Error> {
    // Clone the expr as it's very hard to do pattern matching with reference
    let expr: Expr = expr.clone();

    match expr {
        Expr::Value(Value::Number(s, _)) => { Ok(s.parse::<f32>().unwrap()) },
        Expr::UnaryOp {op, expr} => {
            match op {
                UnaryOperator::Minus => {
                    if let Expr::Value(Value::Number(s, _)) = *expr {
                        return Ok(-s.parse::<f32>().unwrap());
                    }
                },
                UnaryOperator::Plus => {
                    if let Expr::Value(Value::Number(s, _)) = *expr {
                        return Ok(s.parse::<f32>().unwrap());
                    }
                },
                _ => {}
            }

            return Err(Error::new(format!("Cannot parse: {expr}")));
        },
        _ => {
            Err(Error::new(format!("Unrecognised float expression {expr:?}")))
        }
    }
}
