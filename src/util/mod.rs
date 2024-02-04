use std::ops::Range;
use chrono::{Datelike, NaiveDate, Utc};

pub(crate) fn year_of(year: i32) -> Range<NaiveDate> {
    let first_day = NaiveDate::from_ymd_opt(year, 1, 1).unwrap();
    let first_day_next_year = NaiveDate::from_ymd_opt(year + 1, 1, 1).unwrap();
    first_day..first_day_next_year
}

/// Compute month from an int, based on current date. If the month given is in future return the
/// same month in last year. E.g. if now is 2024-03, input 6 will return 2023-06.
pub(crate) fn month_of(month: u32) -> Range<NaiveDate> {
    let mut month = month % 12;
    if month == 0 {
        month = 12;
    }

    let today = Utc::now().naive_utc().date();
    let mut year = today.year();
    if month >= today.month() {
        year -= 1;
    }

    let first_day = NaiveDate::from_ymd_opt(year, month, 1).unwrap();
    let next_month = if month == 12 { 1 } else { month + 1 };
    let next_month_year = if month == 12 { year + 1 } else { year };
    let first_day_next_month = NaiveDate::from_ymd_opt(next_month_year, next_month, 1).unwrap();

    first_day..first_day_next_month
}
