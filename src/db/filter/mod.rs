mod not_equal;
mod equals;
mod like;

pub(crate) use equals::handle_equals;
pub(crate) use not_equal::handle_not_equal;
pub(crate) use like::handle_like;
