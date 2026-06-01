use std::collections::HashSet;

use lazy_static::lazy_static;
use tokenizers::{normalizers::BertNormalizer, pre_tokenizers::bert::BertPreTokenizer, NormalizedString, Normalizer, OffsetReferential, OffsetType, PreTokenizedString, PreTokenizer};

lazy_static! {
    static ref IGNORED_TOKENS: HashSet<&'static str> = {
        let mut ignored_tokens = HashSet::new();
        ignored_tokens.insert("*");
        ignored_tokens.insert(",");
        ignored_tokens.insert(".");
        ignored_tokens.insert("&");
        ignored_tokens.insert("-");
        ignored_tokens.insert("_");
        ignored_tokens.insert("(");
        ignored_tokens.insert(")");

        ignored_tokens
    };
}

pub(crate) fn tokenise(text: &str) -> Vec<String> {
    let normaliser = BertNormalizer::new(true, true, None, true);
    let mut normalised = NormalizedString::from(text);
    normaliser.normalize(&mut normalised).unwrap();

    let pre_tokenizer = BertPreTokenizer {};
    let mut pre_tokenized = PreTokenizedString::from(normalised.get());
    pre_tokenizer.pre_tokenize(&mut pre_tokenized).unwrap();
    
    
    pre_tokenized.get_splits(OffsetReferential::Original, OffsetType::Byte)
        .into_iter()
        .filter_map(|s| {
            if IGNORED_TOKENS.contains(s.0) {
                None
            } else{
                Some(s.0.to_string())
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::tokenise;

    #[test]
    fn test() {
        let result = tokenise("DBS*Knox Grammar Sch,Wahroonga");
        assert_eq!(result, vec!["dbs", "knox", "grammar", "sch", "wahroonga"]);
    }
}