pub mod lang;
//pub mod pdf;
pub mod search_snippet;
pub mod stemmer;
pub mod term_index;
pub mod tokenizers;

#[derive(Debug, PartialEq, Clone, Copy, Hash, Eq, serde::Serialize, serde::Deserialize)]
pub enum Language {
    Esperanto = 0,
    English = 1,
    Russian = 2,
    Mandarin = 3,
    Spanish = 4,
    Portuguese = 5,
    Italian = 6,
    Bengali = 7,
    French = 8,
    German = 9,
    Ukrainian = 10,
    Georgian = 11,
    Arabic = 12,
    Hindi = 13,
    Japanese = 14,
    Hebrew = 15,
    Yiddish = 16,
    Polish = 17,
    Amharic = 18,
    Javanese = 19,
    Korean = 20,
    Bokmal = 21,
    Danish = 22,
    Swedish = 23,
    Finnish = 24,
    Turkish = 25,
    Dutch = 26,
    Hungarian = 27,
    Czech = 28,
    Greek = 29,
    Bulgarian = 30,
    Belarusian = 31,
    Marathi = 32,
    Kannada = 33,
    Romanian = 34,
    Slovene = 35,
    Croatian = 36,
    Serbian = 37,
    Macedonian = 38,
    Lithuanian = 39,
    Latvian = 40,
    Estonian = 41,
    Tamil = 42,
    Vietnamese = 43,
    Urdu = 44,
    Thai = 45,
    Gujarati = 46,
    Uzbek = 47,
    Punjabi = 48,
    Azerbaijani = 49,
    Indonesian = 50,
    Telugu = 51,
    Persian = 52,
    Malayalam = 53,
    Oriya = 54,
    Burmese = 55,
    Nepali = 56,
    Sinhalese = 57,
    Khmer = 58,
    Turkmen = 59,
    Akan = 60,
    Zulu = 61,
    Shona = 62,
    Afrikaans = 63,
    Latin = 64,
    Slovak = 65,
    Catalan = 66,
    Tagalog = 67,
    Armenian = 68,
    Unknown = 69,
    None = 70,
}

impl Language {
    pub fn from_iso_639(code: &str) -> Self {
        match code.split_once('-').map(|c| c.0).unwrap_or(code) {
            "en" => Language::English,
            "es" => Language::Spanish,
            "pt" => Language::Portuguese,
            "it" => Language::Italian,
            "fr" => Language::French,
            "de" => Language::German,
            "ru" => Language::Russian,
            "zh" => Language::Mandarin,
            "ja" => Language::Japanese,
            "ar" => Language::Arabic,
            "hi" => Language::Hindi,
            "ko" => Language::Korean,
            "bn" => Language::Bengali,
            "he" => Language::Hebrew,
            "ur" => Language::Urdu,
            "fa" => Language::Persian,
            "ml" => Language::Malayalam,
            "or" => Language::Oriya,
            "my" => Language::Burmese,
            "ne" => Language::Nepali,
            "si" => Language::Sinhalese,
            "km" => Language::Khmer,
            "tk" => Language::Turkmen,
            "am" => Language::Amharic,
            "az" => Language::Azerbaijani,
            "id" => Language::Indonesian,
            "te" => Language::Telugu,
            "ta" => Language::Tamil,
            "vi" => Language::Vietnamese,
            "gu" => Language::Gujarati,
            "pa" => Language::Punjabi,
            "uz" => Language::Uzbek,
            "hy" => Language::Armenian,
            "ka" => Language::Georgian,
            "la" => Language::Latin,
            "sl" => Language::Slovene,
            "hr" => Language::Croatian,
            "sr" => Language::Serbian,
            "mk" => Language::Macedonian,
            "lt" => Language::Lithuanian,
            "lv" => Language::Latvian,
            "et" => Language::Estonian,
            "tl" => Language::Tagalog,
            "af" => Language::Afrikaans,
            "zu" => Language::Zulu,
            "sn" => Language::Shona,
            "ak" => Language::Akan,
            _ => Language::Unknown,
        }
    }
}
