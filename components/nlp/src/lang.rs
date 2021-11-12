use whatlang::{detect, Lang};

use crate::Language;

pub fn detect_language(text: &str) -> (Language, f64) {
    detect(text).map_or((Language::Unknown, 0.0), |info| {
        (
            match info.lang() {
                Lang::Epo => Language::Esperanto,
                Lang::Eng => Language::English,
                Lang::Rus => Language::Russian,
                Lang::Cmn => Language::Mandarin,
                Lang::Spa => Language::Spanish,
                Lang::Por => Language::Portuguese,
                Lang::Ita => Language::Italian,
                Lang::Ben => Language::Bengali,
                Lang::Fra => Language::French,
                Lang::Deu => Language::German,
                Lang::Ukr => Language::Ukrainian,
                Lang::Kat => Language::Georgian,
                Lang::Ara => Language::Arabic,
                Lang::Hin => Language::Hindi,
                Lang::Jpn => Language::Japanese,
                Lang::Heb => Language::Hebrew,
                Lang::Yid => Language::Yiddish,
                Lang::Pol => Language::Polish,
                Lang::Amh => Language::Amharic,
                Lang::Jav => Language::Javanese,
                Lang::Kor => Language::Korean,
                Lang::Nob => Language::Bokmal,
                Lang::Dan => Language::Danish,
                Lang::Swe => Language::Swedish,
                Lang::Fin => Language::Finnish,
                Lang::Tur => Language::Turkish,
                Lang::Nld => Language::Dutch,
                Lang::Hun => Language::Hungarian,
                Lang::Ces => Language::Czech,
                Lang::Ell => Language::Greek,
                Lang::Bul => Language::Bulgarian,
                Lang::Bel => Language::Belarusian,
                Lang::Mar => Language::Marathi,
                Lang::Kan => Language::Kannada,
                Lang::Ron => Language::Romanian,
                Lang::Slv => Language::Slovene,
                Lang::Hrv => Language::Croatian,
                Lang::Srp => Language::Serbian,
                Lang::Mkd => Language::Macedonian,
                Lang::Lit => Language::Lithuanian,
                Lang::Lav => Language::Latvian,
                Lang::Est => Language::Estonian,
                Lang::Tam => Language::Tamil,
                Lang::Vie => Language::Vietnamese,
                Lang::Urd => Language::Urdu,
                Lang::Tha => Language::Thai,
                Lang::Guj => Language::Gujarati,
                Lang::Uzb => Language::Uzbek,
                Lang::Pan => Language::Punjabi,
                Lang::Aze => Language::Azerbaijani,
                Lang::Ind => Language::Indonesian,
                Lang::Tel => Language::Telugu,
                Lang::Pes => Language::Persian,
                Lang::Mal => Language::Malayalam,
                Lang::Ori => Language::Oriya,
                Lang::Mya => Language::Burmese,
                Lang::Nep => Language::Nepali,
                Lang::Sin => Language::Sinhalese,
                Lang::Khm => Language::Khmer,
                Lang::Tuk => Language::Turkmen,
                Lang::Aka => Language::Akan,
                Lang::Zul => Language::Zulu,
                Lang::Sna => Language::Shona,
                Lang::Afr => Language::Afrikaans,
                Lang::Lat => Language::Latin,
                Lang::Slk => Language::Slovak,
                Lang::Cat => Language::Catalan,
            },
            info.confidence(),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detect_languages() {
        let inputs = [
            ("The quick brown fox jumps over the lazy dog", Language::English),
            ("Jovencillo emponzoñado de whisky: ¡qué figurota exhibe!", Language::Spanish),
            ("Ma la volpe col suo balzo ha raggiunto il quieto Fido", Language::Italian),
            ("Jaz em prisão bota que vexa dez cegonhas felizes", Language::Portuguese),
            ("Zwölf Boxkämpfer jagten Victor quer über den großen Sylter Deich", Language::German),
            ("עטלף אבק נס דרך מזגן שהתפוצץ כי חם", Language::Hebrew),
            ("Съешь ещё этих мягких французских булок, да выпей же чаю", Language::Russian),
            ("Чуєш їх, доцю, га? Кумедна ж ти, прощайся без ґольфів!", Language::Ukrainian),
            ("Љубазни фењерџија чађавог лица хоће да ми покаже штос", Language::Serbian),
            ("Pijamalı hasta yağız şoföre çabucak güvendi", Language::Turkish),
            ("己所不欲,勿施于人。", Language::Mandarin),
            ("井の中の蛙大海を知らず", Language::Japanese),
            ("시작이 반이다", Language::Korean),
        ];

        for input in inputs.iter() {
            let (lang, _) = detect_language(input.0);
            //println!("{:?}", lang);
            assert_eq!(lang, input.1);
        }
    }
}
