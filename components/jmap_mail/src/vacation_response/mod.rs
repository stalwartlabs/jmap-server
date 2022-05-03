pub mod get;
pub mod set;

use std::fmt::Display;

use jmap::{jmap_store::orm::PropertySchema, Property};
use store::core::collection::Collection;

#[derive(Debug, PartialEq, Eq, Hash, Copy, Clone, serde::Serialize, serde::Deserialize)]
#[repr(u8)]
pub enum VacationResponseProperty {
    Id = 0,
    IsEnabled = 1,
    FromDate = 2,
    ToDate = 3,
    Subject = 4,
    TextBody = 5,
    HtmlBody = 6,
}

impl Property for VacationResponseProperty {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "id" => Some(VacationResponseProperty::Id),
            "isEnabled" => Some(VacationResponseProperty::IsEnabled),
            "fromDate" => Some(VacationResponseProperty::FromDate),
            "toDate" => Some(VacationResponseProperty::ToDate),
            "subject" => Some(VacationResponseProperty::Subject),
            "textBody" => Some(VacationResponseProperty::TextBody),
            "htmlBody" => Some(VacationResponseProperty::HtmlBody),
            _ => None,
        }
    }

    fn collection() -> store::core::collection::Collection {
        Collection::VacationResponse
    }
}

impl Display for VacationResponseProperty {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VacationResponseProperty::Id => write!(f, "id"),
            VacationResponseProperty::IsEnabled => write!(f, "isEnabled"),
            VacationResponseProperty::FromDate => write!(f, "fromDate"),
            VacationResponseProperty::ToDate => write!(f, "toDate"),
            VacationResponseProperty::Subject => write!(f, "subject"),
            VacationResponseProperty::TextBody => write!(f, "textBody"),
            VacationResponseProperty::HtmlBody => write!(f, "htmlBody"),
        }
    }
}

impl PropertySchema for VacationResponseProperty {
    fn required() -> &'static [Self] {
        &[]
    }

    fn indexed() -> &'static [(Self, u64)] {
        &[]
    }
}

impl From<VacationResponseProperty> for u8 {
    fn from(property: VacationResponseProperty) -> Self {
        property as u8
    }
}
