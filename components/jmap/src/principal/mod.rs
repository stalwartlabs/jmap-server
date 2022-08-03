use std::fmt::Display;

use ::store::FieldId;

use self::schema::Property;

pub mod orm;
pub mod raft;
pub mod schema;
pub mod serialize;
pub mod store;

impl Display for Property {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Property::Id => f.write_str("id"),
            Property::Type => f.write_str("type"),
            Property::Name => f.write_str("name"),
            Property::Description => f.write_str("description"),
            Property::Email => f.write_str("email"),
            Property::Timezone => f.write_str("timezone"),
            Property::Capabilities => f.write_str("capabilities"),
            Property::Secret => f.write_str("secret"),
            Property::DKIM => f.write_str("dkim"),
            Property::Quota => f.write_str("quota"),
            Property::Picture => f.write_str("picture"),
            Property::Members => f.write_str("members"),
            Property::Aliases => f.write_str("aliases"),
            Property::ACL => f.write_str("acl"),
            Property::Invalid => Ok(()),
        }
    }
}

impl From<Property> for FieldId {
    fn from(property: Property) -> Self {
        property as FieldId
    }
}

impl From<FieldId> for Property {
    fn from(field: FieldId) -> Self {
        match field {
            0 => Property::Id,
            1 => Property::Type,
            2 => Property::Name,
            3 => Property::Description,
            4 => Property::Email,
            5 => Property::Timezone,
            6 => Property::Capabilities,
            7 => Property::Aliases,
            8 => Property::Secret,
            9 => Property::DKIM,
            10 => Property::Quota,
            11 => Property::Picture,
            12 => Property::Members,
            13 => Property::ACL,
            _ => Property::Invalid,
        }
    }
}

impl TryFrom<&str> for Property {
    type Error = ();

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match Property::parse(value) {
            Property::Invalid => Err(()),
            property => Ok(property),
        }
    }
}

impl Property {
    pub fn parse(value: &str) -> Self {
        match value {
            "id" => Property::Id,
            "type" => Property::Type,
            "name" => Property::Name,
            "description" => Property::Description,
            "email" => Property::Email,
            "timezone" => Property::Timezone,
            "capabilities" => Property::Capabilities,
            "secret" => Property::Secret,
            "aliases" => Property::Aliases,
            "dkim" => Property::DKIM,
            "quota" => Property::Quota,
            "picture" => Property::Picture,
            "members" => Property::Members,
            "acl" => Property::ACL,
            _ => Property::Invalid,
        }
    }
}
