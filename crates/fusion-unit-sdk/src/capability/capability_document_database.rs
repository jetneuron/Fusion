use super::Capability;
use crate::runtime::UnitResult;

/// Document database capability.
///
/// Implementations provide document-oriented storage with query/filter semantics.
///
/// # Well-known names
///
/// ```ignore
/// use fusion_unit_sdk::capability::capability_document_database::well_known;
/// let mongo = capability::read().doc(well_known::MONGODB);
/// ```
#[async_trait::async_trait]
pub trait CapabilityDocumentDatabase: Capability {
    /// Find documents matching a filter. Returns matching rows.
    async fn find(
        &self,
        collection: &str,
        filter: serde_json::Value,
        limit: Option<u64>,
    ) -> UnitResult<Vec<crate::proto::transfer::Frame>>;

    /// Insert documents into a collection. Returns the count inserted.
    async fn insert(
        &self,
        collection: &str,
        docs: &[crate::proto::transfer::Frame],
    ) -> UnitResult<u64>;
}

/// Well-known `CapabilityDocumentDatabase` capability names.
pub mod well_known {
    /// MongoDB — `"mongodb"`
    pub const MONGODB: &str = "mongodb";
    /// Apache CouchDB — `"couchdb"`
    pub const COUCHDB: &str = "couchdb";
    /// Default / unspecified implementation — `"default"`
    pub const DEFAULT: &str = "default";
}
