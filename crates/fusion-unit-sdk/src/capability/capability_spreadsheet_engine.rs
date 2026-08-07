use super::Capability;
use crate::runtime::UnitResult;

/// Spreadsheet engine capability.
///
/// Implementations provide read/write access to spreadsheet formats.
///
/// # Well-known names
///
/// ```ignore
/// use fusion_unit_sdk::capability::capability_spreadsheet_engine::well_known;
/// let excel = capability::read().spreadsheet(well_known::EXCEL);
/// ```
#[async_trait::async_trait]
pub trait CapabilitySpreadsheetEngine: Capability {
    /// Read rows from a sheet in a spreadsheet file.
    async fn read_sheet(
        &self,
        path: &str,
        sheet: &str,
        skip_rows: u64,
    ) -> UnitResult<Vec<crate::proto::transfer::Row>>;

    /// Write rows to a sheet in a spreadsheet file.
    async fn write_sheet(
        &self,
        path: &str,
        sheet: &str,
        rows: &[crate::proto::transfer::Row],
    ) -> UnitResult<()>;
}

/// Well-known `CapabilitySpreadsheetEngine` capability names.
pub mod well_known {
    /// Microsoft Excel (via calamine + rust_xlsxwriter) — `"excel"`
    pub const EXCEL: &str = "excel";
    /// Default / unspecified implementation — `"default"`
    pub const DEFAULT: &str = "default";
}
