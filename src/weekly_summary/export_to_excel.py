from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd


@dataclass(frozen=True)
class ExcelExportResult:
    path: Path
    total_rows: int
    loc_rows: int


def export_report_to_excel(
    df: pd.DataFrame,
    *,
    output_path: Optional[Path] = None,
    base_filename: str = "weekly_summary_report",
    include_loc_sheet: bool = True,
) -> ExcelExportResult:
    """
    Export a DataFrame to an Excel file under project-root `data/` by default.

    - Writes sheet "Report"
    - Optionally writes sheet "LOC Only" (sku endswith -LOC, case-insensitive)
    """
    data_dir = Path("data")
    data_dir.mkdir(parents=True, exist_ok=True)

    if output_path is None:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = data_dir / f"{base_filename}_{ts}.xlsx"
    else:
        output_path.parent.mkdir(parents=True, exist_ok=True)

    df_out = df.copy()
    if "sku" in df_out.columns:
        df_out["sku"] = df_out["sku"].astype(str)
    if "asin" in df_out.columns:
        df_out["asin"] = df_out["asin"].astype(str)

    loc_df = pd.DataFrame()
    if include_loc_sheet and "sku" in df_out.columns:
        loc_df = df_out[df_out["sku"].astype(str).str.upper().str.endswith("-LOC")].copy()

    with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
        df_out.to_excel(writer, index=False, sheet_name="Report")
        if include_loc_sheet:
            loc_df.to_excel(writer, index=False, sheet_name="LOC Only")

        workbook = writer.book
        header_fmt = workbook.add_format({"bold": True, "bg_color": "#E6E6E6", "border": 1})

        def _format_sheet(sheet_name: str, frame: pd.DataFrame) -> None:
            ws = writer.sheets[sheet_name]
            ws.freeze_panes(1, 0)
            ws.autofilter(0, 0, max(0, len(frame)), max(0, len(frame.columns) - 1))

            # header formatting + column widths
            sample = frame.head(250)
            for col_idx, col in enumerate(frame.columns):
                ws.write(0, col_idx, col, header_fmt)
                try:
                    max_len = max([len(str(col))] + [len(str(x)) for x in sample[col].tolist()])
                except Exception:
                    max_len = len(str(col))
                ws.set_column(col_idx, col_idx, min(max(10, max_len + 2), 45))

        _format_sheet("Report", df_out)
        if include_loc_sheet:
            _format_sheet("LOC Only", loc_df)

    return ExcelExportResult(path=output_path, total_rows=int(len(df_out)), loc_rows=int(len(loc_df)))