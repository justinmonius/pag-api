from fastapi import FastAPI, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import pandas as pd
import io
from openpyxl.styles import NamedStyle
from typing import Optional
import zipfile

app = FastAPI()

# -------------------------------
# CORS CONFIGURATION
# -------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://pag-frontend.vercel.app"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# -------------------------------
# MAIN ENDPOINT (Full Process + Optional Delta)
# -------------------------------
@app.post("/process")
async def process_files(
    pag_file: UploadFile = File(...),
    ship_file: UploadFile = File(...),
    ebu_file: UploadFile = File(...),
    old_pag_file: Optional[UploadFile] = File(None)  # optional extra input
):
    # Step 1: Process the three main files to produce updated PAG
    pag_df = pd.read_excel(pag_file.file)
    ship_df = pd.read_excel(ship_file.file)

    # -------------------------------
    # NORMALIZE COLUMN NAMES
    # -------------------------------
    for df in [pag_df, ship_df]:
        df.rename(columns=lambda x: str(x).strip(), inplace=True)

    # Standardize column names
    if "Material" in pag_df.columns:
        pag_df.rename(columns={"Material": "Part #"}, inplace=True)
    if "(a)P/N&S/N" in ship_df.columns:
        ship_df.rename(columns={"(a)P/N&S/N": "Part #"}, inplace=True)

    # Validate required columns
    if "Purchasing Document" not in pag_df.columns:
        raise ValueError("PAG file must contain 'Purchasing Document' column")
    if "PO Number" not in ship_df.columns:
        raise ValueError("Shipment file must contain 'PO Number' column")

    # -------------------------------
    # ROUND 1: SHIPMENT DOWNCOUNTING
    # -------------------------------
    ship_df["SlipDate"] = pd.to_datetime(
        ship_df["PackingSlip"].astype(str).str[:8],
        format="%Y%m%d",
        errors="coerce"
    )

    ship_latest_dates = ship_df.groupby(["Part #", "PO Number"])["SlipDate"].max()
    shipped_map = (
        ship_df.groupby(["Part #", "PO Number"])["Total général"]
        .sum()
        .to_dict()
    )

    # Apply shipment downcount to PAG
    for (part, po), total_shipped in shipped_map.items():
        qty_to_remove = -total_shipped
        if qty_to_remove <= 0:
            continue

        mask = (pag_df["Part #"] == part) & (pag_df["Purchasing Document"] == po)
        for idx in pag_df[mask].index:
            if qty_to_remove <= 0:
                break
            available = pag_df.at[idx, "Qty remaining to deliver"]
            if available > 0:
                if available <= qty_to_remove:
                    pag_df.at[idx, "Qty remaining to deliver"] = 0
                    qty_to_remove -= available
                else:
                    pag_df.at[idx, "Qty remaining to deliver"] = available - qty_to_remove
                    qty_to_remove = 0

    # -------------------------------
    # ROUND 2: EBU DOWNCOUNTING
    # -------------------------------
    special_header_sheets = {
        "Toulouse Shipments",
        "Rogerville Shipments",
        "Morocco Shipments",
        "Tianjin Shipments"
    }

    ebu_sheets = pd.read_excel(ebu_file.file, sheet_name=None)
    ebu_frames = []

    for name in [
        "Toulouse Shipments", "Pylon Shipments", "Hamburg Shipments",
        "Rogerville Shipments", "Morocco Shipments", "Tianjin Shipments"
    ]:
        if name not in ebu_sheets:
            continue

        header_row = 1 if name in special_header_sheets else 0
        df = pd.read_excel(ebu_file.file, sheet_name=name, header=header_row)
        df.rename(columns=lambda x: str(x).strip(), inplace=True)

        if "(a)P/N&S/N" in df.columns and "PO Number" in df.columns and "Ship Date" in df.columns and "(f) Qty" in df.columns:
            df = df[["(a)P/N&S/N", "PO Number", "Ship Date", "(f) Qty"]].copy()
            df.rename(columns={"(a)P/N&S/N": "Part #"}, inplace=True)
            df["Ship Date"] = pd.to_datetime(df["Ship Date"], errors="coerce")
            df["(f) Qty"] = pd.to_numeric(df["(f) Qty"], errors="coerce").fillna(0)
            ebu_frames.append(df)

    if ebu_frames:
        ebu_df = pd.concat(ebu_frames, ignore_index=True)

        ebu_counts = {}
        for (part, po), cutoff_date in ship_latest_dates.items():
            if pd.notna(cutoff_date):
                mask = (
                    (ebu_df["Part #"] == part) &
                    (ebu_df["PO Number"] == po) &
                    (ebu_df["Ship Date"] > cutoff_date)
                )
                ebu_counts[(part, po)] = ebu_df.loc[mask, "(f) Qty"].sum()

        for (part, po), qty_to_remove in ebu_counts.items():
            if qty_to_remove <= 0:
                continue

            mask = (pag_df["Part #"] == part) & (pag_df["Purchasing Document"] == po)
            for idx in pag_df[mask].index:
                if qty_to_remove <= 0:
                    break
                available = pag_df.at[idx, "Qty remaining to deliver"]
                if available > 0:
                    if available <= qty_to_remove:
                        pag_df.at[idx, "Qty remaining to deliver"] = 0
                        qty_to_remove -= available
                    else:
                        pag_df.at[idx, "Qty remaining to deliver"] = available - qty_to_remove
                        qty_to_remove = 0
    else:
        ebu_counts = {}

    # -------------------------------
    # FINAL FORMATTING
    # -------------------------------
    for col in pag_df.columns:
        if "Date" in col:
            pag_df[col] = pd.to_datetime(pag_df[col], errors="coerce")

    pag_output = pag_df.rename(columns={"Part #": "Material"}).copy()

    # -------------------------------
    # OUTPUT 1: UPDATED PAG
    # -------------------------------
    updated_output = io.BytesIO()
    with pd.ExcelWriter(updated_output, engine="openpyxl") as writer:
        pag_output.to_excel(writer, index=False, sheet_name="Updated")

        ws = writer.sheets["Updated"]
        date_style = NamedStyle(name="date_style", number_format="MM/DD/YYYY")
        for cell in ws[1]:
            if "Date" in str(cell.value):
                col_idx = cell.column_letter
                for c in ws[col_idx][1:]:
                    c.style = date_style

        # Summary sheets
        pd.DataFrame([
            {"Part #": part, "PO Number": po, "Shipped_Total": total}
            for (part, po), total in shipped_map.items()
        ]).to_excel(writer, index=False, sheet_name="Shipment_Totals")

        ship_latest_dates.reset_index().rename(
            columns={"SlipDate": "Latest_SlipDate"}
        ).to_excel(writer, index=False, sheet_name="Shipment_Latest_Dates")

        pd.DataFrame([
            {"Part #": part, "PO Number": po, "EBU_Qty_AfterCutoff": qty}
            for (part, po), qty in ebu_counts.items()
        ]).to_excel(writer, index=False, sheet_name="EBU_Qty_AfterCutoff")

    updated_output.seek(0)

    # -------------------------------
    # STEP 2: DELTA REPORT (if old_pag_file uploaded)
    # -------------------------------
    delta_output = None
    if old_pag_file:
        old_df = pd.read_excel(old_pag_file.file)
        new_df = pag_output.copy()

        for df in [new_df, old_df]:
            df.rename(columns=lambda x: str(x).strip(), inplace=True)
            if "Part #" in df.columns:
                df.rename(columns={"Part #": "Material"}, inplace=True)

        # Convert date column
        for df in [new_df, old_df]:
            df["Stat.-Rel Del. Date"] = pd.to_datetime(df["Stat.-Rel Del. Date"], errors="coerce")
            df["Month"] = df["Stat.-Rel Del. Date"].dt.to_period("M").astype(str)

        # Group and aggregate
        new_grouped = (
            new_df.groupby(["Material", "Purchasing Document", "Month"])["Qty remaining to deliver"]
            .sum()
            .reset_index()
            .rename(columns={"Qty remaining to deliver": "New_Qty"})
        )

        old_grouped = (
            old_df.groupby(["Material", "Purchasing Document", "Month"])["Qty remaining to deliver"]
            .sum()
            .reset_index()
            .rename(columns={"Qty remaining to deliver": "Old_Qty"})
        )

        merged = pd.merge(
            new_grouped, old_grouped,
            on=["Material", "Purchasing Document", "Month"], how="outer"
        ).fillna(0)

        merged["Delta"] = merged["New_Qty"] - merged["Old_Qty"]

        delta_pivot = (
            merged.pivot_table(
                index=["Material", "Purchasing Document"],
                columns="Month", values="Delta",
                aggfunc="sum", fill_value=0
            ).reset_index()
        )

        delta_pivot.columns.name = None
        sorted_cols = ["Material", "Purchasing Document"] + sorted(
            [c for c in delta_pivot.columns if c not in ["Material", "Purchasing Document"]]
        )
        delta_pivot = delta_pivot[sorted_cols]

        delta_output = io.BytesIO()
        with pd.ExcelWriter(delta_output, engine="openpyxl") as writer:
            delta_pivot.to_excel(writer, index=False, sheet_name="Delta_Pivot")

        delta_output.seek(0)

    # -------------------------------
    # RETURN BOTH FILES (ZIP if both exist)
    # -------------------------------
    if delta_output:
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w") as zf:
            zf.writestr("updated_pag.xlsx", updated_output.getvalue())
            zf.writestr("delta_report.xlsx", delta_output.getvalue())
        zip_buffer.seek(0)

        return StreamingResponse(
            zip_buffer,
            media_type="application/zip",
            headers={"Content-Disposition": "attachment; filename=pag_outputs.zip"}
        )

    # Default: only return updated_pag.xlsx
    return StreamingResponse(
        updated_output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=updated_pag.xlsx"}
    )


# -------------------------------
# ROOT ENDPOINT
# -------------------------------
@app.get("/")
def root():
    return {"message": "PAG API is live!"}
