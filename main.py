from fastapi import FastAPI, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import pandas as pd
import io
from openpyxl.styles import NamedStyle

app = FastAPI()

# -------------------------------
# CORS CONFIGURATION
# -------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://pag-frontend.vercel.app"],  # your Vercel frontend URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# -------------------------------
# STEP 1: MAIN PROCESS ENDPOINT
# -------------------------------
@app.post("/process")
async def process_files(
    pag_file: UploadFile = File(...),
    ship_file: UploadFile = File(...),
    ebu_file: UploadFile = File(...)
):
    # Step 1: Read Excel files
    pag_df = pd.read_excel(pag_file.file)
    ship_df = pd.read_excel(ship_file.file)

    # Normalize column names
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

    # Latest SlipDate per (Part, PO)
    ship_latest_dates = ship_df.groupby(["Part #", "PO Number"])["SlipDate"].max()

    # Total shipped qty map
    shipped_map = (
        ship_df.groupby(["Part #", "PO Number"])["Total général"]
        .sum()
        .to_dict()
    )

    # Apply shipment downcount
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
        "Toulouse Shipments", "Rogerville Shipments",
        "Morocco Shipments", "Tianjin Shipments"
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

    ebu_counts = {}
    if ebu_frames:
        ebu_df = pd.concat(ebu_frames, ignore_index=True)
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

    # -------------------------------
    # FINAL FORMATTING
    # -------------------------------
    for col in pag_df.columns:
        if "Date" in col:
            pag_df[col] = pd.to_datetime(pag_df[col], errors="coerce")

    pag_output = pag_df.rename(columns={"Part #": "Material"}).copy()

    # -------------------------------
    # WRITE UPDATED PAG FILE
    # -------------------------------
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        pag_output.to_excel(writer, index=False, sheet_name="Updated")

        ws = writer.sheets["Updated"]
        date_style = NamedStyle(name="date_style", number_format="MM/DD/YYYY")
        for cell in ws[1]:
            if "Date" in str(cell.value):
                col_idx = cell.column_letter
                for c in ws[col_idx][1:]:
                    c.style = date_style

    output.seek(0)
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=updated_pag.xlsx"}
    )


# -------------------------------
# STEP 2: DELTA + CUMULATIVE ENDPOINT
# -------------------------------
@app.post("/delta")
async def delta_report(
    new_file: UploadFile = File(...),
    old_file: UploadFile = File(...)
):
    new_df = pd.read_excel(new_file.file)
    old_df = pd.read_excel(old_file.file)

    # Normalize & rename
    for df in [new_df, old_df]:
        df.rename(columns=lambda x: str(x).strip(), inplace=True)
        if "Part #" in df.columns:
            df.rename(columns={"Part #": "Material"}, inplace=True)

        # Auto-detect Stat-Rel Del Date column
        possible_cols = [c for c in df.columns if "stat" in c.lower() and "del" in c.lower() and "date" in c.lower()]
        if not possible_cols:
            raise ValueError("Could not find 'Stat.-Rel. Del. Date' column in file.")
        date_col = possible_cols[0]

        df["Stat_Rel_Date"] = pd.to_datetime(df[date_col], errors="coerce")
        df["Month"] = df["Stat_Rel_Date"].dt.to_period("M").astype(str)

    # Group & merge
    new_grouped = (
        new_df.groupby(["Material", "Purchasing Document", "Month"])["Qty remaining to deliver"]
        .sum().reset_index().rename(columns={"Qty remaining to deliver": "New_Qty"})
    )
    old_grouped = (
        old_df.groupby(["Material", "Purchasing Document", "Month"])["Qty remaining to deliver"]
        .sum().reset_index().rename(columns={"Qty remaining to deliver": "Old_Qty"})
    )

    merged = pd.merge(
        new_grouped, old_grouped,
        on=["Material", "Purchasing Document", "Month"], how="outer"
    ).fillna(0)
    merged["Delta"] = merged["New_Qty"] - merged["Old_Qty"]

    # Pivot for month-by-month deltas
    pivot = (
        merged.pivot_table(
            index=["Material", "Purchasing Document"],
            columns="Month",
            values="Delta",
            aggfunc="sum",
            fill_value=0
        ).reset_index()
    )

    pivot.columns.name = None
    sorted_cols = ["Material", "Purchasing Document"] + sorted(
        [c for c in pivot.columns if c not in ["Material", "Purchasing Document"]]
    )
    pivot = pivot[sorted_cols]

    # ✅ Create cumulative running totals
    cumulative = pivot.copy()
    month_cols = [c for c in cumulative.columns if c not in ["Material", "Purchasing Document"]]
    for i, col in enumerate(month_cols):
        if i == 0:
            continue
        prev_col = month_cols[i - 1]
        cumulative[col] = cumulative[prev_col] + cumulative[col]

    # -------------------------------
    # EXPORT BOTH SHEETS
    # -------------------------------
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        pivot.to_excel(writer, index=False, sheet_name="Delta_Report")
        cumulative.to_excel(writer, index=False, sheet_name="Cumulative")

    output.seek(0)
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=delta_report.xlsx"}
    )


# -------------------------------
# ROOT ENDPOINT
# -------------------------------
@app.get("/")
def root():
    return {"message": "PAG API is live!"}
