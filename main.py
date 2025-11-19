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

# Helpers
EBU_SHEETS = [
    "Toulouse Shipments", "Pylon Shipments", "Hamburg Shipments",
    "Rogerville Shipments", "Morocco Shipments", "Tianjin Shipments"
]
SPECIAL_HEADER_SHEETS = {"Toulouse Shipments", "Rogerville Shipments", "Morocco Shipments", "Tianjin Shipments"}

def read_excel(file, **kw):
    # Force openpyxl to avoid engine detection issues on some hosts
    return pd.read_excel(file, engine="openpyxl", **kw)

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
    pag_df = read_excel(pag_file.file)
    ship_df = read_excel(ship_file.file)

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

    # ------------------------------------------
    # FIX APPLIED HERE: qty_to_remove = abs(total_shipped)
    # ------------------------------------------
    for (part, po), total_shipped in shipped_map.items():
        qty_to_remove = abs(total_shipped)   # <-- FIXED LINE (only change)
        if qty_to_remove <= 0:
            continue

        mask = (pag_df["Part #"] == part) & (pag_df["Purchasing Document"] == po)
        for idx in pag_df[mask].index:
            if qty_to_remove <= 0:
                break
            available = pag_df.at[idx, "Qty remaining to deliver"]
            if pd.notna(available) and available > 0:
                if available <= qty_to_remove:
                    pag_df.at[idx, "Qty remaining to deliver"] = 0
                    qty_to_remove -= available
                else:
                    pag_df.at[idx, "Qty remaining to deliver"] = available - qty_to_remove
                    qty_to_remove = 0

    # -------------------------------
    # ROUND 2: EBU DOWNCOUNTING (quantities) + BUILD PRICE LOOKUP
    # -------------------------------
    ebu_sheets_all = read_excel(ebu_file.file, sheet_name=None)
    ebu_frames = []
    price_frames = []

    for name in EBU_SHEETS:
        if name not in ebu_sheets_all:
            continue
        header_row = 1 if name in SPECIAL_HEADER_SHEETS else 0
        df = read_excel(ebu_file.file, sheet_name=name, header=header_row)
        df.rename(columns=lambda x: str(x).strip(), inplace=True)

        # For downcounting
        if {"(a)P/N&S/N", "PO Number", "Ship Date", "(f) Qty"}.issubset(df.columns):
            ship_df2 = df[["(a)P/N&S/N", "PO Number", "Ship Date", "(f) Qty"]].copy()
            ship_df2.rename(columns={"(a)P/N&S/N": "Part #"}, inplace=True)
            ship_df2["Ship Date"] = pd.to_datetime(ship_df2["Ship Date"], errors="coerce")
            ship_df2["(f) Qty"] = pd.to_numeric(ship_df2["(f) Qty"], errors="coerce").fillna(0)
            ebu_frames.append(ship_df2)

        # For price lookup (unit price)
        if {"(a)P/N&S/N", "PO Number", "(g) Unit/Lot (Repair) Price"}.issubset(df.columns):
            price_df = df[["(a)P/N&S/N", "PO Number", "(g) Unit/Lot (Repair) Price"]].copy()
            price_df.rename(columns={
                "(a)P/N&S/N": "Material",
                "PO Number": "Purchasing Document",
                "(g) Unit/Lot (Repair) Price": "Unit_Price"
            }, inplace=True)
            price_df["Unit_Price"] = pd.to_numeric(price_df["Unit_Price"], errors="coerce").fillna(0)
            price_frames.append(price_df)

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

        # Apply downcounting from EBU shipments after cutoff
        for (part, po), qty_to_remove in ebu_counts.items():
            if qty_to_remove <= 0:
                continue
            mask = (pag_df["Part #"] == part) & (pag_df["Purchasing Document"] == po)
            for idx in pag_df[mask].index:
                if qty_to_remove <= 0:
                    break
                available = pag_df.at[idx, "Qty remaining to deliver"]
                if pd.notna(available) and available > 0:
                    if available <= qty_to_remove:
                        pag_df.at[idx, "Qty remaining to deliver"] = 0
                        qty_to_remove -= available
                    else:
                        pag_df.at[idx, "Qty remaining to deliver"] = available - qty_to_remove
                        qty_to_remove = 0

    # Build Price_Lookup sheet (unique per Material + Purchasing Document)
    if price_frames:
        price_lookup = (
            pd.concat(price_frames, ignore_index=True)
            .sort_values(["Material", "Purchasing Document"])  
        )
        price_lookup = (
            price_lookup
            .dropna(subset=["Material", "Purchasing Document"])
            .drop_duplicates(subset=["Material", "Purchasing Document"], keep="last")
            .reset_index(drop=True)
        )
    else:
        price_lookup = pd.DataFrame(columns=["Material", "Purchasing Document", "Unit_Price"])

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
        price_lookup.to_excel(writer, index=False, sheet_name="Price_Lookup")

        # Date formatting
        ws = writer.sheets["Updated"]
        date_style = NamedStyle(name="date_style", number_format="MM/DD/YYYY")
        for cell in ws[1]:
            if "Date" in str(cell.value):
                col_letter = cell.column_letter
                for c in ws[col_letter][1:]:
                    c.style = date_style

    output.seek(0)
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=updated_pag.xlsx"}
    )


# -------------------------------
# STEP 2: DELTA + CUMULATIVE + REVENUE ENDPOINT
# -------------------------------
@app.post("/delta")
async def delta_report(
    new_file: UploadFile = File(...),
    old_file: UploadFile = File(...)
):
    new_xl = pd.ExcelFile(new_file.file, engine="openpyxl")
    old_df = read_excel(old_file.file)

    if "Updated" not in new_xl.sheet_names:
        raise ValueError("The 'new_file' must contain a sheet named 'Updated'.")
    if "Price_Lookup" not in new_xl.sheet_names:
        raise ValueError("The 'new_file' must contain a sheet named 'Price_Lookup'.")

    new_df = new_xl.parse("Updated")
    price_df = new_xl.parse("Price_Lookup")

    # Normalize
    for df in [new_df, old_df]:
        df.rename(columns=lambda x: str(x).strip(), inplace=True)
        if "Part #" in df.columns:
            df.rename(columns={"Part #": "Material"}, inplace=True)

        possible_cols = [c for c in df.columns if "stat" in c.lower() and "del" in c.lower() and "date" in c.lower()]
        if not possible_cols:
            raise ValueError("Could not find 'Stat.-Rel. Del. Date' column in one of the files.")
        date_col = possible_cols[0]

        df["Stat_Rel_Date"] = pd.to_datetime(df[date_col], errors="coerce")
        df["Month"] = df["Stat_Rel_Date"].dt.to_period("M").astype(str)

    # Group
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

    # Pivot
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

    # Cumulative
    cumulative = pivot.copy()
    month_cols = [c for c in cumulative.columns if c not in ["Material", "Purchasing Document"]]
    for i in range(1, len(month_cols)):
        cumulative[month_cols[i]] = cumulative[month_cols[i-1]] + cumulative[month_cols[i]]

    # Revenue
    price_df.rename(columns=lambda x: str(x).strip(), inplace=True)
    price_df["Unit_Price"] = pd.to_numeric(price_df["Unit_Price"], errors="coerce").fillna(0)

    merged_price = merged.merge(price_df, on=["Material", "Purchasing Document"], how="left").fillna({"Unit_Price": 0})
    merged_price["Revenue"] = merged_price["Delta"] * merged_price["Unit_Price"]

    revenue_pivot = (
        merged_price.pivot_table(
            index=["Material", "Purchasing Document"],
            columns="Month",
            values="Revenue",
            aggfunc="sum",
            fill_value=0
        ).reset_index()
    )
    revenue_pivot.columns.name = None
    revenue_sorted_cols = ["Material", "Purchasing Document"] + sorted(
        [c for c in revenue_pivot.columns if c not in ["Material", "Purchasing Document"]]
    )
    revenue_pivot = revenue_pivot[revenue_sorted_cols]

    # Write output
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        pivot.to_excel(writer, index=False, sheet_name="Delta_Report")
        cumulative.to_excel(writer, index=False, sheet_name="Cumulative")
        revenue_pivot.to_excel(writer, index=False, sheet_name="Revenue")

    output.seek(0)
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=delta_report.xlsx"}
    )


@app.get("/")
def root():
    return {"message": "PAG API is live!"}
