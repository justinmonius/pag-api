from fastapi import FastAPI, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import pandas as pd
import io
from openpyxl.styles import NamedStyle, PatternFill, Font
from openpyxl import Workbook

app = FastAPI()

# -------------------------------
# CORS CONFIGURATION
# -------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://pag-frontend.vercel.app"],  # your Vercel frontend
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ======================================================
# 1️⃣ PROCESS ENDPOINT (Generates updated_pag.xlsx)
# ======================================================
@app.post("/process")
async def process_files(
    pag_file: UploadFile = File(...),
    ship_file: UploadFile = File(...),
    ebu_file: UploadFile = File(...)
):
    pag_df = pd.read_excel(pag_file.file)
    ship_df = pd.read_excel(ship_file.file)

    # Normalize columns
    for df in [pag_df, ship_df]:
        df.rename(columns=lambda x: str(x).strip(), inplace=True)

    # Standardize key names
    if "Material" in pag_df.columns:
        pag_df.rename(columns={"Material": "Part #"}, inplace=True)
    if "(a)P/N&S/N" in ship_df.columns:
        ship_df.rename(columns={"(a)P/N&S/N": "Part #"}, inplace=True)

    # Validate inputs
    if "Purchasing Document" not in pag_df.columns:
        raise ValueError("PAG file must contain 'Purchasing Document'")
    if "PO Number" not in ship_df.columns:
        raise ValueError("Shipment file must contain 'PO Number'")

    # Extract shipment info
    ship_df["SlipDate"] = pd.to_datetime(
        ship_df["PackingSlip"].astype(str).str[:8],
        format="%Y%m%d",
        errors="coerce"
    )
    ship_latest_dates = ship_df.groupby(["Part #", "PO Number"])["SlipDate"].max()
    shipped_map = ship_df.groupby(["Part #", "PO Number"])["Total général"].sum().to_dict()

    # Step 1 downcount
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

    # Step 2: EBU shipments
    special_header_sheets = {"Toulouse Shipments", "Rogerville Shipments", "Morocco Shipments", "Tianjin Shipments"}
    ebu_sheets = pd.read_excel(ebu_file.file, sheet_name=None)
    ebu_frames, price_map = [], []

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
            df = df[["(a)P/N&S/N", "PO Number", "Ship Date", "(f) Qty", "(g) Unit/Lot (Repair) Price"]].copy()
            df.rename(columns={"(a)P/N&S/N": "Part #"}, inplace=True)
            df["Ship Date"] = pd.to_datetime(df["Ship Date"], errors="coerce")
            df["(f) Qty"] = pd.to_numeric(df["(f) Qty"], errors="coerce").fillna(0)
            df["(g) Unit/Lot (Repair) Price"] = pd.to_numeric(df["(g) Unit/Lot (Repair) Price"], errors="coerce").fillna(0)
            ebu_frames.append(df)
            price_map.append(df[["Part #", "PO Number", "(g) Unit/Lot (Repair) Price"]].drop_duplicates())

    if ebu_frames:
        ebu_df = pd.concat(ebu_frames, ignore_index=True)
        price_df = pd.concat(price_map, ignore_index=True).drop_duplicates()
    else:
        ebu_df = pd.DataFrame()
        price_df = pd.DataFrame(columns=["Part #", "PO Number", "(g) Unit/Lot (Repair) Price"])

    ebu_counts = {}
    for (part, po), cutoff_date in ship_latest_dates.items():
        if pd.notna(cutoff_date):
            mask = (
                (ebu_df["Part #"] == part) &
                (ebu_df["PO Number"] == po) &
                (ebu_df["Ship Date"] > cutoff_date)
            )
            ebu_counts[(part, po)] = ebu_df.loc[mask, "(f) Qty"].sum()

    # Step 2 downcount
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

    # Format date columns
    for col in pag_df.columns:
        if "Date" in col:
            pag_df[col] = pd.to_datetime(pag_df[col], errors="coerce")

    pag_output = pag_df.rename(columns={"Part #": "Material"}).copy()

    # Generate summary sheets
    ship_totals_df = pd.DataFrame([
        {"Material": part, "PO Number": po, "Shipped_Total": total}
        for (part, po), total in shipped_map.items()
    ])
    ship_latest_df = ship_latest_dates.reset_index().rename(
        columns={"Part #": "Material", "PO Number": "PO Number", "SlipDate": "Latest_SlipDate"}
    )
    ebu_qty_df = pd.DataFrame([
        {"Material": part, "PO Number": po, "EBU_Qty_AfterCutoff": qty}
        for (part, po), qty in ebu_counts.items()
    ])
    price_df.rename(columns={
        "Part #": "Material",
        "PO Number": "Purchasing Document",
        "(g) Unit/Lot (Repair) Price": "Unit_Price"
    }, inplace=True)

    # Excel output
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        pag_output.to_excel(writer, index=False, sheet_name="Updated")
        ship_totals_df.to_excel(writer, index=False, sheet_name="Shipment_Totals")
        ship_latest_df.to_excel(writer, index=False, sheet_name="Shipment_Latest_Dates")
        ebu_qty_df.to_excel(writer, index=False, sheet_name="EBU_Qty_AfterCutoff")
        price_df.to_excel(writer, index=False, sheet_name="Price_Map")

        # Format date columns
        wb = writer.book
        date_style = NamedStyle(name="date_style", number_format="MM/DD/YYYY")
        if "Shipment_Latest_Dates" in wb.sheetnames:
            ws = wb["Shipment_Latest_Dates"]
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

# ======================================================
# 2️⃣ DELTA ENDPOINT (Generates weekly deltas + revenue)
# ======================================================
@app.post("/delta")
async def delta_report(new_file: UploadFile = File(...), old_file: UploadFile = File(...)):
    new_df = pd.read_excel(new_file.file, sheet_name="Updated")
    old_df = pd.read_excel(old_file.file, sheet_name="Updated")

    for df in [new_df, old_df]:
        df["Stat.-Rel. Del. Date"] = pd.to_datetime(df["Stat.-Rel. Del. Date"], errors="coerce")

    new_grouped = (
        new_df.groupby(["Material", "Purchasing Document", new_df["Stat.-Rel. Del. Date"].dt.to_period("W")])["Qty remaining to deliver"]
        .sum().reset_index()
    )
    old_grouped = (
        old_df.groupby(["Material", "Purchasing Document", old_df["Stat.-Rel. Del. Date"].dt.to_period("W")])["Qty remaining to deliver"]
        .sum().reset_index()
    )

    merged = pd.merge(new_grouped, old_grouped,
                      on=["Material", "Purchasing Document", "Stat.-Rel. Del. Date"],
                      how="outer", suffixes=("_new", "_old")).fillna(0)
    merged["Delta"] = merged["Qty remaining to deliver_new"] - merged["Qty remaining to deliver_old"]

    pivot = merged.pivot_table(index=["Material", "Purchasing Document"],
                               columns="Stat.-Rel. Del. Date", values="Delta", fill_value=0)
    pivot_cumulative = pivot.cumsum(axis=1)

    try:
        price_map = pd.read_excel(new_file.file, sheet_name="Price_Map")
    except Exception:
        price_map = pd.DataFrame(columns=["Material", "Purchasing Document", "Unit_Price"])

    merged_price = merged.merge(price_map, on=["Material", "Purchasing Document"], how="left")
    merged_price["Revenue_Delta"] = merged_price["Delta"] * merged_price["Unit_Price"]

    revenue_pivot = merged_price.pivot_table(index=["Material", "Purchasing Document"],
                                             columns="Stat.-Rel. Del. Date", values="Revenue_Delta", fill_value=0)

    # Excel output
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        pivot.to_excel(writer, sheet_name="Delta_Report")
        pivot_cumulative.to_excel(writer, sheet_name="Cumulative")
        revenue_pivot.to_excel(writer, sheet_name="Revenue_Delta")

        wb = writer.book
        ws = wb["Revenue_Delta"]
        for row in ws.iter_rows(min_row=2, min_col=3):
            for cell in row:
                val = cell.value
                if isinstance(val, (int, float)):
                    if val > 0:
                        cell.fill = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
                        cell.font = Font(color="006100")
                    elif val < 0:
                        cell.fill = PatternFill(start_color="F2DEDE", end_color="F2DEDE", fill_type="solid")
                        cell.font = Font(color="9C0006")

    output.seek(0)
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=delta_report.xlsx"}
    )

# ======================================================
# ROOT ENDPOINT
# ======================================================
@app.get("/")
def root():
    return {"message": "PAG API is live!"}
