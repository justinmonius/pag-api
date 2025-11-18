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
    allow_origins=["https://pag-frontend.vercel.app"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Helpers
EBU_SHEETS = [
    "Toulouse Shipments", "Pylon Shipments", "Hamburg Shipments",
    "Rogerville Shipments", "Morocco Shipments", "Tianjin Shipments"
]
SPECIAL_HEADER_SHEETS = {
    "Toulouse Shipments", "Rogerville Shipments",
    "Morocco Shipments", "Tianjin Shipments"
}

def read_excel(file, **kw):
    return pd.read_excel(file, engine="openpyxl", **kw)


# -------------------------------
# STEP 1: PROCESS ENDPOINT
# -------------------------------
@app.post("/process")
async def process_files(
    pag_file: UploadFile = File(...),
    ship_file: UploadFile = File(...),
    ebu_file: UploadFile = File(...)
):

    pag_df = read_excel(pag_file.file)
    ship_df = read_excel(ship_file.file)

    # Normalize
    for df in [pag_df, ship_df]:
        df.rename(columns=lambda x: str(x).strip(), inplace=True)

    # Standardize naming
    if "Material" in pag_df.columns:
        pag_df.rename(columns={"Material": "Part #"}, inplace=True)
    if "(a)P/N&S/N" in ship_df.columns:
        ship_df.rename(columns={"(a)P/N&S/N": "Part #"}, inplace=True)

    # Normalize PO column so it matches PAG
    ship_df.rename(columns={"PO Number": "Purchasing Document"}, inplace=True)

    if "Purchasing Document" not in pag_df.columns:
        raise ValueError("PAG file must contain 'Purchasing Document'")
    if "Purchasing Document" not in ship_df.columns:
        raise ValueError("Shipment file must contain 'PO Number' (now mapped to Purchasing Document)")

    # -------------------------------
    # ROUND 1: SHIPMENT DOWNCOUNTING
    # -------------------------------
    ship_df["SlipDate"] = pd.to_datetime(
        ship_df["PackingSlip"].astype(str).str[:8],
        format="%Y%m%d", errors="coerce"
    )

    # Latest SlipDate
    ship_latest_dates = ship_df.groupby(
        ["Part #", "Purchasing Document"]
    )["SlipDate"].max()

    # Step 1 total shipped quantity
    shipped_map = (
        ship_df.groupby(["Part #", "Purchasing Document"])["Total général"]
        .sum().to_dict()
    )

    # STEP1: summary table
    step1_rows = []
    for (part, po), qty in shipped_map.items():
        step1_rows.append({
            "Material": part,
            "Purchasing Document": po,
            "Step1_Downcount": qty
        })
    step1_df = pd.DataFrame(step1_rows)

    # Apply Step1 downcount
    for (part, po), total_shipped in shipped_map.items():
        qty_to_remove = total_shipped
        if qty_to_remove <= 0:
            continue

        mask = (pag_df["Part #"] == part) & (pag_df["Purchasing Document"] == po)

        for idx in pag_df[mask].index:
            if qty_to_remove <= 0:
                break
            available = pag_df.at[idx, "Qty remaining to deliver"]
            if pd.notna(available) and available > 0:
                if available <= qty_to_remove:
                    qty_to_remove -= available
                    pag_df.at[idx, "Qty remaining to deliver"] = 0
                else:
                    pag_df.at[idx, "Qty remaining to deliver"] = available - qty_to_remove
                    qty_to_remove = 0

    # -------------------------------
    # ROUND 2: EBU DOWNCOUNTING & PRICE LOOKUP
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

        # EBU quantity
        if {"(a)P/N&S/N", "PO Number", "Ship Date", "(f) Qty"}.issubset(df.columns):
            temp = df[["(a)P/N&S/N", "PO Number", "Ship Date", "(f) Qty"]].copy()
            temp.rename(columns={"(a)P/N&S/N": "Part #",
                                 "PO Number": "Purchasing Document"}, inplace=True)
            temp["Ship Date"] = pd.to_datetime(temp["Ship Date"], errors="coerce")
            temp["(f) Qty"] = pd.to_numeric(temp["(f) Qty"], errors="coerce").fillna(0)
            ebu_frames.append(temp)

        # EBU price
        if {"(a)P/N&S/N", "PO Number", "(g) Unit/Lot (Repair) Price"}.issubset(df.columns):
            p = df[["(a)P/N&S/N", "PO Number", "(g) Unit/Lot (Repair) Price"]].copy()
            p.rename(columns={
                "(a)P/N&S/N": "Material",
                "PO Number": "Purchasing Document",
                "(g) Unit/Lot (Repair) Price": "Unit_Price"
            }, inplace=True)
            p["Unit_Price"] = pd.to_numeric(p["Unit_Price"], errors="coerce").fillna(0)
            price_frames.append(p)

    # Step2 downcount summary
    step2_data = []

    ebu_counts = {}
    if ebu_frames:
        ebu_df = pd.concat(ebu_frames, ignore_index=True)
        for (part, po), cutoff_date in ship_latest_dates.items():
            if pd.notna(cutoff_date):
                mask = (
                    (ebu_df["Part #"] == part) &
                    (ebu_df["Purchasing Document"] == po) &
                    (ebu_df["Ship Date"] > cutoff_date)
                )
                qty = ebu_df.loc[mask, "(f) Qty"].sum()
                ebu_counts[(part, po)] = qty
                step2_data.append({
                    "Material": part,
                    "Purchasing Document": po,
                    "Step2_Downcount": qty
                })
    else:
        step2_data = []

    step2_df = pd.DataFrame(step2_data)

    # Apply Step2 downcounting
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
                    qty_to_remove -= available
                    pag_df.at[idx, "Qty remaining to deliver"] = 0
                else:
                    pag_df.at[idx, "Qty remaining to deliver"] = available - qty_to_remove
                    qty_to_remove = 0

    # -------------------------------
    # LATEST DATES SHEET
    # -------------------------------
    latest_rows = []
    for (part, po), d in ship_latest_dates.items():
        latest_rows.append({
            "Material": part,
            "Purchasing Document": po,
            "Latest_SlipDate": d
        })
    latest_df = pd.DataFrame(latest_rows)

    # -------------------------------
    # PRICE LOOKUP SHEET
    # -------------------------------
    if price_frames:
        price_lookup = (
            pd.concat(price_frames, ignore_index=True)
            .drop_duplicates(subset=["Material", "Purchasing Document"], keep="last")
            .reset_index(drop=True)
        )
    else:
        price_lookup = pd.DataFrame(columns=["Material", "Purchasing Document", "Unit_Price"])

    # -------------------------------
    # FINAL Updated sheet formatting
    # -------------------------------
    for col in pag_df.columns:
        if "Date" in col:
            pag_df[col] = pd.to_datetime(pag_df[col], errors="coerce")

    pag_output = pag_df.rename(columns={"Part #": "Material"}).copy()

    # -------------------------------
    # WRITE OUTPUT FILE
    # -------------------------------
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:

        # 1️⃣ FIRST SHEET
        pag_output.to_excel(writer, index=False, sheet_name="Updated")

        # 2️⃣ OTHER SHEETS
        latest_df.to_excel(writer, index=False, sheet_name="Latest_Dates")
        step1_df.to_excel(writer, index=False, sheet_name="Step1_Downcount")
        step2_df.to_excel(writer, index=False, sheet_name="Step2_Downcount")
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
# DELTA / CUMULATIVE / REVENUE
# -------------------------------
@app.post("/delta")
async def delta_report(
    new_file: UploadFile = File(...),
    old_file: UploadFile = File(...)
):
    new_xl = pd.ExcelFile(new_file.file, engine="openpyxl")
    old_df = read_excel(old_file.file)

    if "Updated" not in new_xl.sheet_names:
        raise ValueError("Updated sheet missing")
    if "Price_Lookup" not in new_xl.sheet_names:
        raise ValueError("Price_Lookup missing")

    new_df = new_xl.parse("Updated")
    price_df = new_xl.parse("Price_Lookup")

    # Normalize
    for df in [new_df, old_df]:
        df.rename(columns=lambda x: str(x).strip(), inplace=True)
        if "Part #" in df.columns:
            df.rename(columns={"Part #": "Material"}, inplace=True)

        possible_cols = [c for c in df.columns if "stat" in c.lower() and "del" in c.lower() and "date" in c.lower()]
        if not possible_cols:
            raise ValueError("Missing Stat.-Rel. Del. Date")
        date_col = possible_cols[0]

        df["Stat_Rel_Date"] = pd.to_datetime(df[date_col], errors="coerce")
        df["Month"] = df["Stat_Rel_Date"].dt.to_period("M").astype(str)

    # Grouping
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
        c for c in pivot.columns if c not in ["Material", "Purchasing Document"]
    )
    pivot = pivot[sorted_cols]

    # Cumulative
    cumulative = pivot.copy()
    month_cols = sorted_cols[2:]
    for i in range(1, len(month_cols)):
        cumulative[month_cols[i]] = cumulative[month_cols[i-1]] + cumulative[month_cols[i]]

    # Revenue
    price_df.rename(columns=lambda x: str(x).strip(), inplace=True)
    price_df["Unit_Price"] = pd.to_numeric(price_df["Unit_Price"], errors="coerce").fillna(0)

    merged_price = merged.merge(
        price_df, on=["Material", "Purchasing Document"], how="left"
    ).fillna({"Unit_Price": 0})

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
        c for c in revenue_pivot.columns if c not in ["Material", "Purchasing Document"]
    )
    revenue_pivot = revenue_pivot[revenue_sorted_cols]

    # Write
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
