from fastapi import FastAPI, UploadFile, File, Form
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

# EBU sheet names
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

# -----------------------------------------------------------
# 🔥 UNIVERSAL FIX — CLEAN ALL PO NUMBERS TO INTEGERS
# -----------------------------------------------------------
def clean_po(po):
    if pd.isna(po):
        return None

    po_str = str(po).strip()

    # strip decimals
    if "." in po_str:
        po_str = po_str.split(".")[0]

    # scientific notation
    if "e" in po_str.lower():
        try:
            return int(float(po_str))
        except:
            return None

    po_str = po_str.replace(",", "").replace(" ", "")

    try:
        return int(po_str)
    except:
        return None

# -------------------------------
# STEP 1: PROCESS ENDPOINT
# ✅ NEW: cutoff_date used ONLY for (Part, PO) NOT in Ship file, to downcount from EBU after that date
# -------------------------------
@app.post("/process")
async def process_files(
    pag_file: UploadFile = File(...),
    ship_file: UploadFile = File(...),
    ebu_file: UploadFile = File(...),
    cutoff_date: str = Form(None),  # ✅ NEW (YYYY-MM-DD from <input type="date">)
):
    cutoff_dt = pd.to_datetime(cutoff_date, errors="coerce") if cutoff_date else None

    pag_df = read_excel(pag_file.file)

    # NOTE: you mentioned your ship test file has headers on row 2 (Excel),
    # so header=1 is used here.
    ship_df = read_excel(ship_file.file, header=1)

    # Normalize column names
    for df in [pag_df, ship_df]:
        df.rename(columns=lambda x: str(x).strip(), inplace=True)

    # Standardize naming
    if "Material" in pag_df.columns:
        pag_df.rename(columns={"Material": "Part #"}, inplace=True)
    if "(a)P/N&S/N" in ship_df.columns:
        ship_df.rename(columns={"(a)P/N&S/N": "Part #"}, inplace=True)

    # Normalize PO column
    ship_df.rename(columns={"PO Number": "Purchasing Document"}, inplace=True)

    # ---------------------------------------------------
    # 🔥 FORCE PURCHASING DOCUMENT TO NUMERIC
    # ---------------------------------------------------
    pag_df["Purchasing Document"] = pag_df["Purchasing Document"].apply(clean_po)
    ship_df["Purchasing Document"] = ship_df["Purchasing Document"].apply(clean_po)

    # -------------------------------
    # ROUND 1: SHIPMENT DOWNCOUNTING
    # -------------------------------
    ship_df["SlipDate"] = pd.to_datetime(
        ship_df["PackingSlip"].astype(str).str[:8],
        format="%Y%m%d", errors="coerce"
    )

    ship_latest_dates = ship_df.groupby(
        ["Part #", "Purchasing Document"]
    )["SlipDate"].max()

    shipped_map = (
        ship_df.groupby(["Part #", "Purchasing Document"])["Total général"]
        .sum().to_dict()
    )

    step1_rows = []
    for (part, po), qty in shipped_map.items():
        step1_rows.append({
            "Material": part,
            "Purchasing Document": po,
            "Step1_Downcount": qty
        })
    step1_df = pd.DataFrame(step1_rows)

    # APPLY STEP 1 DOWNCOUNT
    for (part, po), total_shipped in shipped_map.items():
        qty_to_remove = abs(total_shipped)
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
    # ROUND 2: EBU DOWNCOUNTING
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

        # EBU quantities
        if {"(a)P/N&S/N", "PO Number", "Ship Date", "(f) Qty"}.issubset(df.columns):
            temp = df[["(a)P/N&S/N", "PO Number", "Ship Date", "(f) Qty"]].copy()
            temp.rename(columns={"(a)P/N&S/N": "Part #",
                                 "PO Number": "Purchasing Document"}, inplace=True)

            temp["Purchasing Document"] = temp["Purchasing Document"].apply(clean_po)

            # EBU Ship Date like "MM/DD/YY" parses fine
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

            p["Purchasing Document"] = p["Purchasing Document"].apply(clean_po)
            p["Unit_Price"] = pd.to_numeric(p["Unit_Price"], errors="coerce").fillna(0)

            price_frames.append(p)

    # STEP 2 SUMMARY
    step2_data = []
    ebu_counts = {}

    if ebu_frames:
        ebu_df = pd.concat(ebu_frames, ignore_index=True)

        # A) Existing behavior: for (Part, PO) found in Ship file, cutoff = latest ship slip date
        for (part, po), ship_cutoff in ship_latest_dates.items():
            if pd.notna(ship_cutoff):
                mask = (
                    (ebu_df["Part #"] == part) &
                    (ebu_df["Purchasing Document"] == po) &
                    (ebu_df["Ship Date"] > ship_cutoff)
                )
                qty = ebu_df.loc[mask, "(f) Qty"].sum()
                ebu_counts[(part, po)] = qty

                step2_data.append({
                    "Material": part,
                    "Purchasing Document": po,
                    "Step2_Downcount": qty
                })

        # ✅ NEW B) For (Part, PO) present in PAG but NOT in Ship file:
        # cutoff = user-provided cutoff_date, and downcount EBU rows after that date
        ship_keys = set(ship_latest_dates.index.tolist())  # {(part, po), ...}

        # build pag keys (exclude nulls)
        pag_keys = set(
            (p, po)
            for p, po in zip(pag_df.get("Part #", pd.Series(dtype=object)), pag_df.get("Purchasing Document", pd.Series(dtype=object)))
            if pd.notna(p) and pd.notna(po)
        )

        missing_ship_keys = pag_keys - ship_keys

        if missing_ship_keys and cutoff_dt is None:
            # user didn't provide a cutoff date but we need one for these keys
            raise ValueError("cutoff_date is required in Step 1 to downcount EBU for (Part, PO) not present in the shipment/receipt file.")

        if cutoff_dt is not None and missing_ship_keys:
            for (part, po) in missing_ship_keys:
                mask = (
                    (ebu_df["Part #"] == part) &
                    (ebu_df["Purchasing Document"] == po) &
                    (ebu_df["Ship Date"] > cutoff_dt)
                )
                qty = ebu_df.loc[mask, "(f) Qty"].sum()

                # Only store/apply if there is any qty (keeps Step2_Downcount sheet cleaner)
                if qty != 0:
                    # if already has a ship-based entry (unlikely for missing keys), add to it
                    ebu_counts[(part, po)] = ebu_counts.get((part, po), 0) + qty

                    step2_data.append({
                        "Material": part,
                        "Purchasing Document": po,
                        "Step2_Downcount": qty
                    })

    step2_df = pd.DataFrame(step2_data)

    # APPLY STEP 2 DOWNCOUNT (applies both A and B from above via ebu_counts)
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
    # SUMMARY SHEETS
    # -------------------------------
    latest_rows = []
    for (part, po), d in ship_latest_dates.items():
        latest_rows.append({
            "Material": part,
            "Purchasing Document": po,
            "Latest_SlipDate": d
        })
    latest_df = pd.DataFrame(latest_rows)

    if price_frames:
        price_lookup = (
            pd.concat(price_frames, ignore_index=True)
            .drop_duplicates(subset=["Material", "Purchasing Document"], keep="last")
            .reset_index(drop=True)
        )
    else:
        price_lookup = pd.DataFrame(columns=["Material", "Purchasing Document", "Unit_Price"])

    # Final formatting
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
        latest_df.to_excel(writer, index=False, sheet_name="Latest_Dates")
        step1_df.to_excel(writer, index=False, sheet_name="Step1_Downcount")
        step2_df.to_excel(writer, index=False, sheet_name="Step2_Downcount")
        price_lookup.to_excel(writer, index=False, sheet_name="Price_Lookup")

    output.seek(0)
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=updated_pag.xlsx"}
    )

# -------------------------------
# DELTA / CUMULATIVE / REVENUE
# + ✅ EBU-only cutoff downcount applied to OLD file before delta calc
# -------------------------------
@app.post("/delta")
async def delta_report(
    new_file: UploadFile = File(...),
    old_file: UploadFile = File(...),
    ebu_file: UploadFile = File(...),
    cutoff_date: str = Form(...),
):
    cutoff_dt = pd.to_datetime(cutoff_date, errors="raise")

    new_xl = pd.ExcelFile(new_file.file, engine="openpyxl")
    old_df = read_excel(old_file.file)

    if "Updated" not in new_xl.sheet_names:
        raise ValueError("Updated sheet missing")
    if "Price_Lookup" not in new_xl.sheet_names:
        raise ValueError("Price_Lookup missing")

    new_df = new_xl.parse("Updated")
    price_df = new_xl.parse("Price_Lookup")

    # ---------------------------------------------------------
    # ✅ Downcount OLD file using ONLY EBU rows after cutoff
    # ---------------------------------------------------------
    old_df.rename(columns=lambda x: str(x).strip(), inplace=True)
    if "Part #" in old_df.columns:
        old_df.rename(columns={"Part #": "Material"}, inplace=True)
    old_df["Purchasing Document"] = old_df["Purchasing Document"].apply(clean_po)

    ebu_sheets_all = read_excel(ebu_file.file, sheet_name=None)
    ebu_frames = []

    for name in EBU_SHEETS:
        if name not in ebu_sheets_all:
            continue

        header_row = 1 if name in SPECIAL_HEADER_SHEETS else 0
        df = read_excel(ebu_file.file, sheet_name=name, header=header_row)
        df.rename(columns=lambda x: str(x).strip(), inplace=True)

        if {"(a)P/N&S/N", "PO Number", "Ship Date", "(f) Qty"}.issubset(df.columns):
            temp = df[["(a)P/N&S/N", "PO Number", "Ship Date", "(f) Qty"]].copy()
            temp.rename(columns={
                "(a)P/N&S/N": "Material",
                "PO Number": "Purchasing Document"
            }, inplace=True)

            temp["Purchasing Document"] = temp["Purchasing Document"].apply(clean_po)
            temp["Ship Date"] = pd.to_datetime(temp["Ship Date"], errors="coerce")
            temp["(f) Qty"] = pd.to_numeric(temp["(f) Qty"], errors="coerce").fillna(0)
            ebu_frames.append(temp)

    ebu_tx = pd.concat(ebu_frames, ignore_index=True) if ebu_frames else pd.DataFrame(
        columns=["Material", "Purchasing Document", "Ship Date", "(f) Qty"]
    )

    ebu_tx = ebu_tx[(ebu_tx["Ship Date"].notna()) & (ebu_tx["Ship Date"] > cutoff_dt)]

    ebu_counts = (
        ebu_tx.groupby(["Material", "Purchasing Document"])["(f) Qty"]
        .sum()
        .to_dict()
    )

    for (mat, po), qty_to_remove in ebu_counts.items():
        if qty_to_remove <= 0:
            continue

        mask = (old_df["Material"] == mat) & (old_df["Purchasing Document"] == po)

        for idx in old_df[mask].index:
            if qty_to_remove <= 0:
                break

            available = old_df.at[idx, "Qty remaining to deliver"]
            if pd.notna(available) and available > 0:
                if available <= qty_to_remove:
                    qty_to_remove -= available
                    old_df.at[idx, "Qty remaining to deliver"] = 0
                else:
                    old_df.at[idx, "Qty remaining to deliver"] = available - qty_to_remove
                    qty_to_remove = 0

    # ---------------------------------------------------------
    # Existing delta logic (unchanged), now using adjusted old_df
    # ---------------------------------------------------------
    for df in [new_df, old_df]:
        df.rename(columns=lambda x: str(x).strip(), inplace=True)

        if "Part #" in df.columns:
            df.rename(columns={"Part #": "Material"}, inplace=True)

        df["Purchasing Document"] = df["Purchasing Document"].apply(clean_po)

        possible_cols = [
            c for c in df.columns
            if "stat" in c.lower() and "del" in c.lower() and "date" in c.lower()
        ]
        if not possible_cols:
            raise ValueError("Missing Stat.-Rel. Del. Date column")
        date_col = possible_cols[0]

        df["Stat_Rel_Date"] = pd.to_datetime(df[date_col], errors="coerce")
        df["Month"] = df["Stat_Rel_Date"].dt.to_period("M").astype(str)

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

    cumulative = pivot.copy()
    month_cols = sorted_cols[2:]
    for i in range(1, len(month_cols)):
        cumulative[month_cols[i]] = cumulative[month_cols[i-1]] + cumulative[month_cols[i]]

    price_df.rename(columns=lambda x: str(x).strip(), inplace=True)
    price_df["Purchasing Document"] = price_df["Purchasing Document"].apply(clean_po)
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
        [c for c in revenue_pivot.columns if c not in ["Material", "Purchasing Document"]]
    )
    revenue_pivot = revenue_pivot[revenue_sorted_cols]

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
