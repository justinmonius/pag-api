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
# MAIN ENDPOINT
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
    # ROUND 1: SHIPMENT DOWNCOUNTING (by Part # + PO)
    # -------------------------------
    ship_df["SlipDate"] = pd.to_datetime(
        ship_df["PackingSlip"].astype(str).str[:8],
        format="%Y%m%d",
        errors="coerce"
    )

    # Latest SlipDate per (Part, PO)
    ship_latest_dates = (
        ship_df
        .groupby(["Part #", "PO Number"])["SlipDate"]
        .max()
    )

    # Build total shipped map per (Part, PO)
    shipped_map = (
        ship_df.groupby(["Part #", "PO Number"])["Total général"]
        .sum()
        .to_dict()
    )

    # Apply shipment downcount to PAG
    for (part, po), total_shipped in shipped_map.items():
        qty_to_remove = -total_shipped  # negative = reduction
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
    # ROUND 2: EBU SHIP DOWNCOUNTING (by Part # + PO, using Qty)
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
    # Ensure all columns with 'Date' in the name are datetime
    for col in pag_df.columns:
        if "Date" in col:
            pag_df[col] = pd.to_datetime(pag_df[col], errors="coerce")

    # ✅ Rename "Part #" to "Material" ONLY for output
    pag_output = pag_df.rename(columns={"Part #": "Material"}).copy()

    # -------------------------------
    # EXCEL OUTPUT
    # -------------------------------
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        # Write main updated sheet
        pag_output.to_excel(writer, index=False, sheet_name="Updated")

        # Apply date formatting for 'Stat.-Rel Del. Date'
        ws = writer.sheets["Updated"]
        date_style = NamedStyle(name="date_style", number_format="MM/DD/YYYY")

        for cell in ws[1]:  # header row
            if cell.value == "Stat.-Rel Del. Date":
                col_idx = cell.column_letter
                for c in ws[col_idx][1:]:
                    c.style = date_style
                break

        # Write summary sheets
        ship_totals_df = pd.DataFrame([
            {"Part #": part, "PO Number": po, "Shipped_Total": total}
            for (part, po), total in shipped_map.items()
        ])
        ship_totals_df.to_excel(writer, index=False, sheet_name="Shipment_Totals")

        ship_latest_df = ship_latest_dates.reset_index().rename(
            columns={"Part #": "Part #", "PO Number": "PO Number", "SlipDate": "Latest_SlipDate"}
        )
        ship_latest_df.to_excel(writer, index=False, sheet_name="Shipment_Latest_Dates")

        ebu_qty_df = pd.DataFrame([
            {"Part #": part, "PO Number": po, "EBU_Qty_AfterCutoff": qty}
            for (part, po), qty in ebu_counts.items()
        ])
        ebu_qty_df.to_excel(writer, index=False, sheet_name="EBU_Qty_AfterCutoff")

    output.seek(0)

    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=updated_pag.xlsx"}
    )


# -------------------------------
# ROOT ENDPOINT
# -------------------------------
@app.get("/")
def root():
    return {"message": "PAG API is live!"}
