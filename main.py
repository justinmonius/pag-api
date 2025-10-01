from fastapi import FastAPI, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import pandas as pd
import io

app = FastAPI()

# CORS - allow your frontend (adjust if needed)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://pag-frontend.vercel.app"],  # your Vercel frontend URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.post("/process")
async def process_files(
    pag_file: UploadFile = File(...),
    ship_file: UploadFile = File(...),
    ebu_file: UploadFile = File(...)
):
    # -------------------------------
    # Step 1: Read Excel files
    # -------------------------------
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

    # Ensure PO column names exist
    if "Purchasing Document" not in pag_df.columns:
        raise ValueError("PAG file must contain 'Purchasing Document' column")
    if "PO Number" not in ship_df.columns:
        raise ValueError("Shipment file must contain 'PO Number' column")

    # -------------------------------
    # ROUND 1: Shipment downcounting (by Part # + PO)
    # -------------------------------
    ship_df["SlipDate"] = pd.to_datetime(
        ship_df["PackingSlip"].astype(str).str[:8],
        format="%Y%m%d",
        errors="coerce"
    )

    # Track latest date per (Part, PO) where Total général != 0
    ship_latest_dates = (
        ship_df[ship_df["Total général"] != 0]
        .groupby(["Part #", "PO Number"])["SlipDate"]
        .max()
    )

    # Build a map of total shipped per (Part #, PO Number)
    shipped_map = (
        ship_df.groupby(["Part #", "PO Number"])["Total général"]
        .sum()
        .to_dict()
    )

    # Apply downcounting to PAG
    for (part, po), total_shipped in shipped_map.items():
        qty_to_remove = -total_shipped  # negative means reduce
        if qty_to_remove <= 0:
            continue

        mask = (pag_df["Part #"] == part) & (pag_df["Purchasing Document"] == po)
        for idx in pag_df[mask].index:
            if qty_to_remove <= 0:
                break
            if pag_df.at[idx, "Qty remaining to deliver"] > 0:
                available = pag_df.at[idx, "Qty remaining to deliver"]
                if available <= qty_to_remove:
                    pag_df.at[idx, "Qty remaining to deliver"] = 0
                    qty_to_remove -= available
                else:
                    pag_df.at[idx, "Qty remaining to deliver"] = available - qty_to_remove
                    qty_to_remove = 0

    # -------------------------------
    # ROUND 2: EBU Ship downcounting (by Part # + PO)
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

        if "(a)P/N&S/N" in df.columns and "PO Number" in df.columns and "Ship Date" in df.columns:
            df = df[["(a)P/N&S/N", "PO Number", "Ship Date"]].copy()
            df.rename(columns={"(a)P/N&S/N": "Part #"}, inplace=True)
            df["Ship Date"] = pd.to_datetime(df["Ship Date"], errors="coerce")
            ebu_frames.append(df)

    if ebu_frames:
        ebu_df = pd.concat(ebu_frames, ignore_index=True)

        # Count rows AFTER the cutoff date from Shipment vs Receipt
        ebu_counts = {}
        for (part, po), cutoff_date in ship_latest_dates.items():
            if pd.notna(cutoff_date):
                mask = (
                    (ebu_df["Part #"] == part) &
                    (ebu_df["PO Number"] == po) &
                    (ebu_df["Ship Date"] > cutoff_date)
                )
                ebu_counts[(part, po)] = mask.sum()

        # Apply downcounting to PAG
        for (part, po), qty_to_remove in ebu_counts.items():
            if qty_to_remove <= 0:
                continue

            mask = (pag_df["Part #"] == part) & (pag_df["Purchasing Document"] == po)
            for idx in pag_df[mask].index:
                if qty_to_remove <= 0:
                    break
                if pag_df.at[idx, "Qty remaining to deliver"] > 0:
                    available = pag_df.at[idx, "Qty remaining to deliver"]
                    if available <= qty_to_remove:
                        pag_df.at[idx, "Qty remaining to deliver"] = 0
                        qty_to_remove -= available
                    else:
                        pag_df.at[idx, "Qty remaining to deliver"] = available - qty_to_remove
                        qty_to_remove = 0
    else:
        ebu_counts = {}

    # -------------------------------
    # Final formatting
    # -------------------------------
    for col in pag_df.select_dtypes(include=["datetime64[ns]"]).columns:
        pag_df[col] = pag_df[col].dt.strftime("%m/%d/%Y")

    # Step 5: Return updated Excel
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        pag_df.to_excel(writer, index=False, sheet_name="Updated")
        pd.DataFrame.from_dict(
            {str(k): v for k, v in shipped_map.items()},
            orient="index", columns=["Shipped_Total"]
        ).to_excel(writer, sheet_name="Shipment_Totals")
        pd.DataFrame.from_dict(
            {str(k): v for k, v in ebu_counts.items()},
            orient="index", columns=["EBU_Count"]
        ).to_excel(writer, sheet_name="EBU_Counts")
    output.seek(0)

    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=updated_pag.xlsx"}
    )


@app.get("/")
def root():
    return {"message": "PAG API is live!"}
