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
    subinv_file: UploadFile = File(...)
):
    # Step 1: Read Excel files
    pag_df = pd.read_excel(pag_file.file)
    ship_df = pd.read_excel(ship_file.file)
    subinv_df = pd.read_excel(subinv_file.file)

    # -------------------------------
    # Normalize column names
    # -------------------------------
    for df in [pag_df, ship_df, subinv_df]:
        df.rename(columns=lambda x: str(x).strip(), inplace=True)

    # Standardize "Part #" column across all files
    if "Material" in pag_df.columns:
        pag_df.rename(columns={"Material": "Part #"}, inplace=True)
    if "(a)P/N&S/N" in ship_df.columns:
        ship_df.rename(columns={"(a)P/N&S/N": "Part #"}, inplace=True)
    # Subinv already uses "Part #"

    # -------------------------------
    # ROUND 1: Shipment downcounting
    # -------------------------------
    total_to_remove = -ship_df["Total général"].sum()

    qty_to_remove = total_to_remove
    for idx, row in pag_df.iterrows():
        if qty_to_remove <= 0:
            break
        if row["Qty remaining to deliver"] > 0:
            available = row["Qty remaining to deliver"]
            if available <= qty_to_remove:
                pag_df.at[idx, "Qty remaining to deliver"] = 0
                qty_to_remove -= available
            else:
                pag_df.at[idx, "Qty remaining to deliver"] = available - qty_to_remove
                qty_to_remove = 0

    # -------------------------------
    # ROUND 2: Sub-Inv Transfers downcounting
    # -------------------------------
    # Extract date from PackingSlip (first 8 chars = YYYYMMDD)
    ship_df["SlipDate"] = pd.to_datetime(
        ship_df["PackingSlip"].astype(str).str[:8],
        format="%Y%m%d",
        errors="coerce"
    )

    # Get latest SlipDate per Part # where Total général != 0
    latest_dates = (
        ship_df[ship_df["Total général"] != 0]
        .groupby("Part #")["SlipDate"]
        .max()
    )

    # Convert Sub-Inv Date column
    subinv_df["Date"] = pd.to_datetime(subinv_df["Date"], errors="coerce")

    # Count Sub-Inv rows after cutoff date for each part
    subinv_counts = {}
    for part, cutoff_date in latest_dates.items():
        if pd.notna(cutoff_date):
            mask = (subinv_df["Part #"] == part) & (subinv_df["Date"] > cutoff_date)
            subinv_counts[part] = mask.sum()

    # Apply second round of downcounting in PAG
    for part, qty_to_remove in subinv_counts.items():
        if qty_to_remove <= 0:
            continue

        part_rows = pag_df[pag_df["Part #"] == part].index
        for idx in part_rows:
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
    # Final formatting
    # -------------------------------
    for col in pag_df.select_dtypes(include=["datetime64[ns]"]).columns:
        pag_df[col] = pag_df[col].dt.strftime("%m/%d/%Y")

    # Step 5: Return updated Excel
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        pag_df.to_excel(writer, index=False, sheet_name="Updated")
        # Optional: save Sub-Inv counts for transparency
        pd.DataFrame.from_dict(subinv_counts, orient="index", columns=["SubInv_Count"]).to_excel(
            writer, sheet_name="SubInv_Counts"
        )
    output.seek(0)

    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=updated_pag.xlsx"}
    )


# Optional root endpoint for quick check
@app.get("/")
def root():
    return {"message": "PAG API is live!"}
