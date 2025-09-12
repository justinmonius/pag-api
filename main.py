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
    ship_file: UploadFile = File(...)
):
    # Step 1: Read Excel files
    pag_df = pd.read_excel(pag_file.file)
    ship_df = pd.read_excel(ship_file.file)

    # Step 2: Group Shipment vs Receipt by PO Number + P/N
    # Match keys: PO Number <-> Purchasing Document, (a)P/N&S/N <-> Material
    grouped_ship = (
        ship_df.groupby(["PO Number", "(a)P/N&S/N"])["Total général"]
        .sum()
        .reset_index()
    )

    # Step 3: Downcount in PAG Integration per (PO Number, Material)
    for _, row in grouped_ship.iterrows():
        po_number = row["PO Number"]
        material = row["(a)P/N&S/N"]
        qty_to_remove = row["Total général"]

        # Filter matching rows in PAG Integration
        matching_rows = pag_df[
            (pag_df["Purchasing Document"] == po_number) &
            (pag_df["Material"] == material)
        ]

        for idx, pag_row in matching_rows.iterrows():
            if qty_to_remove <= 0:
                break
            if pag_row["Qty remaining to deliver"] > 0:
                available = pag_row["Qty remaining to deliver"]
                if available <= qty_to_remove:
                    pag_df.at[idx, "Qty remaining to deliver"] = 0
                    qty_to_remove -= available
                else:
                    pag_df.at[idx, "Qty remaining to deliver"] = available - qty_to_remove
                    qty_to_remove = 0

    # Step 4: Convert datetime columns to MM/DD/YYYY strings
    for col in pag_df.select_dtypes(include=["datetime64[ns]"]).columns:
        pag_df[col] = pag_df[col].dt.strftime("%m/%d/%Y")

    # Step 5: Return updated Excel
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        pag_df.to_excel(writer, index=False, sheet_name="Updated")
    output.seek(0)

    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=updated_pag.xlsx"}
    )


# Optional root endpoint
@app.get("/")
def root():
    return {"message": "PAG API is live!"}
