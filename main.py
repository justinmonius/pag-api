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

    # Step 2: Calculate total unbooked shipped MRAS
    unbooked = ship_df[
        (ship_df["Booked SNA"].isna()) | (ship_df["Booked SNA"] == 0)
    ]
    total_unbooked = -unbooked["Shipped MRAS"].sum()  # make positive

    # Step 3: Apply downcounting to PAG Integration
    qty_to_remove = total_unbooked

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


# Optional root endpoint for quick check
@app.get("/")
def root():
    return {"message": "PAG API is live!"}
