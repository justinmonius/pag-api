from fastapi import FastAPI, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import pandas as pd
import unicodedata
import re
import io

app = FastAPI()

# Allow your Vercel frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://pag-frontend.vercel.app"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def _norm(s: str) -> str:
    """Normalize a header: lowercase, strip, collapse spaces, remove accents."""
    s = unicodedata.normalize("NFKD", str(s))
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower().strip()
    s = re.sub(r"\s+", " ", s)
    return s

@app.post("/process")
async def process_files(
    pag_file: UploadFile = File(...),
    ship_file: UploadFile = File(...)
):
    # Read
    pag_df = pd.read_excel(pag_file.file)
    ship_df = pd.read_excel(ship_file.file)

    # ---- locate required columns with light header normalization ----
    ship_cols = { _norm(c): c for c in ship_df.columns }
    pag_cols  = { _norm(c): c for c in pag_df.columns }

    total_key_norm = "total general"            # covers "Total général" / "Total general"
    qty_key_norm   = "qty remaining to deliver" # target column in PAG

    if total_key_norm not in ship_cols:
        raise ValueError(
            f"Could not find 'Total général' column in Shipment vs Receipt. "
            f"Available: {list(ship_df.columns)}"
        )
    total_col = ship_cols[total_key_norm]

    if qty_key_norm not in pag_cols:
        raise ValueError(
            f"Could not find 'Qty remaining to deliver' column in PAG Integration. "
            f"Available: {list(pag_df.columns)}"
        )
    qty_col = pag_cols[qty_key_norm]

    # ---- compute total to remove (ensure numeric) ----
    total_to_remove = pd.to_numeric(ship_df[total_col], errors="coerce").fillna(0).sum()
    qty_series = pd.to_numeric(pag_df[qty_col], errors="coerce").fillna(0)

    # ---- sequential downcount across the PAG file ----
    qty_to_remove = float(total_to_remove)
    if qty_to_remove > 0:
        for idx in pag_df.index:
            if qty_to_remove <= 0:
                break
            available = float(qty_series.at[idx])
            if available <= 0:
                continue
            if available <= qty_to_remove:
                qty_series.at[idx] = 0
                qty_to_remove -= available
            else:
                qty_series.at[idx] = available - qty_to_remove
                qty_to_remove = 0

    # write back updated quantities
    pag_df[qty_col] = qty_series

    # ---- optional: make any datetime columns display as MM/DD/YYYY ----
    for col in pag_df.select_dtypes(include=["datetime64[ns]"]).columns:
        pag_df[col] = pag_df[col].dt.strftime("%m/%d/%Y")

    # Return Excel
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        pag_df.to_excel(writer, index=False, sheet_name="Updated")
    output.seek(0)

    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=updated_pag.xlsx"}
    )

@app.get("/")
def root():
    return {"message": "PAG API is live!"}
