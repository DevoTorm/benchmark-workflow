from fastapi import FastAPI, UploadFile
from io import BytesIO

api = FastAPI(title="Fausse API d'OCR")


@api.post("/ocr")
async def ocr(image_file: UploadFile) -> str:
    file_content = await image_file.read()
    file_content_io = BytesIO(file_content) 
    return f"Texte de {image_file.filename}"
