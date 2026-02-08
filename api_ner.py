from fastapi import FastAPI
from pydantic import BaseModel

api = FastAPI(title="Fausse API de NER")


class ResultNER(BaseModel):
    text: str
    entity_name: str


@api.post("/ner")
async def ner(text: str) -> list[ResultNER]:
    return [ResultNER(text=text.split(" ")[0], entity_name="first_word")]
