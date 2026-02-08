from dataclasses import dataclass
from io import BytesIO

import httpx
from temporalio import activity

from utils import MinioClient, app_settings, extract_pdf_content

minio_client = MinioClient()

async_client_ocr = httpx.AsyncClient(base_url=app_settings.OCR_URL)
async_client_ner = httpx.AsyncClient(base_url=app_settings.NER_URL)


@dataclass
class PDFInput:
    s3_link: str


@dataclass
class PDFContents:
    text_link: str
    images_links: list[str]


@dataclass
class ImageLink:
    link: str


@dataclass
class TextLink:
    link: str


@dataclass
class NerResult:
    text: str
    entity_name: str


def get_prefix_from_activity(activity_info: activity.Info) -> str:
    workflow_id = activity_info.workflow_id
    run_id = activity_info.workflow_run_id
    activity_type = activity_info.activity_type
    return f"{workflow_id}/{run_id}/{activity_type}"


@activity.defn(name="extract_pdf_content")
async def extract_pdf_content_activity(pdf_input: PDFInput) -> PDFContents:
    prefix_minio = get_prefix_from_activity(activity.info())
    pdf_content = minio_client.download_data(pdf_input.s3_link)
    extracted_content = extract_pdf_content(pdf_content)
    # Upload text
    output = PDFContents(
        text_link=f"{prefix_minio}/text",
        images_links=[
            f"{prefix_minio}/image_{i}_{len(extracted_content.images)}"
            for i in range(len(extracted_content.images))
        ],
    )

    # Upload
    minio_client.upload_data(
        BytesIO(extracted_content.text.encode()), object_name=output.text_link
    )
    for i, image_content in enumerate(extracted_content.images):
        minio_client.upload_data(data=image_content, object_name=output.images_links[i])
    return output


@activity.defn(name="ocr_activity")
async def ocr_activity(image: ImageLink) -> TextLink:
    image_content = minio_client.download_data(image.link)
    res_ocr = await async_client_ocr.post(
        "/ocr",
        files={"image_file": image_content.read()},
    )
    res_ocr.raise_for_status()
    minio_client.upload_data(
        data=BytesIO(res_ocr.json().encode("utf-8")),
        object_name=f"{image.link}_ocerized",
    )
    return TextLink(link=f"{image.link}_ocerized")


@activity.defn(name="ner_activity")
async def ner_activity(text: TextLink) -> list[NerResult]:
    downloaded_text = minio_client.download_data(text.link).read().decode("utf-8")
    res_ner = await async_client_ner.post("/ner", params={"text": downloaded_text})
    res_ner.raise_for_status()
    return [
        NerResult(
            text=res_entity["text"],
            entity_name=res_entity["entity_name"],
        )
        for res_entity in res_ner.json()
    ]
