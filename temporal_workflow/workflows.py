import asyncio
from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from temporal_workflow.activities import (
        ImageLink,
        PDFInput,
        TextLink,
        extract_pdf_content_activity,
        ner_activity,
        ocr_activity,
    )


@workflow.defn(name="ExtractionPDF")
class ExtractionPDF:
    @workflow.run
    async def run(self, pdf_s3_link: str):
        # 1. Extraire le contenu du PDF (Task Queue: pdf-extraction)
        pdf_contents = await workflow.execute_activity(
            extract_pdf_content_activity,
            PDFInput(s3_link=pdf_s3_link),
            start_to_close_timeout=timedelta(minutes=5),
            task_queue="pdf-extraction",  # Worker spécialisé PDF
        )

        # 2. NER sur le texte simple (Task Queue: ml-processing)
        ner_text_task = workflow.execute_activity(
            ner_activity,
            TextLink(link=pdf_contents.text_link),
            start_to_close_timeout=timedelta(minutes=2),
            task_queue="ml-processing",  # Worker spécialisé ML
        )

        # 3. Premier fan-out : OCR sur toutes les images EN PARALLÈLE
        ocr_tasks = [
            workflow.execute_activity(
                ocr_activity,
                ImageLink(link=image_link),
                start_to_close_timeout=timedelta(minutes=2),
                task_queue="ml-processing",  # Worker spécialisé ML
            )
            for image_link in pdf_contents.images_links
        ]

        # 4. Fan-in : Attendre tous les résultats OCR
        ner_text_result, *ocr_results = await asyncio.gather(ner_text_task, *ocr_tasks)

        # 5. Deuxième fan-out : NER sur chaque résultat d'OCR EN PARALLÈLE
        ner_ocr_tasks = [
            workflow.execute_activity(
                ner_activity,
                ocr_result,  # ocr_result est déjà un TextLink
                start_to_close_timeout=timedelta(minutes=2),
                task_queue="ml-processing",  # Worker spécialisé ML
            )  # type: ignore
            for ocr_result in ocr_results
        ]

        # 6. Fan-in final : Attendre tous les résultats NER
        ner_ocr_results = await asyncio.gather(*ner_ocr_tasks)

        return {
            "ner_text": ner_text_result,
            "ocr_results": ocr_results,
            "ner_ocr_results": ner_ocr_results,
            "total_images": len(ocr_results),
        }
