"""
Worker combiné qui écoute toutes les task queues (pour développement local).

Task Queues: "pdf-extraction" ET "ml-processing"
Toutes les activités

Usage:
    python temporal_workflow/worker_all.py

Note: En production, préférez des workers séparés pour plus de flexibilité.
"""

import asyncio
import logging

from temporalio.client import Client
from temporalio.worker import Worker

from temporal_workflow.activities import (
    extract_pdf_content_activity,
    ner_activity,
    ocr_activity,
)
from temporal_workflow.workflows import ExtractionPDF
from utils import app_settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def run_worker(
    client: Client, task_queue: str, activities: list, workflows: list = []
):
    """Lance un worker pour une task queue donnée."""
    worker = Worker(
        client,
        task_queue=task_queue,
        workflows=workflows or [],
        activities=activities,
        max_concurrent_activities=10,
    )
    logger.info(f"✅ Worker démarré sur task queue '{task_queue}'")
    await worker.run()


async def main():
    # Connexion au serveur Temporal
    client = await Client.connect(app_settings.TEMPORAL_HOST)

    logger.info("🚀 Démarrage de tous les workers...")

    # Lancer les deux workers en parallèle
    await asyncio.gather(
        # Worker 1: PDF extraction + Workflow
        run_worker(
            client,
            task_queue="pdf-extraction",
            activities=[extract_pdf_content_activity],
            workflows=[ExtractionPDF],
        ),
        # Worker 2: ML processing
        run_worker(
            client,
            task_queue="ml-processing",
            activities=[ocr_activity, ner_activity],
        ),
    )


if __name__ == "__main__":
    asyncio.run(main())
