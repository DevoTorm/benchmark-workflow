"""
Worker dédié à l'extraction de PDF.

Task Queue: "pdf-extraction"
Activités: extract_pdf_content_activity

Usage:
    python temporal_workflow/worker_pdf.py
"""

import asyncio
import logging

from temporalio.client import Client
from temporalio.worker import Worker

from temporal_workflow.activities import extract_pdf_content_activity
from temporal_workflow.workflows import ExtractionPDF
from utils import app_settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main():
    # Connexion au serveur Temporal
    client = await Client.connect(app_settings.TEMPORAL_HOST)

    # Créer le worker pour la task queue "pdf-extraction"
    worker = Worker(
        client,
        task_queue="pdf-extraction",
        workflows=[ExtractionPDF],  # Le workflow principal
        activities=[extract_pdf_content_activity],  # Activité d'extraction PDF
        max_concurrent_activities=5,  # Max 5 PDFs extraits en parallèle
    )

    logger.info("🚀 Worker PDF démarré sur task queue 'pdf-extraction'")
    logger.info("📄 Activités disponibles: extract_pdf_content_activity")
    logger.info("⚡ Concurrence max: 5 activités simultanées")

    # Lancer le worker (bloquant)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
