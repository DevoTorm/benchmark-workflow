"""
Worker dédié au traitement ML (OCR + NER).

Task Queue: "ml-processing"
Activités: ocr_activity, ner_activity

Usage:
    python temporal_workflow/worker_ml.py
"""

import asyncio
import logging

from temporalio.client import Client
from temporalio.worker import Worker

from temporal_workflow.activities import ner_activity, ocr_activity
from utils import app_settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main():
    # Connexion au serveur Temporal
    client = await Client.connect(app_settings.TEMPORAL_HOST)

    # Créer le worker pour la task queue "ml-processing"
    worker = Worker(
        client,
        task_queue="ml-processing",
        workflows=[],  # Pas de workflow sur ce worker (seulement activités)
        activities=[ocr_activity, ner_activity],  # Activités ML
        max_concurrent_activities=10,  # Max 10 activités ML en parallèle
    )

    logger.info("🚀 Worker ML démarré sur task queue 'ml-processing'")
    logger.info("🤖 Activités disponibles: ocr_activity, ner_activity")
    logger.info("⚡ Concurrence max: 10 activités simultanées")
    logger.info("💡 Tip: Lancez plusieurs instances de ce worker pour scaler")

    # Lancer le worker (bloquant)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
