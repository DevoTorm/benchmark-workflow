from dataclasses import dataclass
from io import BytesIO, StringIO
from pathlib import Path

from minio import Minio
from pydantic_settings import BaseSettings
from pypdf import PdfReader


class AppSettings(BaseSettings):
    MINIO_ENDPOINT: str
    MINIO_ACCESS_KEY: str
    MINIO_SECRET_KEY: str
    MINIO_BUCKET_NAME: str
    OCR_URL: str
    NER_URL: str
    TEMPORAL_HOST: str = "localhost:7233"


app_settings = AppSettings()  # type: ignore


@dataclass
class PDFContent:
    text: str
    images: list[BytesIO]


def extract_pdf_content(pdf_path: BytesIO) -> PDFContent:
    """Extrait le texte et les images d'un PDF."""
    reader = PdfReader(pdf_path)

    text_parts = []
    images = []

    for page in reader.pages:
        text_parts.append(page.extract_text() or "")

        for img in page.images:
            images.append(BytesIO(img.data))

    return PDFContent(text="\n".join(text_parts), images=images)


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


@dataclass
class MinioClient(metaclass=Singleton):
    """
    Gestionnaire singleton pour les opérations MinIO.

    Exemple d'utilisation:
        # Première initialisation
        s3 = MinioClient(
            endpoint="localhost:9000",
            access_key="access",
            secret_key="secret",
            bucket_name="my-bucket",
            secure=False
        )

        # Upload
        s3.upload_file("local_file.pdf", "path/in/bucket/file.pdf")

        # Download
        s3.download_file("path/in/bucket/file.pdf", "downloaded_file.pdf")
    """

    endpoint: str = app_settings.MINIO_ENDPOINT
    access_key: str = app_settings.MINIO_ACCESS_KEY
    secret_key: str = app_settings.MINIO_SECRET_KEY
    bucket_name: str = app_settings.MINIO_BUCKET_NAME
    secure: bool = False

    def __post_init__(self):
        """Initialise le client MinIO après la création de l'instance."""
        self._client = Minio(
            self.endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=self.secure,
        )

        # Vérifie si le bucket existe, sinon le créer
        if not self._client.bucket_exists(self.bucket_name):
            self._client.make_bucket(self.bucket_name)

    def upload_file(self, local_path: str | Path, object_name: str) -> None:
        """
        Upload un fichier local vers MinIO.

        Args:
            local_path: Chemin du fichier local à uploader
            object_name: Nom/chemin de l'objet dans le bucket MinIO

        Raises:
            FileNotFoundError: Si le fichier local n'existe pas
        """
        local_path = Path(local_path)
        if not local_path.exists():
            raise FileNotFoundError(f"Le fichier {local_path} n'existe pas")

        self._client.fput_object(self.bucket_name, object_name, str(local_path))

    def download_file(self, object_name: str, local_path: str | Path) -> None:
        """
        Télécharge un fichier depuis MinIO vers le système local.

        Args:
            object_name: Nom/chemin de l'objet dans le bucket MinIO
            local_path: Chemin où sauvegarder le fichier localement
        """
        local_path = Path(local_path)
        local_path.parent.mkdir(parents=True, exist_ok=True)

        self._client.fget_object(self.bucket_name, object_name, str(local_path))

    def upload_data(
        self, data: BytesIO | StringIO, object_name: str, length: int = -1
    ) -> None:
        """
        Upload des données en mémoire (BytesIO) vers MinIO.

        Args:
            data: Objet BytesIO contenant les données à uploader
            object_name: Nom/chemin de l'objet dans le bucket MinIO
            length: Taille des données (-1 pour lire jusqu'à la fin)
        """
        if length == -1:
            data.seek(0, 2)  # Aller à la fin
            length = data.tell()
            data.seek(0)  # Revenir au début

        self._client.put_object(self.bucket_name, object_name, data, length)

    def download_data(self, object_name: str) -> BytesIO:
        """
        Télécharge un fichier depuis MinIO vers un objet BytesIO en mémoire.

        Args:
            object_name: Nom/chemin de l'objet dans le bucket MinIO

        Returns:
            BytesIO contenant les données du fichier
        """
        response = self._client.get_object(self.bucket_name, object_name)
        data = BytesIO(response.read())
        response.close()
        response.release_conn()
        data.seek(0)
        return data
