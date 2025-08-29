# utils.py
from google.cloud import vision
import io

def extract_text_from_file(file_content: bytes, mime_type: str) -> str:
    """
    Extracts text from images and PDFs using Google Cloud Vision.
    Returns text as a single string. On failure returns an empty string.
    """
    if not file_content:
        return ""

    client = vision.ImageAnnotatorClient()

    try:
        # Images: single image text detection (document_text_detection)
        if mime_type and mime_type.startswith("image/"):
            image = vision.Image(content=file_content)
            response = client.document_text_detection(image=image)
            if response.error.message:
                raise Exception(response.error.message)
            return response.full_text_annotation.text or ""

        # PDF: use batch_annotate_files and combine page texts
        if mime_type == 'application/pdf' or (mime_type is None and file_content[:4] == b"%PDF"):
            # annotate file
            input_config = vision.InputConfig(content=file_content, mime_type='application/pdf')
            feature = vision.Feature(type_=vision.Feature.Type.DOCUMENT_TEXT_DETECTION)
            request = vision.AnnotateFileRequest(input_config=input_config, features=[feature])
            response = client.batch_annotate_files(requests=[request])
            # response.responses is a list of AnnotateFileResponse; each has responses per page
            pages_text = []
            for resp in response.responses:
                # resp.responses is list of AnnotateImageResponse for each page
                for page_resp in resp.responses:
                    if page_resp.error.message:
                        raise Exception(page_resp.error.message)
                    if page_resp.full_text_annotation and page_resp.full_text_annotation.text:
                        pages_text.append(page_resp.full_text_annotation.text)
            return "\n\n--- Page Break ---\n\n".join(pages_text)

        # fallback: try single-image annotate
        image = vision.Image(content=file_content)
        response = client.document_text_detection(image=image)
        if response.error.message:
            raise Exception(response.error.message)
        return response.full_text_annotation.text or ""
    except Exception as e:
        # return empty string on OCR failure (worker will handle empty)
        return ""
