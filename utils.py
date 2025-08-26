from google.cloud import vision

def extract_text_from_file(file_content: bytes, mime_type: str) -> str:
    """
    Extracts text from an image or PDF file using Google Cloud Vision API,
    explicitly requesting a more advanced model for higher accuracy.
    """
    client = vision.ImageAnnotatorClient()
    
    # Define the advanced feature configuration to use the latest model.
    features = [
        vision.Feature(
            type_=vision.Feature.Type.DOCUMENT_TEXT_DETECTION,
            model="builtin/latest"
        )
    ]

    if mime_type in ['image/jpeg', 'image/png']:
        image = vision.Image(content=file_content)
        request = vision.AnnotateImageRequest(image=image, features=features)
        response = client.annotate_image(request=request)
        if response.error.message: raise Exception(response.error.message)
        return response.full_text_annotation.text
        
    elif mime_type == 'application/pdf':
        input_config = vision.InputConfig(content=file_content, mime_type=mime_type)
        request = vision.AnnotateFileRequest(input_config=input_config, features=features)
        response = client.batch_annotate_files(requests=[request])

        # NEW: Process the response to get text from all pages
        full_text = []
        for file_response in response.responses:
            if file_response.error.message: raise Exception(file_response.error.message)
            # The 'responses' list contains a separate response for each page
            for page_response in file_response.responses:
                if page_response.full_text_annotation:
                    full_text.append(page_response.full_text_annotation.text)
        
        return "\n\n--- Page Break ---\n\n".join(full_text)
        
    else:
        return "Unsupported File Type"
