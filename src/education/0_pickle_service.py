import os
import requests
import pickle

token = os.environ.get('TOKEN')

output_dir = 'temp'

def unstructure_by_service(doc_path, url, token):
    with open(doc_path, 'rb') as f:
        base_name = os.path.basename(doc_path)
        pickle_path = os.path.join(output_dir, f"{base_name}.pkl")

        files = {'file': f}
        headers = {'Authorization': f'Bearer {token}'}
        response = requests.post(url, files=files, headers=headers)
        response.raise_for_status()
        response_data = response.json()
        result = response_data.get('result')

        with open(pickle_path, 'wb') as pkl_file:
            pickle.dump(result, pkl_file)

# 调用函数


dir_path = 'test/edu_new'

pdf_url = 'http://localhost:8770/pdf'
docx_url = 'http://localhost:8770/docx'
ppt_url = 'http://localhost:8770/ppt'


for doc in os.listdir(dir_path):
    if doc.endswith('.pdf'):
        doc_path = os.path.join(dir_path, doc)
        unstructure_by_service(doc_path, pdf_url, token)
    elif doc.endswith('.docx'):
        doc_path = os.path.join(dir_path, doc)
        unstructure_by_service(doc_path, docx_url, token)
    elif doc.endswith('.pptx'):
        doc_path = os.path.join(dir_path, doc)
        unstructure_by_service(doc_path, ppt_url, token)

    