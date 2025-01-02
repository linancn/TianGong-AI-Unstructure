import os
import requests
import pickle

token = os.environ.get('TOKEN')

output_dir = 'test/gufei/pickle'

def unstructure_by_service(pdf_path, url, token):
    with open(pdf_path, 'rb') as f:
        base_name = os.path.basename(pdf_path)
        name, _ = os.path.splitext(base_name)
        pickle_path = os.path.join(output_dir, f"{name}.pkl")

        files = {'file': f}
        headers = {'Authorization': f'Bearer {token}'}
        response = requests.post(url, files=files, headers=headers)
        response.raise_for_status()
        response_data = response.json()
        result = response_data.get('result')

        with open(pickle_path, 'wb') as pkl_file:
            pickle.dump(result, pkl_file)

# 调用函数


dir_path = 'test/gufei/2'

# url = 'http://localhost:7770/pdf'
url = 'http://localhost:7771/pdf'
# url2 = 'http://localhost:7772/docs/'
for pdf in os.listdir(dir_path):
    pdf_path = os.path.join(dir_path, pdf)
    unstructure_by_service(pdf_path, url, token)