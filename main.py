import requests
from typing import Dict, Optional, Any
from urllib.parse import urlencode, urlparse, urlunparse

class APIClient:
    def __init__(self, base_url : str, headers : Optional[Dict]) -> None:
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()

    def generate_url(self,path='', query_dict={}):
        scheme = 'https'
        netloc = self.base_url

        new_query = urlencode(query_dict,doseq=True)

        new_url = urlunparse((scheme,netloc,path,'',new_query,''))
        return new_url
    
    def _make_request(self, method, endpoint:str, params:dict):
        url = self.generate_url(path=endpoint, query_dict=params)
        print(url)
        try:
            response = self.session.request(method,url,params)
            response.raise_for_status() # ini untuk raise except, jika status jelek
            return response
        except requests.exceptions.RequestException as e:
            print(f"request failed : {e}")
            raise e
    
    def get(self,endpoint: str, params= {}):
        response = self._make_request('GET',endpoint,params)
        return response.json() if response.headers.get('content-type', '').startswith('application/json') else response.text


def main():
    client = APIClient("api.jikan.moe/v4",headers={})
    data = client.get('seasons/now', params={'page':2})
    print(data)

if __name__ == "__main__":
    main()
