import requests
from typing import Dict, Optional, Any

class APIClient:
    def __init__(self, base_url : str, headers : Optional[Dict]) -> None:
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()
    
    def _make_request(self, method, endpoint:str, **kwargs):
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        print(url)
        try:
            response = self.session.request(method,url,**kwargs)
            response.raise_for_status() # ini untuk raise except, jika status jelek
            return response
        except requests.exceptions.RequestException as e:
            print(f"request failed : {e}")
            raise e
    
    def get(self,endpoint: str, params: Optional[Dict[str, Any]] = None):
        response = self._make_request('GET',endpoint,params=params)
        return response.json() if response.headers.get('content-type', '').startswith('application/json') else response.text


def main():
    client = APIClient("https://api.jikan.moe/v4",headers={})
    data = client.get('seasons/now',params={})
    print(data)

if __name__ == "__main__":
    main()
