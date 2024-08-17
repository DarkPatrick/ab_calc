import requests
import pandas as pd
from typing import Any
from pydantic import BaseModel, Field
from json import dumps

class Mb_Client(BaseModel):
    url: str
    username: str
    password: str = Field(repr=False)
    session_header: dict = Field(default_factory=dict, repr=False)


    def model_post_init(self, __context: Any) -> None:
        self.get_session()


    def get_session(self) -> None:
        credentials: dict = {
            "username": self.username,
            "password": self.password
        }
        
        response = requests.post(
            f"{self.url}/api/session",
            json=credentials
        )

        session_id = response.json()["id"]
        setattr(self, "session_header", {"X-Metabase-Session": session_id})


    def post(self, api_endpoint: str, payload: dict) -> dict:
        post = requests.post(
            f'{self.url}/api/{api_endpoint}',
            headers = self.session_header | {
                # 'Accept': 'application/json',
                # "Content-Type": "application/json;charset=utf-8"
                "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8"
            },
            # json=payload
            params=[("query", dumps(payload))]
        )
        print("post")
        print(post.json())
        print(post)
        print("end of post")
        return post.json()
