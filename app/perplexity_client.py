import requests
from app.config import settings

class PerplexityClient:
    def __init__(self):
        self.base_url = "https://api.perplexity.ai"
        self.headers = {
            "Authorization": f"Bearer {settings.PPLX_API_KEY}",
            "Content-Type": "application/json"
        }

    def analyze_goal(self, stats: dict, goal: str) -> dict:
        """
        Call Perplexity sonar-pro to get explanation.
        Returns a dict with explanation and score.
        Raises for HTTP / network errors.
        """
        prompt = f"User goal: {goal}\nPortfolio stats: {stats}\nRespond with a 2-line explanation and also give me a numeric score 0-1 for goal alignment."
        # prepare payload
        payload = {
            "model": "sonar-pro",
            "messages": [{"role": "user", "content": prompt}],
        }
        # call API
        resp = requests.post(
            f"{self.base_url}/chat/completions",
            headers=self.headers,
            json=payload,
            timeout=30
        )
        resp.raise_for_status()  # raises HTTPError if not 200

        data = resp.json()
        text = data["choices"][0]["message"]["content"]
        # you can parse score separately if you instruct model to output JSON
        return {"explanation": text, "score": 0.7}
