import random

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/118.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Edge/120.0.0.0 Safari/537.36",
]

class RandomUserAgentMiddleware:
    def process_request(self, request, spider):
        request.headers["User-Agent"] = random.choice(USER_AGENTS)


class ProxyMiddleware:
    """
    Rotation de proxies — ajoute tes proxies dans la liste PROXIES
    Si pas de proxies, le middleware est ignoré automatiquement
    """
    PROXIES = [
        # "http://proxy1:port",
        # "http://proxy2:port",
    ]

    def process_request(self, request, spider):
        if self.PROXIES:
            proxy = random.choice(self.PROXIES)
            request.meta["proxy"] = proxy
            spider.logger.debug(f"[Proxy] Utilisation de : {proxy}")