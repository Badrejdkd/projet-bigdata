BOT_NAME = "scrapy_project"

SPIDER_MODULES = ["scrapy_project.spiders"]
NEWSPIDER_MODULE = "scrapy_project.spiders"

DOWNLOAD_DELAY = 1.5
RANDOMIZE_DOWNLOAD_DELAY = True
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 1
AUTOTHROTTLE_MAX_DELAY = 5

RETRY_ENABLED = True
RETRY_TIMES = 3
RETRY_HTTP_CODES = [500, 502, 503, 504, 408, 429]

ITEM_PIPELINES = {
    "scrapy_project.pipelines.MinIOPipeline": 300,
    "scrapy_project.pipelines.KafkaPipeline": 400,
}

LOG_LEVEL = "INFO"
ROBOTSTXT_OBEY = True
FEED_EXPORT_ENCODING = "utf-8"

# Rotation User-Agents
DOWNLOADER_MIDDLEWARES = {
    "scrapy.downloadermiddlewares.useragent.UserAgentMiddleware": None,
    "scrapy_project.middlewares.RandomUserAgentMiddleware": 400,
    "scrapy_project.middlewares.ProxyMiddleware": 410,
}

# Délais aléatoires anti-blocage
DOWNLOAD_DELAY = 1.5
RANDOMIZE_DOWNLOAD_DELAY = True
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 1
AUTOTHROTTLE_MAX_DELAY = 5
AUTOTHROTTLE_TARGET_CONCURRENCY = 2.0