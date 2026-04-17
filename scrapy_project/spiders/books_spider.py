import scrapy
import json
import redis
from datetime import datetime, timezone

RATING_MAP = {"One": 1, "Two": 2, "Three": 3, "Four": 4, "Five": 5}


# ─── SOURCE 1 : Books to Scrape ──────────────────────────────────────────────
class BooksToScrapeSpider(scrapy.Spider):
    name = "books_toscrape"
    start_urls = ["https://books.toscrape.com/"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        try:
            self.redis_client = redis.Redis(host="localhost", port=6379, db=0)
            self.redis_client.ping()
        except Exception:
            self.redis_client = None

    def _is_visited(self, url):
        if self.redis_client is None:
            return False
        return self.redis_client.sismember("visited_urls", url)

    def _mark_visited(self, url):
        if self.redis_client:
            self.redis_client.sadd("visited_urls", url)

    def parse(self, response):
        for cat in response.css("ul.nav-list li ul li a"):
            yield response.follow(cat.attrib["href"], self.parse_category)

    def parse_category(self, response):
        for book in response.css("article.product_pod"):
            detail_url = response.urljoin(book.css("h3 a").attrib["href"])
            if not self._is_visited(detail_url):
                self._mark_visited(detail_url)
                yield response.follow(detail_url, self.parse_book)

        next_page = response.css("li.next a::attr(href)").get()
        if next_page:
            yield response.follow(next_page, self.parse_category)

    def parse_book(self, response):
        table = {
            row.css("th::text").get(): row.css("td::text").get()
            for row in response.css("table.table tr")
        }
        yield {
            "titre":             response.css("h1::text").get("").strip(),
            "auteur":            "Unknown",
            "prix":              response.css("p.price_color::text").get("").strip(),
            "note_etoiles":      RATING_MAP.get(
                                     response.css("p.star-rating").attrib.get("class", "").split()[-1], 0
                                 ),
            "disponibilite":     response.css("p.instock::text").getall()[-1].strip()
                                 if response.css("p.instock") else "Out of stock",
            "categorie":         response.css("ul.breadcrumb li:nth-child(3) a::text").get("").strip(),
            "description":       response.css("article.product_page > p::text").get("").strip(),
            "isbn":              table.get("UPC", ""),
            "nb_avis":           int(table.get("Number of reviews", 0) or 0),
            "langue":            "en",
            "annee_publication": None,
            "url":               response.url,
            "source":            "books.toscrape.com",
            "date_scraping":     datetime.now(timezone.utc).isoformat(),
        }


# ─── SOURCE 2 : Open Library API ─────────────────────────────────────────────
class OpenLibrarySpider(scrapy.Spider):
    name = "books_openlibrary"

    SUBJECTS = ["fiction", "mystery", "science", "history", "fantasy", "romance"]
    API_URL = "https://openlibrary.org/subjects/{subject}.json?limit=50&offset={offset}"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        try:
            self.redis_client = redis.Redis(host="localhost", port=6379, db=0)
            self.redis_client.ping()
        except Exception:
            self.redis_client = None

    def _is_visited(self, url):
        if self.redis_client is None:
            return False
        return self.redis_client.sismember("visited_urls", url)

    def _mark_visited(self, url):
        if self.redis_client:
            self.redis_client.sadd("visited_urls", url)

    def start_requests(self):
        for subject in self.SUBJECTS:
            url = self.API_URL.format(subject=subject, offset=0)
            yield scrapy.Request(url, callback=self.parse_subject,
                                 cb_kwargs={"subject": subject, "offset": 0})

    def parse_subject(self, response, subject, offset):
        data = json.loads(response.text)
        works = data.get("works", [])

        for work in works:
            key = work.get("key", "")
            url = f"https://openlibrary.org{key}.json"
            if not self._is_visited(url):
                self._mark_visited(url)
                yield scrapy.Request(url, callback=self.parse_book,
                                     cb_kwargs={"subject": subject, "work": work})

        if len(works) == 50:
            next_offset = offset + 50
            next_url = self.API_URL.format(subject=subject, offset=next_offset)
            yield scrapy.Request(next_url, callback=self.parse_subject,
                                 cb_kwargs={"subject": subject, "offset": next_offset})

    def parse_book(self, response, subject, work):
        data = json.loads(response.text)

        authors = work.get("authors", [])
        auteur = authors[0].get("name", "Unknown") if authors else "Unknown"

        desc = data.get("description", "")
        if isinstance(desc, dict):
            desc = desc.get("value", "")

        isbn = ""
        isbn_list = data.get("isbn_13") or data.get("isbn_10") or []
        if isbn_list:
            isbn = isbn_list[0]

        yield {
            "titre":             work.get("title", "").strip(),
            "auteur":            auteur,
            "prix":              None,
            "note_etoiles":      None,
            "disponibilite":     "N/A",
            "categorie":         subject.capitalize(),
            "description":       desc[:500] if desc else "",
            "isbn":              isbn,
            "nb_avis":           work.get("edition_count", 0),
            "langue":            "eng",
            "annee_publication": work.get("first_publish_year"),
            "url":               f"https://openlibrary.org{work.get('key', '')}",
            "source":            "openlibrary.org",
            "date_scraping":     datetime.now(timezone.utc).isoformat(),
        }


# ─── SOURCE 3 : Google Books API ─────────────────────────────────────────────
class GoogleBooksSpider(scrapy.Spider):
    name = "books_google"

    QUERIES = ["fiction bestseller", "mystery novel", "science history",
               "fantasy epic", "romance novel", "thriller suspense"]
    API_URL = "https://www.googleapis.com/books/v1/volumes?q={query}&maxResults=40&startIndex={offset}&key=AIzaSyA9DpsTi9sCVPehmH9gavIU5YVN8TldS54"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        try:
            self.redis_client = redis.Redis(host="localhost", port=6379, db=0)
            self.redis_client.ping()
        except Exception:
            self.redis_client = None

    def _is_visited(self, url):
        if self.redis_client is None:
            return False
        return self.redis_client.sismember("visited_urls", url)

    def _mark_visited(self, url):
        if self.redis_client:
            self.redis_client.sadd("visited_urls", url)

    def start_requests(self):
        for query in self.QUERIES:
            url = self.API_URL.format(query=query.replace(" ", "+"), offset=0)
            yield scrapy.Request(url, callback=self.parse_results,
                                 cb_kwargs={"query": query, "offset": 0})

    def parse_results(self, response, query, offset):
        data = json.loads(response.text)
        items = data.get("items", [])

        for item in items:
            book_id = item.get("id", "")
            url = f"https://www.googleapis.com/books/v1/volumes/{book_id}"
            if not self._is_visited(url):
                self._mark_visited(url)
                yield scrapy.Request(url, callback=self.parse_book)

        if len(items) == 40 and offset < 160:
            next_offset = offset + 40
            next_url = self.API_URL.format(query=query.replace(" ", "+"), offset=next_offset)
            yield scrapy.Request(next_url, callback=self.parse_results,
                                 cb_kwargs={"query": query, "offset": next_offset})

    def parse_book(self, response):
        data = json.loads(response.text)
        info = data.get("volumeInfo", {})
        sale = data.get("saleInfo", {})

        prix = None
        if sale.get("listPrice"):
            prix = f"{sale['listPrice'].get('amount', '')} {sale['listPrice'].get('currencyCode', '')}"

        isbn = ""
        for identifier in info.get("industryIdentifiers", []):
            if identifier.get("type") == "ISBN_13":
                isbn = identifier.get("identifier", "")
                break

        categories = info.get("categories", ["Unknown"])

        yield {
            "titre":             info.get("title", "").strip(),
            "auteur":            ", ".join(info.get("authors", ["Unknown"])),
            "prix":              prix,
            "note_etoiles":      info.get("averageRating"),
            "disponibilite":     sale.get("saleability", "N/A"),
            "categorie":         categories[0] if categories else "Unknown",
            "description":       info.get("description", "")[:500],
            "isbn":              isbn,
            "nb_avis":           info.get("ratingsCount", 0),
            "langue":            info.get("language", "en"),
            "annee_publication": int(info.get("publishedDate", "0")[:4] or 0) or None,
            "url":               info.get("infoLink", response.url),
            "source":            "google_books",
            "date_scraping":     datetime.now(timezone.utc).isoformat(),
        }