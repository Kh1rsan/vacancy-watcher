import asyncio
import hashlib
import json
import logging
import os
import re
from pathlib import Path
from urllib.parse import urlencode, urljoin
from dotenv import load_dotenv

import aiohttp
from bs4 import BeautifulSoup

BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / "config.json"
SENT_PATH = BASE_DIR / "telegram_sent_vacancies.json"
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
load_dotenv()


def load_config():
    return json.loads(CONFIG_PATH.read_text(encoding="utf-8-sig"))


def norm(text):
    return re.sub(r"\s+", " ", text.lower().replace("ё", "е")).strip()


def job_fingerprint(job):
    """Build a duplicate key only when employer or location is available."""
    employer = norm(job.get("employer", ""))
    location = norm(job.get("location", ""))
    if not employer and not location:
        return None
    identity = "|".join((norm(job.get("title", "")), employer, location))
    return "fp:" + hashlib.sha256(identity.encode("utf-8")).hexdigest()


def load_sent():
    if not SENT_PATH.exists():
        return set()

    try:
        return set(json.loads(SENT_PATH.read_text(encoding="utf-8")))
    except (json.JSONDecodeError, OSError):
        logging.warning("Не удалось прочитать %s; начинаю без истории отправок", SENT_PATH.name)
        return set()


async def send_telegram(session, token, chat_id, job, timeout):
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    employer = f"\nРаботодатель: {job['employer']}" if job.get("employer") else ""
    location = f"\nМесто: {job['location']}" if job.get("location") else ""
    payload = {"chat_id": chat_id, "text": f"{job['title']}{employer}{location}\n{job['url']}"}
    async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=timeout)) as response:
        result = await response.json(content_type=None)
        if response.status != 200 or not result.get("ok"):
            description = result.get("description", response.reason)
            raise RuntimeError(f"Telegram API вернул HTTP {response.status}: {description}")


def save_sent(sent):
    SENT_PATH.write_text(json.dumps(sorted(sent), ensure_ascii=False, indent=2), encoding="utf-8")


async def get_text(session, url, timeout, semaphore, attempts=3):
    """Fetch one page asynchronously, retrying transient disconnects/throttling."""
    async with semaphore:
        for attempt in range(attempts):
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as response:
                    if (response.status == 429 or response.status >= 500) and attempt + 1 < attempts:
                        delay = 3 * (2 ** attempt)
                        logging.warning("HH ответил HTTP %s; повтор через %s сек.", response.status, delay)
                        await asyncio.sleep(delay)
                        continue
                    response.raise_for_status()
                    return await response.text()
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                if attempt + 1 >= attempts:
                    raise
                delay = 3 * (2 ** attempt)
                logging.warning("Сбой соединения с HH (%s); повтор через %s сек.", exc, delay)
                await asyncio.sleep(delay)


def parse_search_page(html, page, url):
    soup = BeautifulSoup(html, "html.parser")
    cards = soup.select('[data-qa="vacancy-serp__vacancy"]')
    if not cards:
        cards = soup.select('article[data-qa*="vacancy"], div[data-qa*="vacancy-serp__vacancy"]')
    if not cards:
        return None, 0, "не указан"

    count_match = re.search(r"Найдено\s+([\d\s\u00a0]+)\s+ваканс", soup.get_text(" ", strip=True), re.IGNORECASE)
    reported_count = re.sub(r"\s+", "", count_match.group(1)) if count_match else "не указан"
    candidates = []
    for card in cards:
        link = card.select_one('[data-qa="vacancy-serp__vacancy-title"]')
        if not link:
            link = card.select_one('a[data-qa*="vacancy-title"], a[href*="/vacancy/"]')
        if not link:
            link = next((a for a in card.select('a[href]') if re.search(r"/vacancy/\d+", a.get("href", ""))), None)
        if not link or not link.get("href"):
            continue

        title = link.get_text(" ", strip=True)
        if not title:
            title_node = card.select_one('h2, [data-qa*="vacancy-title"]')
            title = title_node.get_text(" ", strip=True) if title_node else "Вакансия"
        employer = card.select_one('[data-qa*="vacancy-serp__vacancy-employer"], [data-qa*="vacancy-employer"]')
        location = card.select_one('[data-qa*="vacancy-serp__vacancy-address"], [data-qa*="vacancy-address"]')
        vacancy_url = urljoin("https://hh.ru", link["href"])
        candidates.append({
            "id": vacancy_url.split("?")[0].rstrip("/").split("/")[-1],
            "title": title,
            "url": vacancy_url,
            "snippet": card.get_text(" ", strip=True),
            "employer": employer.get_text(" ", strip=True) if employer else "",
            "location": location.get_text(" ", strip=True) if location else "",
        })

    # Fallback for page layouts where vacancy links are not nested in cards.
    if not candidates:
        for link in soup.select('a[href*="/vacancy/"]'):
            href = link.get("href", "")
            if not re.search(r"/vacancy/\d+", href):
                continue
            vacancy_url = urljoin("https://hh.ru", href)
            vacancy_id = vacancy_url.split("?")[0].rstrip("/").split("/")[-1]
            candidates.append({
                "id": vacancy_id,
                "title": link.get_text(" ", strip=True) or "Вакансия",
                "url": vacancy_url,
                "snippet": link.parent.get_text(" ", strip=True) if link.parent else "",
                "employer": "",
                "location": "",
            })
    return candidates, len(cards), reported_count


async def scrape(config, sent=None):
    sent = sent or set()
    concurrency = max(1, int(config.get("concurrency", 4)))
    semaphore = asyncio.Semaphore(concurrency)
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
    }
    connector = aiohttp.TCPConnector(limit=concurrency)
    timeout = config["request_timeout_seconds"]
    all_candidates = []
    total_cards = 0
    total_links = 0
    empty_page = None
    max_pages = int(config["max_pages"])
    if max_pages < 0:
        raise ValueError("max_pages должен быть 0 или положительным числом")
    batch_size = concurrency
    page_limit_text = "до конца выдачи" if max_pages == 0 else str(max_pages)
    logging.info("Пагинация: %s страниц, по %s вакансий; параллельных запросов=%s; без опыта=%s", page_limit_text, config.get("per_page", 50), concurrency, config.get("no_experience_only", False))

    async with aiohttp.ClientSession(headers=headers, connector=connector) as session:
        # Fetch listing pages in small concurrent batches; stop at the first empty page.
        batch_start = 0
        while max_pages == 0 or batch_start < max_pages:
            batch_end = batch_start + batch_size if max_pages == 0 else min(batch_start + batch_size, max_pages)
            page_numbers = range(batch_start, batch_end)

            async def fetch_listing(page):
                params = {
                    "text": config["search_text"],
                    "area": config["region_id"],
                    "page": page,
                    "items_on_page": config.get("per_page", 50),
                    "order_by": config.get("sort_by", "relevance"),
                }
                search_period_days = int(config.get("search_period_days", 0))
                if search_period_days > 0:
                    params["search_period"] = search_period_days
                if config.get("no_experience_only", False):
                    params["experience"] = "noExperience"
                url = config["search_url"] + "?" + urlencode(params)
                try:
                    html = await get_text(session, url, timeout, semaphore)
                    return page, url, html, None
                except aiohttp.ClientResponseError as exc:
                    return page, url, None, exc

            pages = await asyncio.gather(*(fetch_listing(page) for page in page_numbers), return_exceptions=True)
            for result in pages:
                if isinstance(result, Exception):
                    raise result
                page, url, html, error = result
                if error is not None:
                    if max_pages == 0 and page > 0 and error.status == 404:
                        logging.info("HH вернул HTTP 404 на странице %s — достигнут предел выдачи сайта.", page + 1)
                        empty_page = page
                        break
                    raise error
                page_candidates, card_count, reported_count = parse_search_page(html, page, url)
                if page_candidates is None:
                    if page == 0:
                        logging.warning("На первой странице нет карточек вакансий (HTML %s символов, URL %s); HH мог изменить разметку или показать проверку", len(html), url)
                    else:
                        logging.info("На странице %s нет карточек; достигнут конец выдачи HH (URL %s).", page + 1, url)
                    empty_page = page
                    break
                logging.info("Страница %s: карточек=%d; ссылок=%d; HH сообщает результатов=%s", page + 1, card_count, len(page_candidates), reported_count)
                total_cards += card_count
                total_links += len(page_candidates)
                all_candidates.extend(page_candidates)
            if empty_page is not None:
                break
            batch_start = batch_end

        training_keywords = config.get("training_keywords", [])
        welding_keywords = config.get("welding_keywords", [])
        filters_enabled = bool(training_keywords or welding_keywords)
        jobs = all_candidates

        async def enrich_and_filter(job):
            # Full descriptions are downloaded only when keyword filters need them.
            fingerprint = job_fingerprint(job)
            already_sent = job["id"] in sent or (fingerprint is not None and fingerprint in sent)
            if filters_enabled and not already_sent:
                try:
                    html = await get_text(session, job["url"], timeout, semaphore)
                    detail_soup = BeautifulSoup(html, "html.parser")
                    description = detail_soup.select_one('[data-qa="vacancy-description"]')
                    if description:
                        job["snippet"] += " " + description.get_text(" ", strip=True)
                except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                    logging.warning("Не удалось прочитать описание вакансии %s: %s", job["id"], exc)
            searchable = norm(job["title"] + " " + job["snippet"])
            matches_training = not training_keywords or any(norm(k) in searchable for k in training_keywords)
            matches_welding = not welding_keywords or any(norm(k) in searchable for k in welding_keywords)
            return job if matches_training and matches_welding else None

        results = await asyncio.gather(*(enrich_and_filter(job) for job in jobs))
        found = [job for job in results if job is not None]

    logging.info("Скраппинг: карточек=%d, ссылок=%d, собрано вакансий=%d, совпадений с фильтрами=%d", total_cards, total_links, len(all_candidates), len(found))
    return found


async def run_once(config):
    logging.info("Фильтры: запрос=%r; сортировка=%s; период=%s дн. (0=всё время); обучение=%s; сварка=%s; без опыта=%s", config.get("search_text"), config.get("sort_by", "relevance"), config.get("search_period_days", 0), config.get("training_keywords", []), config.get("welding_keywords", []), config.get("no_experience_only", False))
    sent = load_sent()
    jobs = await scrape(config, sent)
    fresh = []
    cycle_ids = set()
    cycle_fingerprints = set()
    duplicate_count = 0
    for job in jobs:
        fingerprint = job_fingerprint(job)
        if job["id"] in sent or job["id"] in cycle_ids or (fingerprint is not None and (fingerprint in sent or fingerprint in cycle_fingerprints)):
            duplicate_count += 1
            continue
        cycle_ids.add(job["id"])
        if fingerprint is not None:
            cycle_fingerprints.add(fingerprint)
        fresh.append(job)
    token = os.environ["TELEGRAM_BOT_TOKEN"]
    chat_id = os.environ["TELEGRAM_CHAT_ID"]
    delivered = 0
    async with aiohttp.ClientSession() as telegram_session:
        for job in fresh:
            try:
                await send_telegram(telegram_session, token, chat_id, job, config["request_timeout_seconds"])
            except (aiohttp.ClientError, asyncio.TimeoutError, RuntimeError) as exc:
                logging.error("Не удалось отправить вакансию %s в Telegram; повторю при следующем поиске: %s", job["id"], exc)
                continue
            sent.add(job["id"])
            fingerprint = job_fingerprint(job)
            if fingerprint is not None:
                sent.add(fingerprint)
            save_sent(sent)
            delivered += 1
            print(f"\nОтправлено в Telegram: {job['title']}\n{job['url']}\n", flush=True)
    logging.info("Подходящих карточек: %d; новых: %d; отправлено в Telegram: %d; дубликатов/уже показанных пропущено: %d", len(jobs), len(fresh), delivered, duplicate_count)


async def main_async():
    config = load_config()

    missing_telegram_settings = [name for name in ("TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID") if not os.environ.get(name)]
    if missing_telegram_settings:
        logging.error("Для отправки в Telegram задайте переменные окружения: %s", ", ".join(missing_telegram_settings))
        return
    logging.info("Поиск на HeadHunter: регион %s (id=%s), интервал %s сек.", config["region_name"], config["region_id"], config["interval_seconds"])
    while True:
        try:
            await run_once(config)
        except aiohttp.ClientResponseError as exc:
            logging.error("HH отказал в запросе (HTTP %s): %s", exc.status, exc)
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            logging.error("Не удалось подключиться к HH после повторных попыток: %s", exc)
        except Exception:
            logging.exception("Ошибка цикла поиска")
        await asyncio.sleep(config["interval_seconds"])


def main():
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        logging.info("Остановлено пользователем.")


if __name__ == "__main__":
    main()
