# Distributed Banking Platform

**تکلیف دوم درس سیستم‌های توزیع‌شده** — Sharding, Replication & Distributed Transactions

## هدف

تبدیل سامانه بانکداری تکلیف اول به یک بانک توزیع‌شده که حساب‌ها را در دو شارد مستقل نگه می‌دارد، درخواست‌ها را بین Replicaهای سالم پخش می‌کند و انتقال بین شاردها را با **Two-Phase Commit (2PC) واقعی و قابل بازیابی** انجام می‌دهد. تمام سرویس‌ها باید با **Docker Compose روی یک میزبان** اجرا شوند.

> **وضعیت فعلی: فاز ۱ — اسکلت پروژه.** فقط سرویس‌های اولیه، پیکربندی، تست‌های پایه و Health Check آماده‌اند. مسیرهای بانکی فعلاً عمداً `501 NOT_IMPLEMENTED` می‌دهند. موجودی، تراکنش پایگاه داده، مسیریابی عملیاتی، 2PC و بازیابی **هنوز پیاده‌سازی نشده‌اند**.

## معماری هدف

```text
Banking Client
      |
      v
Gateway (shard lookup + healthy replica selection)
      |                              |
      |                              v
      |                        Coordinator (2PC)
      |                              |
      +---- Shard A (A0001..A1000) -+---- Shard B (A1001..A2000)
             |        |                      |        |
           Bank A1  Bank A2                Bank B1  Bank B2
              \      /                        \      /
            PostgreSQL A                    PostgreSQL B
```

هر دو Replica در یک شارد، در آینده فقط به دیتابیس **مشترک همان شارد** دسترسی خواهند داشت؛ داده‌های دو دیتابیس مستقل‌اند. Gateway تنها ورودی عمومی سامانه است. Coordinator در پایان فازهای ۵ و ۶ تصمیمات تراکنش را به‌شکل پایدار ثبت و بازیابی خواهد کرد.

## اجزای بسته فاز ۱

| مسیر | نقش فعلی |
|---|---|
| `bank_platform/gateway/` | سرویس Gateway با Health Check؛ عملیات بانکی هنوز غیرفعال |
| `bank_platform/shard/` | سرویس مشترک Replicaها با شناسه A1/A2/B1/B2 |
| `bank_platform/coordinator/` | اسکلت Coordinator؛ پیاده‌سازی 2PC در فازهای بعد |
| `bank_platform/common/validation.py` | اعتبارسنجی محلی برگرفته از تکلیف اول |
| `bank_platform/common/shard_map.py` | شناسایی مالک حساب با نقشه قطعی (فعلاً بدون HTTP routing) |
| `config/shard_map.json` | محدوده حساب‌ها و آدرس Replicaها |
| `compose.yaml` | دو PostgreSQL، چهار Replica، Gateway و Coordinator؛ مجموع ۸ سرویس |
| `client/main.py` | کلاینت برگرفته از تکلیف اول با فرمان Health Check |
| `tests/` | تست‌های پایه مستقل از Docker |

**توجه:** تنظیمات `max_prepared_transactions=100` از همین حالا برای دیتابیس‌ها قرار داده شده، اما این به معنی پیاده‌سازی 2PC نیست.

## اجرای محلی تست‌های فاز ۱

روی Python 3.12 یا نسخه سازگار:

```bash
python -m pip install -r requirements-dev.txt
python -m pytest -q
```

## اجرای Docker Compose (فقط اسکلت؛ هنوز بانک عملیاتی نیست)

```bash
cp .env.example .env
# دو مقدار SHARD_A_DB_PASSWORD و SHARD_B_DB_PASSWORD را به رمزهای قوی تغییر بدهید.
docker compose config --quiet
docker compose up -d --build
docker compose ps
curl -fsS http://127.0.0.1:8001/health
python scripts/smoke_phase1.py
```

در فاز ۱ پاسخ `health` فقط **زنده بودن پردازش** را تأیید می‌کند و فیلد `banking_ready: false` دارد. بانک هنوز آماده پردازش پول نیست. برای دیدن چهار Replica و دو دیتابیس:

```bash
docker compose ps
docker compose logs --tail=30 bank_a1 bank_a2 bank_b1 bank_b2
# دیدن وضعیت PostgreSQL بدون انتشار پورت دیتابیس
docker compose exec shard_a_db pg_isready -U bank -d bank_a
docker compose exec shard_b_db pg_isready -U bank -d bank_b
```

همه سرویس‌ها با نام پروژه Compose مستقل `distributed-banking` راه‌اندازی می‌شوند. Gateway پیش‌فرض روی `127.0.0.1:8001` است و پورت دیتابیس‌ها منتشر نمی‌شود. تنظیمات `.env` واقعی را Commit نکنید. **`docker compose down -v` نزنید**؛ این دستور Volumeهای داده را پاک می‌کند.

برای توقف بدون حذف Volumeها:

```bash
docker compose down
```

## ترتیب توسعه (بدون Issue؛ Commit محور)

| فاز | خروجی الزامی | Commit پیشنهادی |
|---|---|---|
| ۱ | ساختار مستقل، Compose، شناسایی اجزا، تست پایه | `chore: initialize distributed banking scaffold` |
| ۲ | دو دیتابیس، ایجاد حساب‌ها، مالکیت یکتا و ذخیره‌سازی واقعی | `feat: implement persistent account sharding` |
| ۳ | Gateway، مسیریابی، توزیع بار و خروج Replica ناسالم | `feat: route requests to healthy shard replicas` |
| ۴ | Balance/Deposit/Withdraw، هم‌زمانی و انتقال داخل شارد | `feat: implement atomic local banking operations` |
| ۵ | Coordinator و پروتکل واقعی PREPARE/COMMIT/ABORT | `feat: implement cross-shard two-phase commit` |
| ۶ | وضعیت پایدار تراکنش، استعلام و سناریوهای بازیابی A/B/C | `feat: recover unresolved distributed transactions` |
| ۷ | اجرای ۷ آزمایش اجباری و جمع‌آوری شواهد | `test: verify mandatory distributed banking scenarios` |
| ۸ | استقرار روی VPS، Docker Hub، گزارش و ویدئو | `chore: prepare assignment submission` |
| ۹ (اختیاری) | Autonomous Self-Healing؛ بدون restart policy خودکار | `feat: implement autonomous replica recovery` |
| ۱۰ (اختیاری) | افزودن Shard C و مهاجرت زنده با Epoch و Fencing | `feat: implement live resharding` |

هر فاز فقط با پاس شدن معیارهای پذیرش همان فاز تکمیل‌شده در نظر گرفته می‌شود. برای جزئیات گیت‌ها فایل `docs/IMPLEMENTATION_PLAN.md` را ببینید.

## هفت آزمایش اصلی استاد

۱. توزیع حساب‌ها و مسیریابی درست؛ ۲. پخش درخواست بین Replicaها؛ ۳. صحت موجودی زیر بار هم‌زمان؛ ۴. ادامه سرویس با قطع یک Replica؛ ۵. انتقال داخلی و بین‌شاردی؛ ۶. بازیابی Coordinator بعد از ثبت تصمیم COMMIT و پیش از همه تأییدها؛ ۷. انتقال‌های هم‌زمان بین شاردها با حساب‌های مشترک و حفظ مجموع پول.

## امکانات امتیازی

- **+۱۰٪:** ساخت کنترل‌کننده Self-Healing مستقل با بازیابی از Crash؛ Kubernetes، Docker Swarm، ابزارهای آماده خودترمیمی و Docker automatic restart ممنوع‌اند.
- **+۱۲٪:** افزودن شارد C و مهاجرت آنلاین `A1501..A2000` از B، شامل Change Capture، Epoch/Fencing، سازگاری با 2PC و بازیابی خودکار مهاجرت.

## خروجی نهایی تکلیف

سورس در GitHub، تمام ایمیج‌های ضروری در Docker Hub، گزارش فنی حداکثر ۱۰ صفحه A4 و ویدئو حداکثر ۱۵ دقیقه. موعد سند: `۱۴۰۵/۰۸/۱۵`؛ تحویل: LMS، GitHub و Docker Hub.

**مرجع اصلی:** `Assignment2_Distributed_Banking_Platform.pdf`. کد تکلیف اول صرفاً به‌عنوان مبنای مهندسی استفاده شده و پروژه فعلی هیچ فایل یا Volume آن را تغییر نمی‌دهد.
